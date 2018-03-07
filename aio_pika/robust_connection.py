import asyncio
from functools import wraps
from logging import getLogger
from typing import Callable, Generator, Any

import pika.channel
from pika.exceptions import ChannelClosed

from .adapter import AsyncioConnection
from .exceptions import ProbableAuthenticationError
from .connection import Connection, connect
from .robust_channel import RobustChannel


log = getLogger(__name__)


def _ensure_connection(func):
    @wraps(func)
    def wrap(self, *args, **kwargs):
        if self.is_closed:
            raise RuntimeError("Connection closed")

        return func(self, *args, **kwargs)
    return wrap


class RobustConnection(Connection):
    """ Robust connection """

    DEFAULT_RECONNECT_INTERVAL = 1
    CHANNEL_CLASS = RobustChannel

    def __init__(self, host: str = 'localhost', port: int = 5672, login: str = 'guest',
                 password: str = 'guest', virtual_host: str = '/',
                 ssl: bool = False, *, loop=None, **kwargs):

        self.reconnect_interval = kwargs.pop('reconnect_interval',
                                             self.DEFAULT_RECONNECT_INTERVAL)

        super().__init__(host=host, port=port, login=login, password=password,
                         virtual_host=virtual_host, ssl=ssl, loop=loop, **kwargs)

        self._closed = False
        self._on_connection_lost_callbacks = []
        self._on_reconnect_callbacks = []
        self._on_close_callbacks = []

    def add_connection_lost_callback(self, callback: Callable[[], None]):
        """ Add callback which will be called after connection was lost.

        :return: None
        """

        self._on_connection_lost_callbacks.append(lambda c: callback(c))

    def add_reconnect_callback(self, callback: Callable[[], None]):
        """ Add callback which will be called after reconnect.

        :return: None
        """

        self._on_reconnect_callbacks.append(lambda c: callback(c))

    def add_close_callback(self, callback: Callable[[], None]):
        """ Add callback which will be called after connection will be closed.

        :return: None
        """

        self._on_close_callbacks.append(lambda c: callback(c))

    def _on_connection_lost(self, future: asyncio.Future, connection: AsyncioConnection, code, reason):
        for callback in self._on_connection_lost_callbacks:
            callback(self)

        if self._closed:
            return super()._on_connection_lost(future, connection, code, reason)

        if isinstance(reason, ProbableAuthenticationError):
            log.error("Authentication error: %d - %s", code, reason)

        if isinstance(reason, ConnectionRefusedError):
            log.error("Connection refused: %d - %s", code, reason)

        if not future.done():
            future.set_result(None)

        self.loop.call_later(
            self.reconnect_interval,
            lambda: self.loop.create_task(self.connect())
        )

    def _channel_cleanup(self, channel: pika.channel.Channel):
        ch = self._channels[channel.channel_number]     # type: RobustChannel
        ch._futures.reject_all(ChannelClosed)

        if ch._closed:
            self._channels.pop(channel.channel_number)  # type: RobustChannel

    @asyncio.coroutine
    def connect(self):
        result = yield from super().connect()

        while self._connection is None:
            yield from asyncio.sleep(self.reconnect_interval, loop=self.loop)
            result = yield from super().connect()

        for number, channel in tuple(self._channels.items()):
            yield from channel.on_reconnect(self, number)

        for callback in self._on_reconnect_callbacks:
            callback(self)

        return result

    @property
    def is_closed(self):
        """ Is this connection is closed """

        return self._closed or super().is_closed

    def close(self) -> asyncio.Task:
        """ Close AMQP connection """
        self._closed = True

        try:
            for callback in self._on_close_callbacks:
                callback(self)
        finally:
            return super().close()


@asyncio.coroutine
def connect_robust(url: str=None, *, host: str='localhost',
                   port: int=5672, login: str='guest',
                   password: str='guest', virtualhost: str='/',
                   ssl: bool=False, loop=None,
                   connection_class=RobustConnection, **kwargs) -> Generator[Any, None, Connection]:
    """ Make robust connection to the broker.

    That means that connection state will be restored after reconnect.
    After connection has been established the channels, the queues and the
    exchanges with their bindings will be restored.

    Example:

    .. code-block:: python

        import aio_pika

        async def main():
            connection = await aio_pika.connect_robust("amqp://guest:guest@127.0.0.1/")

    Connect to localhost with default credentials:

    .. code-block:: python

        import aio_pika

        async def main():
            connection = await aio_pika.connect_robust()


    :param url: `RFC3986`_ formatted broker address. When :class:`None` will be used keyword arguments.
    :param host: hostname of the broker
    :param port: broker port 5672 by default
    :param login:
        username string. `'guest'` by default. Provide empty string for pika.credentials.ExternalCredentials usage.
    :param password: password string. `'guest'` by default.
    :param virtualhost: virtualhost parameter. `'/'` by default
    :param ssl: use SSL for connection. Should be used with addition kwargs. See `pika documentation`_ for more info.
    :param loop: Event loop (:func:`asyncio.get_event_loop()` when :class:`None`)
    :param connection_class: Factory of a new connection
    :param kwargs: addition parameters which will be passed to the pika connection.
    :return: :class:`aio_pika.connection.Connection`

    .. _RFC3986: https://tools.ietf.org/html/rfc3986
    .. _pika documentation: https://goo.gl/TdVuZ9
    """
    return (
        yield from connect(
            url=url, host=host, port=port, login=login,
            password=password, virtualhost=virtualhost, ssl=ssl,
            loop=loop, connection_class=connection_class, **kwargs
        )
    )


__all__ = 'RobustConnection', 'connect_robust',
