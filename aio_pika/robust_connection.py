import asyncio
from functools import wraps, partial
from logging import getLogger
from typing import Callable

from pika.exceptions import ProbableAuthenticationError

from .adapter import AsyncioConnection
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
    """ Connection abstraction """

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
        self._on_reconnect_callbacks = set()
        self._on_close_callbacks = set()

    def add_reconnect_callback(self, callback: Callable[[], None]):
        """ Add callback which will be called after reconnect.

        :return: None
        """

        self._on_reconnect_callbacks.add(lambda c: callback(c))

    def add_close_callback(self, callback: Callable[[], None]):
        """ Add callback which will be called after connection will be closed.

        :return: None
        """

        self._on_close_callbacks.add(lambda c: callback(c))

    def _on_connection_lost(self, future: asyncio.Future, connection: AsyncioConnection, code, reason):
        if self._closed:
            return super()._on_connection_lost(future, connection, code, reason)

        if isinstance(reason, ProbableAuthenticationError):
            if not future.done():
                future.set_exception(reason)

            self.loop.create_task(self.close())

            return

        if not future.done():
            future.set_result(None)

        self.loop.call_later(
            self.reconnect_interval,
            lambda: self.loop.create_task(self.connect())
        )

    @asyncio.coroutine
    def connect(self, callback=None):
        result = yield from super().connect()

        for number, channel in self._channels.items():
            yield from channel.on_reconnect(self, number)

        if self._connection:
            while self._on_reconnect_callbacks:
                self._on_reconnect_callbacks.pop()(self)

        return result

    @property
    def is_closed(self):
        """ Is this connection are closed """

        return self._closed or super().is_closed

    @asyncio.coroutine
    def close(self) -> None:
        """ Close AMQP connection """
        self._closed = True

        while self._on_close_callbacks:
            self._on_close_callbacks.pop()(self)

        yield from super().close()


connect_robust = partial(connect, connection_class=RobustConnection)


__all__ = 'RobustConnection', 'connect_robust',
