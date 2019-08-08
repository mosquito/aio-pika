import asyncio
from functools import wraps
from logging import getLogger
from typing import Callable

from aiormq.connection import parse_bool, parse_int
from .exceptions import AMQPError, CONNECTION_EXCEPTIONS
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

    CHANNEL_CLASS = RobustChannel
    KWARGS_TYPES = (
        ('reconnect_interval', parse_int, '5'),
        ('fail_fast', parse_bool, '1'),
    )

    def __init__(self, url, loop=None, **kwargs):
        super().__init__(
            loop=loop or asyncio.get_event_loop(), url=url, **kwargs
        )

        self.reconnect_interval = self.kwargs['reconnect_interval']
        self.fail_fast = self.kwargs['fail_fast']

        self.__channels = set()
        self._on_reconnect_callbacks = set()
        self._closed = False

    @property
    def _channels(self) -> dict:
        return {ch.number: ch for ch in self.__channels}

    def _on_connection_close(self, connection, closing, *args, **kwargs):
        self.connection = None

        # Have to remove non initialized channels
        self.__channels = {
            ch for ch in self.__channels if ch.number is not None
        }

        super()._on_connection_close(connection, closing)

        self.loop.call_later(
            self.reconnect_interval,
            lambda: self.loop.create_task(self.reconnect())
        )

    def add_reconnect_callback(self, callback: Callable[[], None]):
        """ Add callback which will be called after reconnect.

        :return: None
        """

        self._on_reconnect_callbacks.add(callback)

    async def connect(self, timeout=None):
        while True:
            try:
                return await super().connect(timeout=timeout)
            except CONNECTION_EXCEPTIONS:
                if self.fail_fast:
                    raise

                log.warning(
                    "First connection attempt failed "
                    "and will be retried after %d seconds",
                    self.reconnect_interval,
                    exc_info=True,
                )

                await asyncio.sleep(self.reconnect_interval)

    async def reconnect(self):
        if self.is_closed:
            return

        try:
            await super().connect()
        except CONNECTION_EXCEPTIONS:
            log.exception('Connection attempt error')

            self.loop.call_later(
                self.reconnect_interval,
                lambda: self.loop.create_task(self.reconnect())
            )
        else:
            await self._on_reconnect()

    def channel(self, channel_number: int = None,
                publisher_confirms: bool = True,
                on_return_raises=False):

        channel = super().channel(
            channel_number=channel_number,
            publisher_confirms=publisher_confirms,
            on_return_raises=on_return_raises,
        )

        self.__channels.add(channel)

        return channel

    async def _on_reconnect(self):
        for number, channel in self._channels.items():
            try:
                await channel.on_reconnect(self, number)
            except (RuntimeError, AMQPError):
                log.exception('Open channel failure')
                await self.close()
                return

        for callback in self._on_reconnect_callbacks:
            try:
                callback(self)
            except Exception:
                log.exception("Callback exception")

    @property
    def is_closed(self):
        """ Is this connection is closed """
        return self._closed or super().is_closed

    async def close(self, exc=asyncio.CancelledError):
        if self.is_closed:
            return

        self._closed = True

        if self.connection is None:
            return
        return await super().close(exc)


async def connect_robust(url: str = None, *, host: str = 'localhost',
                         port: int = 5672, login: str = 'guest',
                         password: str = 'guest', virtualhost: str = '/',
                         ssl: bool = False, loop=None,
                         ssl_options: dict = None,
                         connection_class=RobustConnection,
                         **kwargs) -> Connection:

    """ Make robust connection to the broker.

    That means that connection state will be restored after reconnect.
    After connection has been established the channels, the queues and the
    exchanges with their bindings will be restored.

    Example:

    .. code-block:: python

        import aio_pika

        async def main():
            connection = await aio_pika.connect_robust(
                "amqp://guest:guest@127.0.0.1/"
            )

    Connect to localhost with default credentials:

    .. code-block:: python

        import aio_pika

        async def main():
            connection = await aio_pika.connect_robust()

    .. note::

        The available keys for ssl_options parameter are:
            * cert_reqs
            * certfile
            * keyfile
            * ssl_version

        For an information on what the ssl_options can be set to reference the
        `official Python documentation`_ .

    URL string might be contain ssl parameters e.g.
    `amqps://user:pass@host//?ca_certs=ca.pem&certfile=crt.pem&keyfile=key.pem`

    :param url:
        RFC3986_ formatted broker address. When :class:`None`
        will be used keyword arguments.
    :param host: hostname of the broker
    :param port: broker port 5672 by default
    :param login:
        username string. `'guest'` by default. Provide empty string
        for pika.credentials.ExternalCredentials usage.
    :param password: password string. `'guest'` by default.
    :param virtualhost: virtualhost parameter. `'/'` by default
    :param ssl:
        use SSL for connection. Should be used with addition kwargs.
        See `pika documentation`_ for more info.
    :param ssl_options: A dict of values for the SSL connection.
    :param loop:
        Event loop (:func:`asyncio.get_event_loop()` when :class:`None`)
    :param connection_class: Factory of a new connection
    :param kwargs:
        addition parameters which will be passed to the pika connection.
    :return: :class:`aio_pika.connection.Connection`

    .. _RFC3986: https://goo.gl/MzgYAs
    .. _pika documentation: https://goo.gl/TdVuZ9
    .. _official Python documentation: https://goo.gl/pty9xA

    """
    return (
        await connect(
            url=url, host=host, port=port, login=login,
            password=password, virtualhost=virtualhost, ssl=ssl,
            loop=loop, connection_class=connection_class,
            ssl_options=ssl_options, **kwargs
        )
    )


__all__ = 'RobustConnection', 'connect_robust',
