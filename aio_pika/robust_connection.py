import asyncio
from functools import wraps
from logging import getLogger
from typing import Callable, Type
from weakref import WeakSet

from aiormq.connection import parse_bool, parse_int

from .connection import Connection, ConnectionType, connect
from .exceptions import CONNECTION_EXCEPTIONS
from .robust_channel import RobustChannel
from .tools import CallbackCollection
from .types import TimeoutType


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
        ("reconnect_interval", parse_int, "5"),
        ("fail_fast", parse_bool, "1"),
    )

    def __init__(self, url, loop=None, **kwargs):
        super().__init__(url=url, loop=loop, **kwargs)

        self.connect_kwargs = {}
        self.reconnect_interval = self.kwargs["reconnect_interval"]
        self.fail_fast = self.kwargs["fail_fast"]

        self.__channels = WeakSet()
        self._reconnect_callbacks = CallbackCollection(self)
        self._connect_lock = asyncio.Lock()
        self._closed = False
        self.connected = asyncio.Event()

    @property
    def reconnecting(self) -> bool:
        return self._connect_lock.locked()

    @property
    def reconnect_callbacks(self) -> CallbackCollection:
        return self._reconnect_callbacks

    @property
    def _channels(self) -> dict:
        return {ch.number: ch for ch in self.__channels}

    def __repr__(self):
        return '<{0}: "{1}" {2} channels>'.format(
            self.__class__.__name__, str(self), len(self.__channels),
        )

    def _on_connection_close(self, connection, closing, *args, **kwargs):
        if self.reconnecting:
            return

        self.connected.clear()
        self.connection = None

        super()._on_connection_close(connection, closing)

        if self._closed:
            return

        log.info(
            "Connection to %s closed. Reconnecting after %r seconds.",
            self,
            self.reconnect_interval,
        )
        self.loop.call_later(
            self.reconnect_interval,
            lambda: self.loop.create_task(self.reconnect()),
        )

    def add_reconnect_callback(
        self, callback: Callable[[], None], weak: bool = False
    ):
        """ Add callback which will be called after reconnect.

        :return: None
        """

        self._reconnect_callbacks.add(callback, weak=weak)

    async def __cleanup_connection(self, exc):
        if self.connection is None:
            return
        await asyncio.gather(
            self.connection.close(exc), return_exceptions=True,
        )
        self.connection = None

    async def connect(self, timeout: TimeoutType = None, **kwargs):
        if self.is_closed:
            raise RuntimeError("{!r} connection closed".format(self))

        if kwargs:
            # Store connect kwargs for reconnects
            self.connect_kwargs = kwargs

        if self.reconnecting:
            log.warning(
                "Connect method called but connection %r is "
                "reconnecting right now.",
                self,
            )

        async with self._connect_lock:
            while True:
                try:
                    result = await super().connect(
                        timeout=timeout, **self.connect_kwargs
                    )

                    for channel in self._channels.values():
                        await channel.reopen()

                    self.fail_fast = False
                    self.connected.set()
                    return result
                except CONNECTION_EXCEPTIONS as e:
                    if self.fail_fast:
                        raise

                    await self.__cleanup_connection(e)

                    log.warning(
                        'Connection attempt to "%s" failed: %s. '
                        "Reconnecting after %r seconds.",
                        self,
                        e,
                        self.reconnect_interval,
                    )
                except asyncio.CancelledError as e:
                    await self.__cleanup_connection(e)
                    raise

                await asyncio.sleep(self.reconnect_interval)

    async def reconnect(self):
        await self.connect()
        self._reconnect_callbacks(self)

    def channel(
        self,
        channel_number: int = None,
        publisher_confirms: bool = True,
        on_return_raises=False,
    ):

        channel = super().channel(
            channel_number=channel_number,
            publisher_confirms=publisher_confirms,
            on_return_raises=on_return_raises,
        )

        self.__channels.add(channel)

        return channel

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


async def connect_robust(
    url: str = None,
    *,
    host: str = "localhost",
    port: int = 5672,
    login: str = "guest",
    password: str = "guest",
    virtualhost: str = "/",
    ssl: bool = False,
    loop: asyncio.AbstractEventLoop = None,
    ssl_options: dict = None,
    timeout: TimeoutType = None,
    connection_class: Type[ConnectionType] = RobustConnection,
    client_properties: dict = None,
    **kwargs
) -> ConnectionType:

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
    :param login: username string. `'guest'` by default.
    :param password: password string. `'guest'` by default.
    :param virtualhost: virtualhost parameter. `'/'` by default
    :param ssl: use SSL for connection. Should be used with addition kwargs.
    :param ssl_options: A dict of values for the SSL connection.
    :param timeout: connection timeout in seconds
    :param loop:
        Event loop (:func:`asyncio.get_event_loop()` when :class:`None`)
    :param connection_class: Factory of a new connection
    :param kwargs: addition parameters which will be passed to the connection.
    :return: :class:`aio_pika.connection.Connection`

    .. _RFC3986: https://goo.gl/MzgYAs
    .. _official Python documentation: https://goo.gl/pty9xA

    """
    return await connect(
        url=url,
        host=host,
        port=port,
        login=login,
        password=password,
        virtualhost=virtualhost,
        ssl=ssl,
        loop=loop,
        connection_class=connection_class,
        ssl_options=ssl_options,
        timeout=timeout,
        client_properties=client_properties,
        **kwargs
    )


__all__ = (
    "RobustConnection",
    "connect_robust",
)
