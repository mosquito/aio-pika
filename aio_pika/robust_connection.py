import asyncio
from logging import getLogger
from typing import Any, Type, Union
from weakref import WeakSet

import aiormq.abc
from aiormq.connection import parse_bool, parse_timeout
from pamqp.common import FieldTable
from yarl import URL

from .abc import (
    AbstractRobustChannel, AbstractRobustConnection, TimeoutType,
    UnderlayConnection,
)
from .connection import Connection, make_url
from .exceptions import CONNECTION_EXCEPTIONS
from .robust_channel import RobustChannel
from .tools import CallbackCollection, OneShotCallback


log = getLogger(__name__)


class RobustConnection(Connection, AbstractRobustConnection):
    """ Robust connection """

    CHANNEL_REOPEN_PAUSE = 1
    CHANNEL_CLASS: Type[RobustChannel] = RobustChannel
    KWARGS_TYPES = (
        ("reconnect_interval", parse_timeout, "5"),
        ("fail_fast", parse_bool, "1"),
    )

    def __init__(
        self, url: URL, loop: asyncio.AbstractEventLoop = None, **kwargs: Any
    ):
        super().__init__(url=url, loop=loop, **kwargs)

        self.reconnect_interval = self.kwargs.pop("reconnect_interval")
        self.fail_fast = self.kwargs.pop("fail_fast")

        self.__channels: WeakSet[AbstractRobustChannel] = WeakSet()
        self._reconnect_lock = asyncio.Lock()

        self.reconnect_callbacks: CallbackCollection = CallbackCollection(self)

    @property
    def reconnecting(self) -> bool:
        return self._reconnect_lock.locked()

    def __repr__(self) -> str:
        return (
            f'<{self.__class__.__name__}: "{self}" '
            f"{len(self.__channels)} channels>"
        )

    async def _on_connection_close(self, closing: asyncio.Future) -> None:
        await super()._on_connection_close(closing)

        if self._close_called:
            return

        log.info(
            "Connection to %s closed. Reconnecting after %r seconds.",
            self, self.reconnect_interval,
        )
        await asyncio.sleep(self.reconnect_interval)
        await self.reconnect()

    async def __connection_attempt(
        self, timeout: TimeoutType = None,
    ) -> aiormq.abc.AbstractConnection:
        connection = await UnderlayConnection.make_connection(
            self.url, timeout=timeout, **self.kwargs
        )

        try:
            for channel in self.__channels:
                try:
                    await channel.restore(connection)
                except Exception:
                    log.exception("Failed to reopen channel")
                    raise
        except asyncio.CancelledError:
            # In python 3.7 asyncio.CancelledError inherited
            # from Exception and this needed for catch it first
            raise
        except Exception as e:
            closing = self.loop.create_future()
            closing.set_exception(e)
            await self.close_callbacks(closing)
            await asyncio.gather(connection.close(e), return_exceptions=True)
            raise
        return connection

    async def connect(self, timeout: TimeoutType = None) -> None:
        if self.is_closed:
            raise RuntimeError(f"{self!r} connection closed")

        if self.reconnecting:
            raise RuntimeError(
                (
                    f"Connect method called but connection "
                    f"{self!r} is reconnecting right now."
                ), self,
            )

        async with self._reconnect_lock:
            self.transport = None
            self.connected.clear()

            while not self.is_closed:
                close_callback = OneShotCallback(
                    self._on_connection_close,
                )
                try:
                    connection = await self.__connection_attempt(timeout)
                    self.connected.set()

                    connection.closing.add_done_callback(close_callback)
                    self.transport = UnderlayConnection(
                        connection=connection,
                        close_callback=close_callback,
                    )

                    self.fail_fast = False
                    return
                except CONNECTION_EXCEPTIONS as e:
                    self.transport = None

                    if self.fail_fast:
                        raise

                    log.warning(
                        'Connection attempt to "%s" failed: %s. '
                        "Reconnecting after %r seconds.",
                        self,
                        e,
                        self.reconnect_interval,
                    )

                log.info(
                    "Reconnect attempt failed %s. Retrying after %r seconds.",
                    self, self.reconnect_interval,
                )
                await asyncio.sleep(self.reconnect_interval)

    async def reconnect(self) -> None:
        await self.connect()
        await self.reconnect_callbacks()

    def channel(
        self,
        channel_number: int = None,
        publisher_confirms: bool = True,
        on_return_raises: bool = False,
    ) -> AbstractRobustChannel:

        channel: AbstractRobustChannel = super().channel(
            channel_number=channel_number,
            publisher_confirms=publisher_confirms,
            on_return_raises=on_return_raises,
        )   # type: ignore

        self.__channels.add(channel)
        return channel


async def connect_robust(
    url: Union[str, URL] = None,
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
    client_properties: FieldTable = None,
    connection_class: Type[AbstractRobustConnection] = RobustConnection,
    **kwargs: Any
) -> AbstractRobustConnection:

    """ Make connection to the broker.

    Example:

    .. code-block:: python

        import aio_pika

        async def main():
            connection = await aio_pika.connect(
                "amqp://guest:guest@127.0.0.1/"
            )

    Connect to localhost with default credentials:

    .. code-block:: python

        import aio_pika

        async def main():
            connection = await aio_pika.connect()

    .. note::

        The available keys for ssl_options parameter are:
            * cert_reqs
            * certfile
            * keyfile
            * ssl_version

        For an information on what the ssl_options can be set to reference the
        `official Python documentation`_ .

    Set connection name for RabbitMQ admin panel:

    .. code-block:: python

        read_connection = await connect(
            client_properties={
                'connection_name': 'Read connection'
            }
        )

        write_connection = await connect(
            client_properties={
                'connection_name': 'Write connection'
            }
        )

    .. note:

        ``client_properties`` argument requires ``aiormq>=2.9``

    URL string might be contain ssl parameters e.g.
    `amqps://user:pass@host//?ca_certs=ca.pem&certfile=crt.pem&keyfile=key.pem`

    :param client_properties: add custom client capability.
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

    connection: AbstractRobustConnection = connection_class(
        make_url(
            url,
            host=host,
            port=port,
            login=login,
            password=password,
            virtualhost=virtualhost,
            ssl=ssl,
            ssl_options=ssl_options,
            client_properties=client_properties,
            **kwargs
        ),
        loop=loop,
    )

    await connection.connect(timeout=timeout)
    return connection


__all__ = (
    "RobustConnection",
    "connect_robust",
)
