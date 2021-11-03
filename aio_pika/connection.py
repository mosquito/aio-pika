import asyncio
import logging
from functools import partial
from types import TracebackType
from typing import Any, Callable, Dict, Optional, Tuple, Type, TypeVar, Union

import aiormq
from aiormq.abc import ExceptionType
from aiormq.tools import censor_url
from yarl import URL

from .abc import AbstractConnection, ConnectionCloseCallback, TimeoutType, \
    AbstractChannel
from .channel import Channel
from .tools import CallbackCollection


log = logging.getLogger(__name__)
T = TypeVar("T")


class Connection(AbstractConnection):
    """ Connection abstraction """

    CHANNEL_CLASS = Channel
    KWARGS_TYPES: Tuple[Tuple[str, Callable[[str], Any], str], ...] = ()

    @property
    def is_closed(self) -> bool:
        return self.closing.done()

    async def close(
        self, exc: Optional[ExceptionType] = asyncio.CancelledError,
    ) -> None:
        if not self.closing.done():
            self.closing.set_result(exc)

        if not hasattr(self, "connection"):
            return None

        await self.connection.close(exc)
        del self.connection

    @classmethod
    def _parse_kwargs(cls, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        for key, parser, default in cls.KWARGS_TYPES:
            result[key] = parser(kwargs.get(key, default))
        return result

    def __init__(
        self, url: URL, loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs: Any
    ):
        super().__init__(url, loop, **kwargs)

        self.loop = loop or asyncio.get_event_loop()
        self.url = URL(url)

        self.kwargs: Dict[str, Any] = self._parse_kwargs(
            kwargs or dict(self.url.query),
        )

        self.connection: aiormq.abc.AbstractConnection
        self.close_callbacks = CallbackCollection(self)
        self.connected: asyncio.Event = asyncio.Event()
        self.closing: asyncio.Future = self.loop.create_future()

    def __str__(self) -> str:
        return str(censor_url(self.url))

    def __repr__(self) -> str:
        return f'<{self.__class__.__name__}: "{self}">'

    def add_close_callback(
        self, callback: ConnectionCloseCallback, weak: bool = False,
    ) -> None:
        """ Add callback which will be called after connection will be closed.

        :class:`BaseException` or None will be passed as a first argument.

        Example:

        .. code-block:: python

            import aio_pika

            async def main():
                connection = await aio_pika.connect(
                    "amqp://guest:guest@127.0.0.1/"
                )
                connection.add_close_callback(print)
                await connection.close()
                # None


        :return: None
        """
        self.close_callbacks.add(callback, weak=weak)

    def _on_connection_close(
        self, connection: AbstractConnection, closing: asyncio.Future
    ) -> None:
        log.debug("Closing AMQP connection %r", connection)
        exc: Optional[BaseException] = closing.exception()
        self.close_callbacks(exc)

        if self.closing.done():
            return

        if exc is not None:
            self.closing.set_exception(exc)
            return

        self.closing.set_result(closing.result())

    async def _make_connection(
        self, *, timeout: TimeoutType = None, **kwargs: Any
    ) -> aiormq.abc.AbstractConnection:
        connection: aiormq.abc.AbstractConnection = await asyncio.wait_for(
            aiormq.connect(self.url, **kwargs), timeout=timeout,
        )
        connection.closing.add_done_callback(
            partial(self._on_connection_close, self.connection),
        )
        await connection.ready()
        return connection

    async def connect(
        self, timeout: TimeoutType = None, **kwargs: Any
    ) -> None:
        """ Connect to AMQP server. This method should be called after
        :func:`aio_pika.connection.Connection.__init__`

        .. note::
            This method is called by :func:`connect`.
            You shouldn't call it explicitly.

        """
        self.connection = (
            await self._make_connection(timeout=timeout, **kwargs)
        )
        self.connected.set()
        self.connection.closing.add_done_callback(
            lambda _: self.connected.clear(),
        )

    def channel(
        self,
        channel_number: int = None,
        publisher_confirms: bool = True,
        on_return_raises: bool = False,
    ) -> AbstractChannel:
        """ Coroutine which returns new instance of :class:`Channel`.

        Example:

        .. code-block:: python

            import aio_pika

            async def main(loop):
                connection = await aio_pika.connect(
                    "amqp://guest:guest@127.0.0.1/"
                )

                channel1 = connection.channel()
                await channel1.close()

                # Creates channel with specific channel number
                channel42 = connection.channel(42)
                await channel42.close()

                # For working with transactions
                channel_no_confirms = connection.channel(
                    publisher_confirms=True
                )
                await channel_no_confirms.close()

        Also available as an asynchronous context manager:

        .. code-block:: python

            import aio_pika

            async def main(loop):
                connection = await aio_pika.connect(
                    "amqp://guest:guest@127.0.0.1/"
                )

                async with connection.channel() as channel:
                    # channel is open and available

                # channel is now closed

        :param channel_number: specify the channel number explicit
        :param publisher_confirms:
            if `True` the :func:`aio_pika.Exchange.publish` method will be
            return :class:`bool` after publish is complete. Otherwise the
            :func:`aio_pika.Exchange.publish` method will be return
            :class:`None`
        :param on_return_raises:
            raise an :class:`aio_pika.exceptions.DeliveryError`
            when mandatory message will be returned
        """

        log.debug("Creating AMQP channel for connection: %r", self)

        channel = self.CHANNEL_CLASS(
            connection=self,
            channel_number=channel_number,
            publisher_confirms=publisher_confirms,
            on_return_raises=on_return_raises,
        )

        log.debug("Channel created: %r", channel)
        return channel

    async def ready(self) -> None:
        await self.connected.wait()

    def __del__(self) -> None:
        if any((self.is_closed, self.loop.is_closed(), not self.connection)):
            return

        asyncio.shield(self.close())

    async def __aenter__(self) -> "Connection":
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        for channel in tuple(self.channels.values()):  # can change size
            if channel and not channel.is_closed:
                await channel.close()

        await self.close()


async def connect(
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
    connection_class: Type[AbstractConnection] = Connection,
    client_properties: dict = None,
    **kwargs: Any
) -> AbstractConnection:

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

    amqp_url: URL

    if url is None:
        kw = kwargs
        kw.update(ssl_options or {})

        amqp_url = URL.build(
            scheme="amqps" if ssl else "amqp",
            host=host,
            port=port,
            user=login,
            password=password,
            # yarl >= 1.3.0 requires path beginning with slash
            path="/" + virtualhost,
            query=kw,
        )
    else:
        amqp_url = URL(url)

    connection: AbstractConnection = connection_class(amqp_url, loop=loop)

    await connection.connect(
        timeout=timeout,
        client_properties=client_properties,
        loop=loop,
    )
    return connection


__all__ = ("connect", "Connection")
