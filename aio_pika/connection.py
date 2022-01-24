import asyncio
import logging
from functools import partial
from typing import Optional, Type, TypeVar

import aiormq
from aiormq.tools import censor_url
from yarl import URL

from .channel import Channel
from .pool import PoolInstance
from .tools import CallbackCollection
from .types import CloseCallbackType, TimeoutType


log = logging.getLogger(__name__)


class Connection(PoolInstance):
    """ Connection abstraction """

    CHANNEL_CLASS = Channel
    KWARGS_TYPES = ()

    @property
    def is_closed(self):
        return self.closing.done()

    async def close(self, exc=asyncio.CancelledError):
        if not self.closing.done():
            self.closing.set_result(exc)

        return await self.connection.close(exc)

    @classmethod
    def _parse_kwargs(cls, kwargs):
        result = {}
        for key, parser, default in cls.KWARGS_TYPES:
            result[key] = parser(kwargs.get(key, default))
        return result

    def __init__(
        self, url, loop: Optional[asyncio.AbstractEventLoop] = None, **kwargs
    ):
        self.loop = loop or asyncio.get_event_loop()
        self.url = URL(url)

        self.kwargs = self._parse_kwargs(kwargs or self.url.query)

        self._close_callbacks = CallbackCollection(self)
        self.connection = None  # type: Optional[aiormq.Connection]
        self.closing = self.loop.create_future()

    @property
    def close_callbacks(self) -> CallbackCollection:
        return self._close_callbacks

    @property
    def heartbeat_last(self) -> float:
        """ returns loop.time() value since last received heartbeat """
        return self.connection.heartbeat_last_received

    @property
    def _channels(self) -> dict:
        return self.connection.channels

    def __str__(self):
        return str(censor_url(self.url))

    def __repr__(self):
        return '<{0}: "{1}">'.format(self.__class__.__name__, str(self))

    def add_close_callback(
        self, callback: CloseCallbackType, weak: bool = False
    ):
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

    def _on_connection_close(self, connection, closing, *args, **kwargs):
        exc = closing.exception()
        self.close_callbacks(exc)
        log.debug("Closing AMQP connection %r", connection)

    async def _make_connection(self, **kwargs) -> aiormq.Connection:
        connection = await aiormq.connect(self.url, **kwargs)
        connection.closing.add_done_callback(
            partial(self._on_connection_close, self.connection),
        )
        return connection

    async def connect(self, timeout: TimeoutType = None, **kwargs):
        """ Connect to AMQP server. This method should be called after
        :func:`aio_pika.connection.Connection.__init__`

        .. note::
            This method is called by :func:`connect`.
            You shouldn't call it explicitly.

        """
        self.connection = await asyncio.wait_for(
            self._make_connection(**kwargs), timeout=timeout,
        )

    def channel(
        self,
        channel_number: int = None,
        publisher_confirms: bool = True,
        on_return_raises: bool = False,
    ) -> Channel:
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

    async def ready(self):
        while self.connection is None:
            await asyncio.sleep(0)

    def __del__(self):
        if any((self.is_closed, self.loop.is_closed(), not self.connection)):
            return

        asyncio.shield(self.close())

    async def __aenter__(self) -> "Connection":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        for channel in tuple(self._channels.values()):  # can change size
            if channel and not channel.is_closed:
                await channel.close()

        await self.close()


ConnectionType = TypeVar("ConnectionType", bound=Connection)


async def connect(
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
    connection_class: Type[ConnectionType] = Connection,
    client_properties: dict = None,
    **kwargs
) -> ConnectionType:

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

    if url is None:
        kw = kwargs
        kw.update(ssl_options or {})

        url = URL.build(
            scheme="amqps" if ssl else "amqp",
            host=host,
            port=port,
            user=login,
            password=password,
            # yarl >= 1.3.0 requires path beginning with slash
            path="/" + virtualhost,
            query=kw,
        )

    connection = connection_class(url, loop=loop)

    await connection.connect(
        timeout=timeout, client_properties=client_properties, loop=loop,
    )
    return connection


__all__ = ("connect", "Connection")
