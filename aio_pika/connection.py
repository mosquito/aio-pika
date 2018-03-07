import asyncio
import logging
from contextlib import suppress
from functools import wraps, partial
from typing import Callable, Any, Generator

import pika.channel
from pika import ConnectionParameters
from pika.credentials import ExternalCredentials, PlainCredentials
from pika.spec import REPLY_SUCCESS
from yarl import URL
from . import exceptions
from .channel import Channel
from .common import FutureStore
from .tools import create_future
from .adapter import AsyncioConnection


log = logging.getLogger(__name__)
pika_logger = logging.getLogger('pika.adapters.base_connection')
pika_logger.setLevel(logging.WARNING)


def _ensure_connection(func):
    @wraps(func)
    def wrap(self, *args, **kwargs):
        if self.is_closed:
            raise RuntimeError("Connection closed")

        return func(self, *args, **kwargs)
    return wrap


class Connection:
    """ Connection abstraction """

    __slots__ = (
        'loop', '__closing', '_connection', 'future_store', '__sender_lock',
        '_io_loop', '__connection_parameters', '__credentials',
        '__write_lock', '_channels',
    )

    CHANNEL_CLASS = Channel

    def __init__(self, host: str = 'localhost', port: int = 5672, login: str = 'guest',
                 password: str = 'guest', virtual_host: str = '/',
                 ssl: bool = False, *, loop=None, **kwargs):

        self.loop = loop if loop else asyncio.get_event_loop()
        self.future_store = FutureStore(loop=self.loop)

        self.__credentials = PlainCredentials(login, password) if login else ExternalCredentials()

        self.__connection_parameters = ConnectionParameters(
            host=host,
            port=port,
            credentials=self.__credentials,
            virtual_host=virtual_host,
            ssl=ssl,
            **kwargs
        )

        self._channels = dict()
        self._connection = None
        self.__closing = None
        self.__write_lock = asyncio.Lock(loop=self.loop)

    def __str__(self):
        return 'amqp://{credentials}{host}:{port}/{vhost}'.format(
            credentials="{0.username}:********@".format(self.__credentials) if isinstance(
                self.__credentials, PlainCredentials) else '',
            host=self.__connection_parameters.host,
            port=self.__connection_parameters.port,
            vhost=self.__connection_parameters.virtual_host,
        )

    def __repr__(self):
        cls_name = self.__class__.__name__
        return '<{0}: "{1}">'.format(cls_name, str(self))

    def add_close_callback(self, callback: Callable[[], None]):
        """ Add callback which will be called after connection will be closed.

        :class:`asyncio.Future` will be passed as a first argument.

        Example:

        .. code-block:: python

            import aio_pika

            async def main():
                connection = await aio_pika.connect(
                    "amqp://guest:guest@127.0.0.1/"
                )
                connection.add_close_callback(print)
                await connection.close()
                # <Future finished result='Normal shutdown'>


        :return: None
        """

        self._closing.add_done_callback(callback)

    @property
    def is_closed(self):
        """ Is this connection are closed """

        if not (self._connection and self._connection.socket):
            return True

        if self._closing.done():
            return True

        return False

    @property
    def _closing(self):
        self._create_closing_future()
        return self.__closing

    def _create_closing_future(self, force=False):
        if self.__closing is None or force:
            self.__closing = self.future_store.create_future()

    @property
    @asyncio.coroutine
    def closing(self):
        """ Return coroutine which will be finished after connection close.

        Example:

        .. code-block:: python

            import aio_pika

            async def async_close(connection):
                await asyncio.sleep(2)
                await connection.close()

            async def main(loop):
                connection = await aio_pika.connect(
                    "amqp://guest:guest@127.0.0.1/"
                )
                loop.create_task(async_close(connection))

                await connection.closing

        """
        return (yield from self._closing)

    def _channel_cleanup(self, channel: pika.channel.Channel):
        ch = self._channels.pop(channel.channel_number)     # type: Channel
        ch._futures.reject_all(exceptions.ChannelClosed)

    def _on_connection_refused(self, future: asyncio.Future, connection: AsyncioConnection, message: str):
        self._on_connection_lost(future, connection, code=500, reason=ConnectionRefusedError(message))

    def _on_connection_lost(self, future: asyncio.Future, connection: AsyncioConnection, code, reason):
        if self.__closing and self.__closing.done():
            return

        if code == REPLY_SUCCESS:
            return self.__closing.set_result(reason)

        if isinstance(reason, Exception):
            exc = reason
        else:
            exc = ConnectionError(reason, code)

        self.future_store.reject_all(exc)

        if future.done():
            return

        future.set_exception(exc)

    @asyncio.coroutine
    def connect(self) -> AsyncioConnection:
        """ Connect to AMQP server. This method should be called after :func:`aio_pika.connection.Connection.__init__`

        .. note::
            This method is called by :func:`connect`. You shouldn't call it explicitly.

        """

        if self.__closing and self.__closing.done():
            raise RuntimeError("Invalid connection state")

        with (yield from self.__write_lock):
            self._connection = None

            log.debug("Creating a new AMQP connection: %s", self)

            f = create_future(loop=self.loop)

            connection = AsyncioConnection(
                parameters=self.__connection_parameters,
                loop=self.loop,
                on_open_callback=f.set_result,
                on_close_callback=partial(self._on_connection_lost, f),
                on_open_error_callback=partial(self._on_connection_refused, f),
            )

            connection.channel_cleanup_callback = self._channel_cleanup

            result = yield from f

            log.debug("Connection ready: %r", self)

            self._connection = connection
            return result

    @_ensure_connection
    @asyncio.coroutine
    def channel(self, channel_number: int=None, publisher_confirms: bool=True,
                on_return_raises=False) -> Generator[Any, None, Channel]:
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
                channel_no_confirms = connection.channel(publisher_confirms=True)
                await channel_no_confirms.close()

        :param channel_number: specify the channel number explicit
        :param publisher_confirms:
            if `True` the :method:`aio_pika.Exchange.publish` method will be return
            :class:`bool` after publish is complete. Otherwise the
            :method:`aio_pika.Exchange.publish` method will be return :class:`None`
        :param on_return_raises:
            raise an :class:`aio_pika.exceptions.UnroutableError`
            when mandatory message will be returned
        """
        with (yield from self.__write_lock):
            log.debug("Creating AMQP channel for conneciton: %r", self)

            channel = self.CHANNEL_CLASS(self, self.loop, self.future_store,
                                         channel_number=channel_number,
                                         publisher_confirms=publisher_confirms,
                                         on_return_raises=on_return_raises)
            yield from channel.initialize()

            log.debug("Channel created: %r", channel)

            self._channels[channel.number] = channel

            return channel

    def close(self) -> asyncio.Task:
        """ Close AMQP connection """
        log.debug("Closing AMQP connection")

        @asyncio.coroutine
        def inner():
            if self._connection:
                self._connection.close()
            yield from self.closing

        return self.loop.create_task(inner())

    def __del__(self):
        with suppress(Exception):
            if not self.is_closed:
                self.close()

    @asyncio.coroutine
    def __aenter__(self) -> 'Connection':
        return self

    @asyncio.coroutine
    def __aexit__(self, exc_type, exc_val, exc_tb):
        for channel in tuple(self._channels.values()):
            yield from channel.close()

        yield from self.close()


@asyncio.coroutine
def connect(url: str=None, *, host: str='localhost',
            port: int=5672, login: str='guest',
            password: str='guest', virtualhost: str='/',
            ssl: bool=False, loop=None, connection_class=Connection, **kwargs) -> Generator[Any, None, Connection]:
    """ Make connection to the broker

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

    if url:
        url = URL(str(url))
        host = url.host or host
        port = url.port or port
        login = url.user or login
        password = url.password or password
        virtualhost = url.path[1:] if len(url.path) > 1 else virtualhost

    connection = connection_class(
        host=host, port=port, login=login, password=password,
        virtual_host=virtualhost, ssl=ssl, loop=loop, **kwargs
    )

    yield from connection.connect()
    return connection


__all__ = ('connect', 'Connection')
