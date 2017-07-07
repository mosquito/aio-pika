import asyncio
import warnings
from enum import IntEnum, unique
from functools import wraps, partial
from logging import getLogger
from typing import Callable, Any, Generator

from pika import ConnectionParameters
from pika.credentials import PlainCredentials
from pika.spec import REPLY_SUCCESS
from yarl import URL
from .channel import Channel
from .common import FutureStore
from .tools import create_future
from .adapter import AsyncioConnection


log = getLogger(__name__)


@unique
class ConnectionState(IntEnum):
    INITIALIZED = 0
    CONNECTING = 1
    READY = 2
    CLOSED = 3


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
        'loop', '__state', '_connection', 'future_store', '__sender_lock',
        '_io_loop', '__connection_parameters', '__credentials',
        '__write_lock', '__close_callbacks',
    )

    CHANNEL_CLASS = Channel

    def __init__(self, host: str = 'localhost', port: int = 5672, login: str = 'guest',
                 password: str = 'guest', virtual_host: str = '/',
                 ssl: bool = False, *, loop=None, **kwargs):

        self.loop = loop if loop else asyncio.get_event_loop()
        self.future_store = FutureStore(loop=self.loop)

        self.__credentials = PlainCredentials(login, password) if login else None

        self.__connection_parameters = ConnectionParameters(
            host=host,
            port=port,
            credentials=self.__credentials,
            virtual_host=virtual_host,
            ssl=ssl,
            connection_attempts=1,
            **kwargs
        )

        self._connection = None
        self.__state = ConnectionState.INITIALIZED
        self.__write_lock = asyncio.Lock(loop=self.loop)
        self.__close_callbacks = set()

    def __str__(self):
        return 'amqp://{credentials}{host}:{port}/{vhost}'.format(
            credentials="{0.username}:********@".format(self.__credentials) if self.__credentials else '',
            host=self.__connection_parameters.host,
            port=self.__connection_parameters.port,
            vhost=self.__connection_parameters.virtual_host,
        )

    def __repr__(self):
        cls_name = self.__class__.__name__
        return '<{0}: "{1}">'.format(cls_name, str(self))

    def add_close_callback(self, callback: Callable[[], None]):
        """ Add callback which will be called after connection will be closed.

        :return: None
        """
        self.__close_callbacks.add(callback)

    @property
    def is_closed(self):
        """ Returns True if connection is closed """

        if not (self._connection and self._connection.socket):
            return True

        return self.__state == ConnectionState.CLOSED

    @property
    def is_initialized(self):
        """ Returns True if connection in initialized state """
        return self.__state == ConnectionState.INITIALIZED

    @property
    def is_opened(self):
        """ Returns True if connection is ready """

        if not self._connection:
            return False
        elif not self._connection.is_open:
            return False
        return self.__state == ConnectionState.READY

    @asyncio.coroutine
    def ready(self):
        while not self.is_opened:
            yield
        return True

    @property
    @asyncio.coroutine
    def closing(self):
        """ Return coroutine which will be finished after connection close. """
        while not self.is_closed:
            yield
        return True

    def _on_connection_refused(self, future, connection, message: str):
        self._on_connection_lost(future, connection, code=500, reason=ConnectionRefusedError(message))

    def _on_connection_lost(self, future, _, code, reason):
        if self.__state == ConnectionState.CLOSED:
            return

        if code == REPLY_SUCCESS:
            return self.__state.set_result(reason)

        if isinstance(reason, Exception):
            exc = reason
        else:
            exc = ConnectionError(reason, code)

        self.future_store.reject_all(exc)

        for cb in map(asyncio.coroutine, self.__close_callbacks):
            self.loop.create_task(cb(exc))

        if future.done():
            return

        self.__state = ConnectionState.CLOSED
        future.set_exception(exc)

    @asyncio.coroutine
    def _create_pika_connection(self):
        with (yield from self.__write_lock):
            log.debug("Creating a new AMQP connection: %s", self)

            future = create_future(loop=self.loop)

            connection = AsyncioConnection(
                parameters=self.__connection_parameters,
                loop=self.loop,
                on_open_callback=future.set_result,
                on_close_callback=partial(self._on_connection_lost, future),
                on_open_error_callback=partial(self._on_connection_refused, future),
            )

            try:
                yield from future
            except:
                connection.close()
                raise

            log.debug("Connection %r ready.", self)

            return connection

    @asyncio.coroutine
    def connect(self):
        """ Perform connect. This method should be called after :func:`aio_pika.connection.Connection.__init__`"""
        log.debug("Performing connection for object %r", self)

        self.__state = ConnectionState.CONNECTING
        try:
            self._connection = yield from self._create_pika_connection()
        except:
            self.__state = ConnectionState.INITIALIZED
            raise
        else:
            self.__state = ConnectionState.READY

    @_ensure_connection
    @asyncio.coroutine
    def channel(self) -> Generator[Any, None, Channel]:
        """ Get a channel """
        yield from self.ready()

        with (yield from self.__write_lock):
            log.debug("Creating AMQP channel for connection: %r", self)

            channel = self.CHANNEL_CLASS(self, self.loop, self.future_store)
            yield from channel.initialize()

            log.debug("Channel created: %r", channel)
            return channel

    @asyncio.coroutine
    def close(self) -> None:
        """ Close AMQP connection """
        log.debug("Closing AMQP connection")
        self._connection.close()
        yield from self.closing


@asyncio.coroutine
def connect(url: str=None, *, host: str='localhost',
            port: int=5672, login: str='guest',
            password: str='guest', virtualhost: str='/',
            ssl: bool=False, loop=None, connection_class=Connection, **kwargs) -> Generator[Any, None, Connection]:
    """ Make connection to the broker

    :param url: `RFC3986`_ formatted broker address. When :class:`None` will be used keyword arguments.
    :param host: hostname of the broker
    :param port: broker port 5672 by default
    :param login: username string. `'guest'` by default.
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
        virtualhost = url.path[1:] if url.path else virtualhost

    connection = connection_class(
        host=host, port=port, login=login, password=password,
        virtual_host=virtualhost, ssl=ssl, loop=loop, **kwargs
    )

    yield from connection.connect()
    return connection


@asyncio.coroutine
def connect_url(url: str, loop=None) -> Connection:
    warnings.warn(
        'Please use connect(url) instead connect_url(url)', DeprecationWarning
    )

    return (yield from connect(url, loop=loop))


__all__ = ('connect', 'connect_url', 'Connection')
