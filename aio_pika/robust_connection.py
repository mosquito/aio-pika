import asyncio
from functools import wraps
from logging import getLogger
from typing import Callable

from .adapter import AsyncioConnection
from .connection import Connection
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

    RECONNECT_INTERVAL = 1
    CHANNEL_CLASS = RobustChannel

    def __init__(self, host: str = 'localhost', port: int = 5672, login: str = 'guest',
                 password: str = 'guest', virtual_host: str = '/',
                 ssl: bool = False, *, loop=None, **kwargs):

        super().__init__(host=host, port=port, login=login, password=password,
                         virtual_host=virtual_host, ssl=ssl, loop=loop, **kwargs)

        self._closed = False
        self._on_close_callbacks = set()

    def add_close_callback(self, callback: Callable[[], None]):
        """ Add callback which will be called after connection will be closed.

        :return: None
        """

        self._on_close_callbacks.add(lambda c: callback(c))

    def _on_connection_lost(self, future: asyncio.Future, connection: AsyncioConnection, code, reason):
        if self._closed:
            return super()._on_connection_lost(future, connection, code, reason)

        if not future.done():
            future.set_result(None)

        self.loop.call_later(
            self.RECONNECT_INTERVAL,
            lambda: self.loop.create_task(self.connect())
        )

    @asyncio.coroutine
    def connect(self):
        result = yield from super().connect()

        for number, channel in self._channels.items():
            channel.on_reconnect(self, number)

        return result

    @property
    def is_closed(self):
        """ Is this connection are closed """

        return self._closed or super().is_closed

    @property
    @asyncio.coroutine
    def closing(self):
        """ Return future which will be finished after connection close. """
        return (yield from self._closing)

    @asyncio.coroutine
    def close(self) -> None:
        """ Close AMQP connection """
        self._closed = True
        yield from super().close()


__all__ = 'RobustConnection',