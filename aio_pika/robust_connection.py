import asyncio
import logging

from .connection import Connection, connect as _connect
from .robust_channel import RobustChannel


__all__ = ('connect', 'RobustConnection')
log = logging.getLogger(__name__)


class RobustConnection(Connection):

    CHANNEL_CLASS = RobustChannel

    __slots__ = ('__connection', '__connection_args', '__channels', '__loop')

    def __init__(self, host: str = 'localhost', port: int = 5672, login: str = 'guest', password: str = 'guest',
                 virtual_host: str = '/', ssl: bool = False, *, loop=None, **kwargs):

        self.__channels = set()
        super().__init__(host, port, login, password, virtual_host, ssl, loop=loop, **kwargs)

    def _on_connection_close(self, result):
        self._create_closing_future(force=True)
        self.loop.create_task(self.connect())

    @asyncio.coroutine
    def connect(self):
        yield from super().connect()
        self.add_close_callback(self._on_connection_close)

        for channel in self.__channels:
            yield from channel.set_connection(self)

    @asyncio.coroutine
    def channel(self):
        channel = yield from super().channel()
        self.__channels.add(channel)

        return channel


def connect(*args, **kwargs):
    return _connect(connection_class=RobustConnection, *args, **kwargs)

