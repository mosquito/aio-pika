import asyncio
import logging

from pika.exceptions import ChannelClosed

from aio_pika.common import State
from .connection import Connection, connect as _connect
from .robust_channel import RobustChannel


__all__ = ('connect', 'RobustConnection')
log = logging.getLogger(__name__)


class RobustConnection(Connection):

    CHANNEL_CLASS = RobustChannel
    RECONNECT_TIMEOUT = 5

    __slots__ = ('_connection', '__connection_args', '_channels', '__loop', '_state')

    def __init__(self, host: str = 'localhost', port: int = 5672, login: str = 'guest', password: str = 'guest',
                 virtual_host: str = '/', ssl: bool = False, *, loop=None, **kwargs):

        super().__init__(host, port, login, password, virtual_host, ssl, loop=loop, **kwargs)

    def _on_connection_close(self, _):
        if not self.is_initialized:
            self.loop.create_task(self.connect())

    def _on_channel_cleanup(self, channel):
        channel = self._channels[channel.channel_number]
        channel._futures.reject_all(ChannelClosed)

    @asyncio.coroutine
    def connect(self):
        while True:
            self._state = State.CONNECTING

            try:
                self._connection = yield from self._create_pika_connection()
                break
            except:
                self._state = State.INITIALIZED
                log.exception("Error when connecting to %r. Reconnecting after %s seconds",
                              self, self.RECONNECT_TIMEOUT)
                yield from asyncio.sleep(self.RECONNECT_TIMEOUT, loop=self.loop)

        self.add_close_callback(self._on_connection_close)
        self._state = State.READY

        for channel in self._channels.values():
            yield from channel.set_connection(self)


def connect(*args, **kwargs):
    return _connect(connection_class=RobustConnection, *args, **kwargs)
