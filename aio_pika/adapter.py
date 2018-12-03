import asyncio
import logging
import platform
from contextlib import contextmanager
from functools import partial

from .pika.adapters import base_connection
from .pika import channel

from .version import __version__


log = logging.getLogger(__name__)

PRODUCT = 'aio-pika'


class IOLoopAdapter:
    __slots__ = 'loop', 'handlers', 'readers', 'writers'

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.handlers = {}
        self.readers = set()
        self.writers = set()

    def add_timeout(self, deadline, callback_method):
        return self.loop.call_later(deadline, callback_method)

    @staticmethod
    def remove_timeout(handle: asyncio.Handle):
        return handle.cancel()

    def add_handler(self, fd, cb, event_state):
        if fd in self.handlers:
            raise ValueError("fd {} added twice".format(fd))
        self.handlers[fd] = cb

        if event_state & base_connection.BaseConnection.READ:
            self.loop.add_reader(
                fd,
                partial(
                    cb,
                    fd=fd,
                    events=base_connection.BaseConnection.READ
                )
            )
            self.readers.add(fd)

        if event_state & base_connection.BaseConnection.WRITE:
            self.loop.add_writer(
                fd,
                partial(
                    cb,
                    fd=fd,
                    events=base_connection.BaseConnection.WRITE
                )
            )
            self.writers.add(fd)

    def remove_handler(self, fd):
        if fd not in self.handlers:
            return

        if fd in self.readers:
            self.loop.remove_reader(fd)
            self.readers.remove(fd)

        if fd in self.writers:
            self.loop.remove_writer(fd)
            self.writers.remove(fd)

        del self.handlers[fd]

    def update_handler(self, fd, event_state):
        if event_state & base_connection.BaseConnection.READ:
            if fd not in self.readers:
                self.loop.add_reader(
                    fd,
                    partial(
                        self.handlers[fd],
                        fd=fd,
                        events=base_connection.BaseConnection.READ
                    )
                )
                self.readers.add(fd)
        else:
            if fd in self.readers:
                self.loop.remove_reader(fd)
                self.readers.remove(fd)

        if event_state & base_connection.BaseConnection.WRITE:
            if fd not in self.writers:
                self.loop.add_writer(
                    fd,
                    partial(
                        self.handlers[fd],
                        fd=fd,
                        events=base_connection.BaseConnection.WRITE
                    )
                )
                self.writers.add(fd)
        else:
            if fd in self.writers:
                self.loop.remove_writer(fd)
                self.writers.remove(fd)

    def start(self):
        if self.loop.is_running():
            return

        self.loop.run_forever()

    def stop(self):
        if self.loop.is_closed():
            return

        self.loop.stop()


class AsyncioConnection(base_connection.BaseConnection):

    def __init__(self, parameters=None, on_open_callback=None,
                 on_open_error_callback=None,
                 on_close_callback=None,
                 stop_ioloop_on_close=False, loop=None):

        self.sleep_counter = 0
        self.loop = loop or asyncio.get_event_loop()
        self.ioloop = IOLoopAdapter(self.loop)
        self.channel_cleanup_callback = None
        self.channel_cancel_callback = None

        super().__init__(parameters, on_open_callback,
                         on_open_error_callback,
                         on_close_callback, self.ioloop,
                         stop_ioloop_on_close=stop_ioloop_on_close)

    def _adapter_connect(self):
        error = super()._adapter_connect()

        if not error:
            self.ioloop.add_handler(
                self.socket.fileno(), self._handle_events, self.event_state
            )

        return error

    def _adapter_disconnect(self):
        if self.socket:
            self.ioloop.remove_handler(self.socket.fileno())

        super()._adapter_disconnect()

    def _handle_disconnect(self):
        try:
            super()._handle_disconnect()
            super()._handle_write()
        except Exception as e:
            self._on_disconnect(-1, e)

    @property
    def _client_properties(self) -> dict:
        """ Return the client properties dictionary. """
        return {
            'product': PRODUCT,
            'platform': 'Python %s' % platform.python_version(),
            'capabilities': {
                'authentication_failure_close': True,
                'basic.nack': True,
                'connection.blocked': True,
                'consumer_cancel_notify': True,
                'publisher_confirms': True
            },
            'information': 'See https://aio-pika.readthedocs.io/',
            'version': __version__
        }

    def _on_channel_cleanup(self, channel):
        try:
            if self.channel_cleanup_callback:
                self.channel_cleanup_callback(channel)
        finally:
            super()._on_channel_cleanup(channel)

    def _on_channel_cancel(self, channel):
        if self.channel_cancel_callback:
            self.channel_cancel_callback(channel)

    def _create_channel(self, channel_number, on_open_callback):
        log.debug('Creating channel %s', channel_number)
        channel = Channel(self, channel_number, on_open_callback)
        channel.add_on_cancel_callback(
            lambda method_frame: self._on_channel_cancel(channel)
        )
        return channel


class Channel(channel.Channel):
    def __init__(self, connection, channel_number, on_open_callback=None):
        super().__init__(connection, channel_number,
                         on_open_callback=on_open_callback)
        self._consume_results = {}
        self._on_getempty_callback = None

    def _on_eventok(self, method_frame):
        callback = self._consume_results.pop(
            method_frame.method.consumer_tag, None
        )

        if callback:
            callback(method_frame)

        return super()._on_eventok(method_frame)

    def basic_consume(self, consumer_callback,
                      queue='',
                      no_ack=False,
                      exclusive=False,
                      consumer_tag=None,
                      arguments=None,
                      result_callback=None):

        if not consumer_tag:
            consumer_tag = self._generate_consumer_tag()

        self._consume_results[consumer_tag] = result_callback

        return super().basic_consume(
            consumer_callback=consumer_callback,
            queue=queue,
            no_ack=no_ack,
            exclusive=exclusive,
            consumer_tag=consumer_tag,
            arguments=arguments
        )

    def _on_getempty(self, method_frame):
        if self._on_getempty_callback:
            self._on_getempty_callback(method_frame)
        else:
            log.debug("Unexpected getempty frame %r", method_frame)

        super()._on_getempty(method_frame)

    @contextmanager
    def set_get_empty_callback(self, callback):
        self._on_getempty_callback = callback
        try:
            yield
        finally:
            self._on_getempty_callback = None
