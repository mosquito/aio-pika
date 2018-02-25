import asyncio
import logging
import platform
from contextlib import contextmanager

from pika.adapters import asyncio_connection
from pika import channel

from .version import __version__


LOGGER = logging.getLogger(__name__)

PRODUCT = 'aio-pika'


class AsyncioConnection(asyncio_connection.AsyncioConnection):

    def __init__(self, parameters=None, on_open_callback=None,
                 on_open_error_callback=None, on_close_callback=None,
                 stop_ioloop_on_close=False, loop: asyncio.AbstractEventLoop=None):

        super().__init__(
            parameters=parameters,
            on_open_callback=on_open_callback,
            on_open_error_callback=on_open_error_callback,
            on_close_callback=on_close_callback,
            stop_ioloop_on_close=stop_ioloop_on_close,
            custom_ioloop=loop,
        )

        self.channel_cleanup_callback = None

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

    def _create_channel(self, channel_number, on_open_callback):
        LOGGER.debug('Creating channel %s', channel_number)
        return Channel(self, channel_number, on_open_callback)


class Channel(channel.Channel):
    def __init__(self, connection, channel_number, on_open_callback=None):
        super().__init__(connection, channel_number, on_open_callback=on_open_callback)
        self._consume_results = {}
        self._on_getempty_callback = None

    def _on_eventok(self, method_frame):
        callback = self._consume_results.pop(method_frame.method.consumer_tag, None)

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
            LOGGER.debug("Unexcpected getempty frame %r", method_frame)

        super()._on_getempty(method_frame)

    @contextmanager
    def set_get_empty_callback(self, callback):
        self._on_getempty_callback = callback
        try:
            yield
        finally:
            self._on_getempty_callback = None
