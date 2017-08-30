import logging
import platform

from pika.adapters.asyncio_connection import AsyncioConnection as _AsyncioConnection
from .version import __version__


LOGGER = logging.getLogger(__name__)

PRODUCT = 'aio-pika'


class AsyncioConnection(_AsyncioConnection):

    def __init__(self, *args, **kwargs):
        self.channel_cleanup_callback = None
        super().__init__(*args, **kwargs)

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
