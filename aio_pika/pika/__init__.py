__version__ = '0.10.0'

import logging
try:
    # not available in python 2.6
    from logging import NullHandler
except ImportError:

    class NullHandler(logging.Handler):

        def emit(self, record):
            pass

# Add NullHandler to prevent logging warnings
logging.getLogger(__name__).addHandler(NullHandler())

from .connection import ConnectionParameters
from .connection import URLParameters
from .credentials import PlainCredentials
from .spec import BasicProperties

from .adapters import BaseConnection
from .adapters import TornadoConnection
from .adapters import TwistedConnection
from .adapters import LibevConnection
