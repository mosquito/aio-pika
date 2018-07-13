__version__ = '0.10.0'

import logging
from logging import NullHandler

# Add NullHandler to prevent logging warnings
logging.getLogger(__name__).addHandler(NullHandler())

from .connection import ConnectionParameters
from .connection import URLParameters
from .credentials import PlainCredentials
from .spec import BasicProperties
from .adapters import BaseConnection
