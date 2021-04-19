import warnings

from .abc import *  # noqa


warnings.warn(
    "aio_pika.types was deprecated and will be removed in "
    "one of next major releases. Use aio_pika.abc instead.",
    category=DeprecationWarning,
    stacklevel=2,
)
