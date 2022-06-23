import logging

import pytest

from aio_pika import set_log_level


@pytest.mark.parametrize("level", [
    logging.DEBUG, logging.INFO, logging.WARNING,
    logging.CRITICAL, logging.FATAL,
], ids=["debug", "info", "warning", "critical", "fatal"])
def test_set_log_level(level):
    loggers = []
    from aio_pika.channel import log
    loggers.append(log)
    from aio_pika.connection import log
    loggers.append(log)
    from aio_pika.exchange import log
    loggers.append(log)

    set_log_level(logging.WARNING)

    try:
        for logger in loggers:
            assert logger.getEffectiveLevel() == logging.WARNING
    finally:
        set_log_level(logging.getLogger().getEffectiveLevel())
