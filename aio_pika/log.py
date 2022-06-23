import logging


logger: logging.Logger = logging.getLogger("aio_pika")


def get_logger(name: str) -> logging.Logger:
    name = name.lstrip(logger.name)
    name = name.lstrip(".")
    return logger.getChild(name)
