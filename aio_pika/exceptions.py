import asyncio

import pamqp
from aiormq.exceptions import (
    AMQPChannelError,
    AMQPConnectionError,
    AMQPError,
    AMQPException,
    AuthenticationError,
    ChannelClosed,
    ConnectionClosed,
    DeliveryError,
    DuplicateConsumerTag,
    IncompatibleProtocolError,
    InvalidFrameError,
    MethodNotImplemented,
    ProbableAuthenticationError,
    ProtocolSyntaxError,
    ChannelPreconditionFailed,
    ChannelNotFoundEntity
)

PAMQP_EXCEPTIONS = (pamqp.exceptions.PAMQPException,) + tuple(
    pamqp.specification.ERRORS.values()
)
CONNECTION_EXCEPTIONS = (ConnectionError, AMQPError) + PAMQP_EXCEPTIONS


class MessageProcessError(AMQPError):
    pass


class QueueEmpty(AMQPError, asyncio.QueueEmpty):
    pass


__all__ = (
    'AMQPChannelError',
    'AMQPConnectionError',
    'AMQPError',
    'AMQPException',
    'AuthenticationError',
    'ChannelClosed',
    'ConnectionClosed',
    'DeliveryError',
    'DuplicateConsumerTag',
    'IncompatibleProtocolError',
    'InvalidFrameError',
    'MessageProcessError',
    'MethodNotImplemented',
    'ProbableAuthenticationError',
    'ProtocolSyntaxError',
    'QueueEmpty',
    'ChannelPreconditionFailed',
    'ChannelNotFoundEntity',
)
