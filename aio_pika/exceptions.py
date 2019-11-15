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
    PublishError,
    DuplicateConsumerTag,
    IncompatibleProtocolError,
    InvalidFrameError,
    MethodNotImplemented,
    ProbableAuthenticationError,
    ProtocolSyntaxError,
    ChannelPreconditionFailed,
    ChannelNotFoundEntity
)

PAMQP_EXCEPTIONS = (
    pamqp.exceptions.PAMQPException,
) + tuple(pamqp.specification.ERRORS.values())

CONNECTION_EXCEPTIONS = (
    RuntimeError,
    ConnectionError,
    AMQPError,
) + PAMQP_EXCEPTIONS


class MessageProcessError(AMQPError):
    pass


class QueueEmpty(AMQPError, asyncio.QueueEmpty):
    pass


class MaxReconnectAttemptsReached(Exception):
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
    'PublishError',
    'DuplicateConsumerTag',
    'IncompatibleProtocolError',
    'InvalidFrameError',
    'MaxReconnectAttemptsReached',
    'MessageProcessError',
    'MethodNotImplemented',
    'ProbableAuthenticationError',
    'ProtocolSyntaxError',
    'QueueEmpty',
    'ChannelPreconditionFailed',
    'ChannelNotFoundEntity',
)
