import asyncio

import pamqp.exceptions
from aiormq.exceptions import (
    AMQPChannelError, AMQPConnectionError, AMQPError, AMQPException,
    AuthenticationError, ChannelClosed, ChannelInvalidStateError,
    ChannelNotFoundEntity, ChannelPreconditionFailed, ConnectionClosed,
    DeliveryError, DuplicateConsumerTag, IncompatibleProtocolError,
    InvalidFrameError, MethodNotImplemented, ProbableAuthenticationError,
    ProtocolSyntaxError, PublishError,
)


CONNECTION_EXCEPTIONS = (
    AMQPError,
    ConnectionError,
    OSError,
    RuntimeError,
    pamqp.exceptions.PAMQPException,
)


class MessageProcessError(AMQPError):
    pass


class QueueEmpty(AMQPError, asyncio.QueueEmpty):
    pass


__all__ = (
    "AMQPChannelError",
    "AMQPConnectionError",
    "AMQPError",
    "AMQPException",
    "AuthenticationError",
    "ChannelClosed",
    "ChannelInvalidStateError",
    "ConnectionClosed",
    "DeliveryError",
    "PublishError",
    "DuplicateConsumerTag",
    "IncompatibleProtocolError",
    "InvalidFrameError",
    "MessageProcessError",
    "MethodNotImplemented",
    "ProbableAuthenticationError",
    "ProtocolSyntaxError",
    "QueueEmpty",
    "ChannelPreconditionFailed",
    "ChannelNotFoundEntity",
)
