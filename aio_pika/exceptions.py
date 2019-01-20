import asyncio

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


class MessageProcessError(AMQPException):
    pass


class QueueEmpty(AMQPException, asyncio.QueueEmpty):
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
