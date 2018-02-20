from pika.exceptions import (
    ProbableAuthenticationError, AMQPChannelError, AMQPConnectionError, AMQPError,
    ChannelClosed, ChannelError, AuthenticationError, BodyTooLongError, ConnectionClosed, ConsumerCancelled,
    DuplicateConsumerTag, IncompatibleProtocolError, InvalidChannelNumber, InvalidFieldTypeException, InvalidFrameError,
    InvalidMaximumFrameSize, InvalidMinimumFrameSize, MethodNotImplemented, NackError, NoFreeChannels,
    ProbableAccessDeniedError, ProtocolSyntaxError, ProtocolVersionMismatch, RecursionError, ShortStringTooLong,
    UnexpectedFrameError, UnroutableError, UnspportedAMQPFieldException, UnsupportedAMQPFieldException
)


class AMQPException(Exception):
    pass


class MessageProcessError(AMQPException):
    pass


class QueueEmpty(AMQPException):
    pass


class TransactionClosed(AMQPException):
    pass


__all__ = (
    'AMQPChannelError',
    'AMQPConnectionError',
    'AMQPError',
    'AMQPException',
    'AuthenticationError',
    'BodyTooLongError',
    'ChannelClosed',
    'ChannelError',
    'ConnectionClosed',
    'ConsumerCancelled',
    'DuplicateConsumerTag',
    'IncompatibleProtocolError',
    'InvalidChannelNumber',
    'InvalidFieldTypeException',
    'InvalidFrameError',
    'InvalidMaximumFrameSize',
    'InvalidMinimumFrameSize',
    'MessageProcessError',
    'MethodNotImplemented',
    'NackError',
    'NoFreeChannels',
    'ProbableAccessDeniedError',
    'ProbableAuthenticationError',
    'ProtocolSyntaxError',
    'ProtocolVersionMismatch',
    'RecursionError',
    'ShortStringTooLong',
    'TransactionClosed',
    'UnexpectedFrameError',
    'UnroutableError',
    'UnspportedAMQPFieldException',
    'UnsupportedAMQPFieldException',
)
