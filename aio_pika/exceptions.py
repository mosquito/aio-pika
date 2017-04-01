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


__all__ = (
    'AMQPException', 'MessageProcessError', 'ProbableAuthenticationError',
    'AMQPChannelError', 'AMQPConnectionError', 'AMQPError', 'ChannelClosed', 'ChannelError',
    'AuthenticationError', 'BodyTooLongError', 'ConnectionClosed', 'ConsumerCancelled', 'DuplicateConsumerTag',
    'IncompatibleProtocolError', 'InvalidChannelNumber', 'InvalidFieldTypeException', 'InvalidFrameError',
    'InvalidMaximumFrameSize', 'InvalidMinimumFrameSize', 'MethodNotImplemented', 'NackError',
    'NoFreeChannels', 'ProbableAccessDeniedError', 'ProtocolSyntaxError', 'ProtocolVersionMismatch',
    'RecursionError', 'ShortStringTooLong', 'UnexpectedFrameError', 'UnroutableError',
    'UnspportedAMQPFieldException', 'UnsupportedAMQPFieldException',
)
