
class AMQPError(Exception):
    message = 'An unspecified AMQP error has occurred'

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.message % self.args)


class AMQPConnectionError(AMQPError):
    message = 'Connection can not be opened'


class IncompatibleProtocolError(AMQPConnectionError):
    message = 'The protocol returned by the server is not supported'


class AuthenticationError(AMQPConnectionError):
    message = (
        'Server and client could not negotiate use of the '
        '%s authentication mechanism'
    )


class ProbableAuthenticationError(AMQPConnectionError):
    message = (
        'Client was disconnected at a connection stage indicating a '
        'probable authentication error'
    )


class ProbableAccessDeniedError(AMQPConnectionError):
    message = ('Client was disconnected at a connection stage indicating a '
               'probable denial of access to the specified virtual host')


class NoFreeChannels(AMQPConnectionError):
    message = 'The connection has run out of free channels'


class ConnectionClosed(AMQPConnectionError):
    message = 'The AMQP connection was closed (%s) %s'


class AMQPChannelError(AMQPError):
    message = 'An unspecified AMQP channel error has occurred'


class ChannelClosed(AMQPChannelError):
    message = 'The channel was closed (%s) %s'


class DuplicateConsumerTag(AMQPChannelError):
    message = ('The consumer tag specified already exists for this '
               'channel: %s')


class ConsumerCancelled(AMQPChannelError):
    message = 'Server cancelled consumer'


class UnroutableError(AMQPChannelError):
    message = '%s: %i unroutable messages returned by broker'


class NackError(UnroutableError):
    pass


class InvalidChannelNumber(AMQPError):
    message = 'An invalid channel number has been specified: %s'


class ProtocolSyntaxError(AMQPError):
    message = 'An unspecified protocol syntax error occurred'


class UnexpectedFrameError(ProtocolSyntaxError):
    message = 'Received a frame out of sequence: %r'


class ProtocolVersionMismatch(ProtocolSyntaxError):
    message = 'Protocol versions did not match: %r vs %r'


class BodyTooLongError(ProtocolSyntaxError):
    message = ('Received too many bytes for a message delivery: '
               'Received %i, expected %i')


class InvalidFrameError(ProtocolSyntaxError):
    message = 'Invalid frame received: %r'


class InvalidFieldTypeException(ProtocolSyntaxError):
    message = 'Unsupported field kind %s'


class UnsupportedAMQPFieldException(ProtocolSyntaxError):
    message = 'Unsupported field kind %s'


class MethodNotImplemented(AMQPError):
    pass


class ChannelError(AMQPError):
    message = 'An unspecified error occurred with the Channel'


class InvalidMinimumFrameSize(ProtocolSyntaxError):
    message = 'AMQP Minimum Frame Size is 4096 Bytes'


class InvalidMaximumFrameSize(ProtocolSyntaxError):
    message = 'AMQP Maximum Frame Size is 131072 Bytes'


class AMQPException(Exception):
    pass


class MessageProcessError(AMQPException):
    pass


class DeliveryError(AMQPException):
    __slots__ = 'channel_number', 'delivery_tag'

    def __init__(self, method_frame):
        self.channel_number = method_frame.channel_number
        self.delivery_tag = method_frame.method.delivery_tag


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
    'QueueEmpty',
    'TransactionClosed',
    'UnexpectedFrameError',
    'UnroutableError',
    'UnsupportedAMQPFieldException',
)
