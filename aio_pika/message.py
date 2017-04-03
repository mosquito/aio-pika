import json
from enum import IntEnum, unique
from logging import getLogger
from pika import BasicProperties
from pika.channel import Channel
from contextlib import contextmanager
from .exceptions import MessageProcessError


log = getLogger(__name__)


@unique
class DeliveryMode(IntEnum):
    NOT_PERSISTENT = 1
    PERSISTENT = 2


class Message:
    """ AMQP message abstraction """

    __slots__ = (
        "body", "headers", "content_type", "content_encoding",
        "delivery_mode", "priority", "correlation_id", "reply_to",
        "expiration", "message_id", "timestamp", "type", "user_id", "app_id",
        "__lock"
    )

    def __init__(self, body: bytes, *, headers: dict = None, content_type: str = None,
                 content_encoding: str = None, delivery_mode: DeliveryMode = DeliveryMode.NOT_PERSISTENT,
                 priority: int = None, correlation_id=None,
                 reply_to: str = None, expiration: int = None,
                 message_id: str = None, timestamp: int = None,
                 type: str = None, user_id: str = None,
                 app_id: str = None):

        """ Creates a new instance of Message

        :param body: message body
        :param headers: message headers
        :param content_type: content type
        :param content_encoding: content encoding
        :param delivery_mode: delivery mode
        :param priority: priority
        :param correlation_id: correlation id
        :param reply_to: reply to
        :param expiration: expiration
        :param message_id: message id
        :param timestamp: timestamp
        :param type: type
        :param user_id: user id
        :param app_id: app id
        """

        self.__lock = False
        self.body = body if isinstance(body, bytes) else bytes(body)
        self.headers = headers
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.delivery_mode = DeliveryMode(int(delivery_mode)).value
        self.priority = priority
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        self.expiration = int(expiration) if expiration else None
        self.message_id = message_id
        self.timestamp = timestamp
        self.type = type
        self.user_id = user_id
        self.app_id = app_id

    def info(self):
        return {
            "body_size": len(self.body) if self.body else 0,
            "headers": self.headers,
            "content_type": self.content_type,
            "content_encoding": self.content_encoding,
            "delivery_mode": self.delivery_mode,
            "priority": self.priority,
            "correlation_id": self.correlation_id,
            "reply_to": self.reply_to,
            "expiration": self.expiration,
            "message_id": self.message_id,
            "timestamp": self.timestamp,
            "type": str(self.type),
            "user_id": self.user_id,
            "app_id": self.app_id,
        }

    @property
    def locked(self):
        """ is message locked

        :return: :class:`bool`
        """
        return self.__lock

    @property
    def properties(self):
        return BasicProperties(
            content_type=self.content_type,
            content_encoding=self.content_encoding,
            headers=self.headers,
            delivery_mode=self.delivery_mode,
            priority=self.priority,
            correlation_id=self.correlation_id,
            reply_to=self.reply_to,
            expiration=str(self.expiration) if self.expiration else None,
            message_id=self.message_id,
            timestamp=self.timestamp,
            type=self.type,
            user_id=self.user_id,
            app_id=self.app_id
        )

    def __repr__(self):
        return "{name}:{repr}".format(
            name=self.__class__.__name__,
            repr=json.dumps(self.info(), indent=1, sort_keys=True)
        )

    def __setattr__(self, key, value):
        if not key.startswith("_") and self.locked:
            raise ValueError("Message is locked")

        return super().__setattr__(key, value)

    def __iter__(self):
        return iter(self.body)

    def lock(self):
        self.__lock = True

    def __copy__(self):
        return Message(
            body=self.body, headers={k: v for k, v in self.headers.items()},
            content_encoding=self.content_encoding, content_type=self.content_type,
            delivery_mode=self.delivery_mode, priority=self.priority,
            correlation_id=self.correlation_id, reply_to=self.reply_to,
            expiration=self.expiration, message_id=self.message_id,
            timestamp=self.timestamp, type=self.type, user_id=self.user_id,
            app_id=self.app_id
        )


class IncomingMessage(Message):
    """ Incoming message it's seems like Message but has additional methods for message acknowledgement.

    Depending on the acknowledgement mode used, RabbitMQ can consider a
    message to be successfully delivered either immediately after it is sent
    out (written to a TCP socket) or when an explicit ("manual") client
    acknowledgement is received. Manually sent acknowledgements can be
    positive or negative and use one of the following protocol methods:

    * basic.ack is used for positive acknowledgements
    * basic.nack is used for negative acknowledgements (note: this is a RabbitMQ extension to AMQP 0-9-1)
    * basic.reject is used for negative acknowledgements but has one limitations compared to basic.nack

    Positive acknowledgements simply instruct RabbitMQ to record a message as delivered.
    Negative acknowledgements with basic.reject have the same effect.
    The difference is primarily in the semantics: positive acknowledgements assume a message was
    successfully processed while their negative counterpart suggests that a delivery wasn't
    processed but still should be deleted.

    """
    __slots__ = (
        '_loop', '__channel', 'cluster_id', 'consumer_tag',
        'delivery_tag', 'exchange', 'routing_key', 'synchronous',
        'redelivered', '__no_ack', '__processed'
    )

    def __init__(self, channel: Channel, envelope, properties, body, no_ack: bool = False):
        """ Create an instance of :class:`IncomingMessage`

        :param channel: :class:`aio_pika.channel.Channel`
        :param envelope: pika envelope
        :param properties: properties
        :param body: message body
        :param no_ack: no ack needed

        """
        self.__channel = channel
        self.__no_ack = no_ack
        self.__processed = False

        super().__init__(
            body=body,
            content_type=properties.content_type,
            content_encoding=properties.content_encoding,
            headers=properties.headers,
            delivery_mode=properties.delivery_mode,
            priority=properties.priority,
            correlation_id=properties.correlation_id,
            reply_to=properties.reply_to,
            expiration=properties.expiration,
            message_id=properties.message_id,
            timestamp=properties.timestamp,
            type=properties.type,
            user_id=properties.user_id,
            app_id=properties.app_id,
        )

        self.cluster_id = properties.cluster_id
        self.consumer_tag = getattr(envelope, 'consumer_tag', None)
        self.delivery_tag = envelope.delivery_tag
        self.exchange = envelope.exchange
        self.routing_key = envelope.routing_key
        self.redelivered = envelope.redelivered
        self.synchronous = envelope.synchronous

    @contextmanager
    def process(self, requeue=False, reject_on_redelivered=False):
        """ Context manager for processing the message

            >>> def on_message_received(message: IncomingMessage):
            ...    with message.process():
            ...        # When exception will be raised
            ...        # the message will be rejected
            ...        print(message.body)

        :param requeue: Requeue message when exception.
        :param reject_on_redelivered: When True message will be rejected only when message was redelivered.

        """
        try:
            yield self
            self.ack()
        except:
            if reject_on_redelivered and self.redelivered:
                log.info("Message %r was redelivered and will be rejected.", self)
                self.reject(requeue=False)

            self.reject(requeue=requeue)
            raise

    def ack(self):
        """ Send basic.ack is used for positive acknowledgements

        :return: None
        """
        if self.__no_ack:
            log.warning("Can't ack message with \"no_ack\" flag")
            return False

        if self.__processed:
            raise MessageProcessError("Message already processed")
        else:
            self.__channel.basic_ack(delivery_tag=self.delivery_tag)
            self.__processed = True
            if not self.locked:
                self.lock()

    def reject(self, requeue=False):
        """ When `requeue=True` the message will be returned to queue. Otherwise message will be dropped.

        :param requeue: bool
        """
        if self.__no_ack:
            raise TypeError('This message has "no_ack" flag.')

        if self.__processed:
            raise MessageProcessError("Message already processed")
        else:
            self.__channel.basic_reject(delivery_tag=self.delivery_tag, requeue=requeue)
            self.__processed = True
            if not self.locked:
                self.lock()

    def info(self):
        """ Method returns dict representation of the message """

        info = super(IncomingMessage, self).info()
        info['cluster_id'] = self.cluster_id
        info['consumer_tag'] = self.consumer_tag
        info['delivery_tag'] = self.delivery_tag
        info['exchange'] = self.exchange
        info['redelivered'] = self.redelivered
        info['routing_key'] = self.routing_key
        info['synchronous'] = self.synchronous
        return info


__all__ = 'Message', 'IncomingMessage',
