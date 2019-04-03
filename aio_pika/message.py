import asyncio
import json
import time
from collections.abc import Mapping
from datetime import datetime, timedelta
from enum import IntEnum, unique
from functools import singledispatch
from logging import getLogger
from pprint import pformat
from typing import Union, Optional, Any, Callable, Dict, Iterable
from warnings import warn

from typing import AsyncContextManager

import aiormq
from aiormq.types import DeliveredMessage

from .exceptions import MessageProcessError

log = getLogger(__name__)
NoneType = type(None)


@unique
class DeliveryMode(IntEnum):
    NOT_PERSISTENT = 1
    PERSISTENT = 2


DateType = Union[int, datetime, float, timedelta, None]

MICROSECONDS = 1000


def to_microseconds(seconds):
    return int(seconds * MICROSECONDS)


@singledispatch
def encode_expiration(value) -> Optional[str]:
    raise ValueError('Invalid timestamp type: %r' % type(value), value)


@encode_expiration.register(datetime)
def _(value):
    now = datetime.now()
    return str(to_microseconds((value - now).total_seconds()))


@encode_expiration.register(int)
@encode_expiration.register(float)
def _(value):
    return str(to_microseconds(value))


@encode_expiration.register(timedelta)
def _(value):
    return str(int(value.total_seconds() * 1000))


@encode_expiration.register(type(None))
def _(_):
    return None


ZERO_TIME = datetime.utcfromtimestamp(0)


@singledispatch
def decode_expiration(t) -> Optional[float]:
    raise ValueError('Invalid expiration type: %r' % type(t), t)


@decode_expiration.register(time.struct_time)
def _(t: time.struct_time) -> float:
    return (datetime(*t[:7]) - ZERO_TIME).total_seconds()


@decode_expiration.register(str)
def _(t: str) -> float:
    return float(t)


@singledispatch
def encode_timestamp(value) -> Optional[time.struct_time]:
    raise ValueError('Invalid timestamp type: %r' % type(value), value)


@encode_timestamp.register(time.struct_time)
def _(value):
    return value


@encode_timestamp.register(datetime)
def _(value):
    return value.timetuple()


@encode_timestamp.register(float)
@encode_timestamp.register(int)
def _(value):
    return datetime.utcfromtimestamp(value).timetuple()


@encode_timestamp.register(timedelta)
def _(value):
    return datetime.utcnow() + value


@encode_timestamp.register(type(None))
def _(_):
    return None


@singledispatch
def decode_timestamp(value) -> Optional[datetime]:
    raise ValueError('Invalid timestamp type: %r' % type(value), value)


@decode_timestamp.register(datetime)
def _(value):
    return value


@decode_timestamp.register(float)
@decode_timestamp.register(int)
def _(value):
    return datetime.utcfromtimestamp(value)


@decode_timestamp.register(time.struct_time)
def _(value: time.struct_time):
    return datetime(*value[:6])


@decode_timestamp.register(type(None))
def _(_):
    return None


def optional(value, func: Callable[[Any], Any]=str, default=None):
    return func(value) if value else default


class HeaderProxy(Mapping):
    def __init__(self, headers: Dict[str, bytes]):
        self._headers = headers   # type: Dict[str, bytes]
        self._cache = {}          # type: Dict[str, Any]

    def __getitem__(self, k):
        if k not in self._headers:
            raise KeyError(k)

        if k not in self._cache:
            value = self._headers[k]
            if isinstance(value, bytes):
                self._cache[k] = value.decode()
            else:
                self._cache[k] = value

        return self._cache[k]

    def __setitem__(self, key, value):
        self._headers[key] = format_headers(value)
        self._cache.pop(key, None)

    def __len__(self) -> int:
        return len(self._headers)

    def __iter__(self):
        for key in self._headers:
            yield key


@singledispatch
def header_converter(value: Any) -> bytes:
    return json.dumps(
        value, separators=(',', ":"),
        ensure_ascii=False, default=repr
    ).encode()


@header_converter.register(bytes)
@header_converter.register(datetime)
@header_converter.register(NoneType)
@header_converter.register(list)
@header_converter.register(int)
def _(v: bytes):
    return v


@header_converter.register(bytearray)
def _(v: bytes):
    return bytes(v)


@header_converter.register(str)
def _(v):
    return v.encode()


@header_converter.register(set)
@header_converter.register(tuple)
@header_converter.register(frozenset)
def _(v: Iterable):
    return header_converter(list(v))


def format_headers(d: Dict[str, Any]) -> Dict[str, bytes]:
    ret = {}

    if not d:
        return ret

    for key, value in d.items():
        ret[key] = header_converter(value)
    return ret


class Message:
    """ AMQP message abstraction """

    __slots__ = (
        "app_id", "body", "body_size", "content_encoding", "content_type",
        "correlation_id", "delivery_mode", "expiration", "headers",
        "headers_raw", "message_id", "priority", "reply_to", "timestamp",
        "type", "user_id", "__lock",
    )

    def __init__(self, body: bytes, *, headers: dict=None,
                 content_type: str=None, content_encoding: str=None,
                 delivery_mode: DeliveryMode=None,
                 priority: int=None, correlation_id=None,
                 reply_to: str=None, expiration: DateType=None,
                 message_id: str=None,
                 timestamp: DateType=None,
                 type: str=None, user_id: str=None,
                 app_id: str=None):

        """ Creates a new instance of Message

        :param body: message body
        :param headers: message headers
        :param headers_raw: message raw headers
        :param content_type: content type
        :param content_encoding: content encoding
        :param delivery_mode: delivery mode
        :param priority: priority
        :param correlation_id: correlation id
        :param reply_to: reply to
        :param expiration: expiration in seconds (or datetime or timedelta)
        :param message_id: message id
        :param timestamp: timestamp
        :param type: type
        :param user_id: user id
        :param app_id: app id
        """

        self.__lock = False
        self.body = body if isinstance(body, bytes) else bytes(body)
        self.body_size = len(self.body) if self.body else 0
        self.headers_raw = format_headers(headers)
        self.headers = HeaderProxy(self.headers_raw)
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.delivery_mode = DeliveryMode(
            optional(
                delivery_mode,
                func=int,
                default=DeliveryMode.NOT_PERSISTENT)
        ).value
        self.priority = optional(priority, int, 0)
        self.correlation_id = optional(correlation_id)
        self.reply_to = optional(reply_to)
        self.expiration = expiration
        self.message_id = optional(message_id)
        self.timestamp = encode_timestamp(timestamp)
        self.type = optional(type)
        self.user_id = optional(user_id)
        self.app_id = optional(app_id)

    @staticmethod
    def _as_bytes(value):
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode()
        elif isinstance(value, NoneType):
            return b''
        else:
            return str(value).encode()

    def info(self) -> dict:
        """ Create a dict with message attributes

        ::

            {
                "body_size": 100,
                "headers": {},
                "content_type": "text/plain",
                "content_encoding": "",
                "delivery_mode": DeliveryMode.NOT_PERSISTENT,
                "priority": 0,
                "correlation_id": "",
                "reply_to": "",
                "expiration": "",
                "message_id": "",
                "timestamp": "",
                "type": "",
                "user_id": "",
                "app_id": "",
            }

        """

        return {
            "body_size": self.body_size,
            "headers": self.headers_raw,
            "content_type": self.content_type,
            "content_encoding": self.content_encoding,
            "delivery_mode": self.delivery_mode,
            "priority": self.priority,
            "correlation_id": self.correlation_id,
            "reply_to": self.reply_to,
            "expiration": self.expiration,
            "message_id": self.message_id,
            "timestamp": decode_timestamp(self.timestamp),
            "type": str(self.type),
            "user_id": self.user_id,
            "app_id": self.app_id,
        }

    @property
    def locked(self) -> bool:
        """ is message locked

        :return: :class:`bool`
        """
        return bool(self.__lock)

    @property
    def properties(self) -> aiormq.spec.Basic.Properties:
        """ Build :class:`pika.BasicProperties` object """
        return aiormq.spec.Basic.Properties(
            content_type=self.content_type,
            content_encoding=self.content_encoding,
            headers=self.headers_raw,
            delivery_mode=self.delivery_mode,
            priority=self.priority,
            correlation_id=self.correlation_id,
            reply_to=self.reply_to,
            expiration=encode_expiration(self.expiration),
            message_id=self.message_id,
            timestamp=self.timestamp,
            message_type=self.type,
            user_id=self.user_id,
            app_id=self.app_id
        )

    def __repr__(self):
        return "{name}:{repr}".format(
            name=self.__class__.__name__,
            repr=pformat(self.info())
        )

    def __setattr__(self, key, value):
        if not key.startswith("_") and self.locked:
            raise ValueError("Message is locked")

        return super().__setattr__(key, value)

    def __iter__(self):
        return iter(self.body)

    def lock(self):
        """ Set lock flag to `True`"""
        self.__lock = True

    def __copy__(self):
        return Message(
            body=self.body,
            headers={
                k: v for k, v in self.headers.items()
            } if self.headers else {},
            content_encoding=self.content_encoding,
            content_type=self.content_type,
            delivery_mode=self.delivery_mode,
            priority=self.priority,
            correlation_id=self.correlation_id,
            reply_to=self.reply_to,
            expiration=self.expiration,
            message_id=self.message_id,
            timestamp=self.timestamp,
            type=self.type,
            user_id=self.user_id,
            app_id=self.app_id
        )


class IncomingMessage(Message):
    """ Incoming message it's seems like Message but has additional methods for
    message acknowledgement.

    Depending on the acknowledgement mode used, RabbitMQ can consider a
    message to be successfully delivered either immediately after it is sent
    out (written to a TCP socket) or when an explicit ("manual") client
    acknowledgement is received. Manually sent acknowledgements can be
    positive or negative and use one of the following protocol methods:

    * basic.ack is used for positive acknowledgements
    * basic.nack is used for negative acknowledgements (note: this is a RabbitMQ
      extension to AMQP 0-9-1)
    * basic.reject is used for negative acknowledgements but has one limitations
      compared to basic.nack

    Positive acknowledgements simply instruct RabbitMQ to record a message as
    delivered. Negative acknowledgements with basic.reject have the same effect.
    The difference is primarily in the semantics: positive acknowledgements
    assume a message was successfully processed while their negative
    counterpart suggests that a delivery wasn't processed but still should
    be deleted.

    """
    __slots__ = (
        '_loop', '__channel', 'cluster_id', 'consumer_tag',
        'delivery_tag', 'exchange', 'routing_key', 'redelivered',
        '__no_ack', '__processed', 'message_count'
    )

    def __init__(self, message: DeliveredMessage, no_ack: bool=False):
        """ Create an instance of :class:`IncomingMessage` """

        self.__channel = message.channel
        self.__no_ack = no_ack
        self.__processed = False

        expiration = None       # type: time.struct_time
        if message.header.properties.expiration:
            expiration = decode_expiration(message.header.properties.expiration)

        super().__init__(
            body=message.body,
            content_type=message.header.properties.content_type,
            content_encoding=message.header.properties.content_encoding,
            headers=message.header.properties.headers,
            delivery_mode=message.header.properties.delivery_mode,
            priority=message.header.properties.priority,
            correlation_id=message.header.properties.correlation_id,
            reply_to=message.header.properties.reply_to,
            expiration=expiration / 1000. if expiration else None,
            message_id=message.header.properties.message_id,
            timestamp=decode_timestamp(message.header.properties.timestamp),
            type=message.header.properties.message_type,
            user_id=message.header.properties.user_id,
            app_id=message.header.properties.app_id,
        )

        self.cluster_id = message.header.properties.cluster_id

        self.cluster_id = None
        self.consumer_tag = None
        self.delivery_tag = None
        self.exchange = None
        self.routing_key = None
        self.redelivered = None
        self.message_count = None

        if isinstance(message.delivery, aiormq.spec.Basic.GetOk):
            self.message_count = message.delivery.message_count
            self.delivery_tag = message.delivery.delivery_tag
            self.redelivered = message.delivery.redelivered
        elif isinstance(message.delivery, aiormq.spec.Basic.Deliver):
            self.consumer_tag = message.delivery.consumer_tag
            self.delivery_tag = message.delivery.delivery_tag
            self.redelivered = message.delivery.redelivered

        self.routing_key = message.delivery.routing_key
        self.exchange = message.delivery.exchange

        if no_ack or not self.delivery_tag:
            self.lock()
            self.__processed = True

    def process(self, requeue=False, reject_on_redelivered=False,
                ignore_processed=False):
        """ Context manager for processing the message

            >>> async def on_message_received(message: IncomingMessage):
            ...    async with message.process():
            ...        # When exception will be raised
            ...        # the message will be rejected
            ...        print(message.body)

        Example with ignore_processed=True

            >>> async def on_message_received(message: IncomingMessage):
            ...    async with message.process(ignore_processed=True):
            ...        # Now (with ignore_processed=True) you may reject
            ...        # (or ack) message manually too
            ...        if True:  # some reasonable condition here
            ...            await message.reject()
            ...        print(message.body)

        :param requeue: Requeue message when exception.
        :param reject_on_redelivered:
            When True message will be rejected only when
            message was redelivered.
        :param ignore_processed: Do nothing if message already processed

        """
        return ProcessContext(
            self, requeue=requeue,
            reject_on_redelivered=reject_on_redelivered,
            ignore_processed=ignore_processed,
        )

    def ack(self, multiple: bool=False) -> asyncio.Task:
        """ Send basic.ack is used for positive acknowledgements

        .. note::
            This method looks like a blocking-method, but actually it's
            just send bytes to the socket and not required any responses
            from the broker.

        :param multiple: If set to True, the message's delivery tag is
                         treated as "up to and including", so that multiple
                         messages can be acknowledged with a single method.
                         If set to False, the ack refers to a single message.
        :return: None
        """
        if self.__no_ack:
            raise TypeError("Can't ack message with \"no_ack\" flag")

        if self.__processed:
            raise MessageProcessError("Message already processed")

        task = asyncio.ensure_future(
            self.__channel.basic_ack(
                delivery_tag=self.delivery_tag, multiple=multiple
            )
        )
        self.__processed = True

        if not self.locked:
            self.lock()

        return task

    def reject(self, requeue=False) -> asyncio.Task:
        """ When `requeue=True` the message will be returned to queue.
        Otherwise message will be dropped.

        .. note::
            This method looks like a blocking-method, but actually it's just
            send bytes to the socket and not required any responses from
            the broker.

        :param requeue: bool
        """
        if self.__no_ack:
            raise TypeError('This message has "no_ack" flag.')

        if self.__processed:
            raise MessageProcessError("Message already processed")

        task = asyncio.ensure_future(self.__channel.basic_reject(
            delivery_tag=self.delivery_tag, requeue=requeue
        ))
        self.__processed = True
        if not self.locked:
            self.lock()

        return task

    def nack(self, multiple: bool=False, requeue: bool=True) -> asyncio.Task:
        if not self.__channel.connection.basic_nack:
            raise RuntimeError("Method not supported on server")

        if self.__no_ack:
            raise TypeError("Can't nack message with \"no_ack\" flag")

        if self.__processed:
            raise MessageProcessError("Message already processed")

        task = asyncio.ensure_future(self.__channel.basic_nack(
            delivery_tag=self.delivery_tag,
            multiple=multiple,
            requeue=requeue
        ))

        self.__processed = True

        if not self.locked:
            self.lock()

        return task

    def info(self) -> dict:
        """ Method returns dict representation of the message """

        info = super(IncomingMessage, self).info()
        info['cluster_id'] = self.cluster_id
        info['consumer_tag'] = self.consumer_tag
        info['delivery_tag'] = self.delivery_tag
        info['exchange'] = self.exchange
        info['redelivered'] = self.redelivered
        info['routing_key'] = self.routing_key
        return info

    @property
    def processed(self):
        return self.__processed


class ReturnedMessage(IncomingMessage):
    pass


class ProcessContext(AsyncContextManager):
    def __init__(self, message: IncomingMessage, *,
                 requeue, reject_on_redelivered, ignore_processed):
        self.message = message
        self.requeue = requeue
        self.reject_on_redelivered = reject_on_redelivered
        self.ignore_processed = ignore_processed

    async def __aenter__(self):
        return self.message

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            if not self.ignore_processed or not self.message.processed:
                await self.message.ack()

            return

        if not self.ignore_processed or not self.message.processed:
            if self.reject_on_redelivered and self.message.redelivered:
                log.info("Message %r was redelivered and will be rejected",
                         self.message)

                await self.message.reject(requeue=False)
            else:
                await self.message.reject(requeue=self.requeue)

    def __enter__(self):
        warn('Use "async with message.process()" instead', DeprecationWarning)
        return self.message

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            if not self.ignore_processed or not self.message.processed:
                self.message.ack()

            return

        if not self.ignore_processed or not self.message.processed:
            if self.reject_on_redelivered and self.message.redelivered:
                log.info("Message %r was redelivered and will be rejected",
                         self.message)

                self.message.reject(requeue=False)
            else:
                self.message.reject(requeue=self.requeue)


__all__ = 'Message', 'IncomingMessage', 'ReturnedMessage', 'DeliveryMode'
