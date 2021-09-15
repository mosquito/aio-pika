import asyncio
import json
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from enum import IntEnum, unique
from functools import singledispatch
from logging import getLogger
from pprint import pformat
from typing import (
    Any, AsyncContextManager, Callable, Iterable, Mapping, Optional, Union,
)

import aiormq
from aiormq.abc import DeliveredMessage, FieldTable

from .abc import (
    MILLISECONDS, ZERO_TIME, AbstractIncomingMessage,
    AbstractMessage, HeadersType,
)
from .compat import cached_property
from .exceptions import MessageProcessError


log = getLogger(__name__)
NoneType = type(None)


@unique
class DeliveryMode(IntEnum):
    NOT_PERSISTENT = 1
    PERSISTENT = 2


DateType = Union[int, datetime, float, timedelta, None]


def to_milliseconds(seconds):
    return int(seconds * MILLISECONDS)


@singledispatch
def encode_expiration(value) -> Optional[str]:
    raise ValueError("Invalid timestamp type: %r" % type(value), value)


@encode_expiration.register(datetime)
def _(value):
    now = datetime.now()
    return str(to_milliseconds((value - now).total_seconds()))


@encode_expiration.register(int)
@encode_expiration.register(float)
def _(value):
    return str(to_milliseconds(value))


@encode_expiration.register(timedelta)
def _(value):
    return str(int(value.total_seconds() * MILLISECONDS))


@encode_expiration.register(type(None))
def _(_):
    return None


@singledispatch
def decode_expiration(t) -> Optional[float]:
    raise ValueError("Invalid expiration type: %r" % type(t), t)


@decode_expiration.register(time.struct_time)
def _(t: time.struct_time) -> float:
    return (datetime(*t[:7]) - ZERO_TIME).total_seconds()


@decode_expiration.register(str)
def _(t: str) -> float:
    return float(t)


@singledispatch
def encode_timestamp(value) -> Optional[time.struct_time]:
    raise ValueError("Invalid timestamp type: %r" % type(value), value)


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
    raise ValueError("Invalid timestamp type: %r" % type(value), value)


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


def optional(value, func: Callable[[Any], Any] = str, default=None):
    return func(value) if value else default


@singledispatch
def header_converter(value: Any) -> bytes:
    return json.dumps(
        value, separators=(",", ":"), ensure_ascii=False, default=repr,
    ).encode()


@header_converter.register(bytearray)
@header_converter.register(str)
@header_converter.register(datetime)
@header_converter.register(time.struct_time)
@header_converter.register(NoneType)
@header_converter.register(list)
@header_converter.register(int)
def _(v: bytes):
    return v


@header_converter.register(bytes)
def _(v: bytes) -> bytearray:
    return bytearray(v)


@header_converter.register(set)
@header_converter.register(tuple)
@header_converter.register(frozenset)
def _(v: Iterable):
    return header_converter(list(v))


def format_headers(d: Mapping[str, Any]) -> FieldTable:
    ret = {}

    if not d:
        return ret

    for key, value in d.items():
        ret[key] = header_converter(value)
    return ret


class MessageMethodsMixin:
    @property
    def body_size(self) -> int:
        return len(self.body)

    def info(self) -> dict:
        return {
            "body_size": self.body_size,
            "headers": self.headers,
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

    @cached_property
    def properties(self) -> aiormq.spec.Basic.Properties:
        """ Build :class:`aiormq.spec.Basic.Properties` object """
        return aiormq.spec.Basic.Properties(
            content_type=self.content_type,
            content_encoding=self.content_encoding,
            headers=self.headers,
            delivery_mode=self.delivery_mode,
            priority=self.priority,
            correlation_id=self.correlation_id,
            reply_to=self.reply_to,
            expiration=encode_expiration(self.expiration),
            message_id=self.message_id,
            timestamp=self.timestamp,
            message_type=self.type,
            user_id=self.user_id,
            app_id=self.app_id,
        )

    def __repr__(self):
        return "{name}:{repr}".format(
            name=self.__class__.__name__,
            repr=pformat(self.info()),
        )

    def __iter__(self):
        return iter(self.body)


class Message(MessageMethodsMixin, AbstractMessage):
    def __init__(
        self,
        body: bytes,
        *,
        headers: HeadersType = None,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        delivery_mode: DeliveryMode = None,
        priority: Optional[int] = None,
        correlation_id: Optional[str] = None,
        reply_to: Optional[str] = None,
        expiration: Optional[DateType] = None,
        message_id: Optional[str] = None,
        timestamp: Optional[DateType] = None,
        type: Optional[str] = None,
        user_id: Optional[str] = None,
        app_id: Optional[str] = None
    ):
        super().__init__(
            body=body,
            headers=format_headers(headers),
            content_type=content_type,
            content_encoding=content_encoding,
            delivery_mode=DeliveryMode(
                optional(
                    delivery_mode,
                    func=int,
                    default=DeliveryMode.NOT_PERSISTENT,
                ),
            ).value,
            priority=optional(priority, int, 0),
            correlation_id=optional(correlation_id),
            reply_to=optional(reply_to),
            expiration=expiration,
            message_id=optional(message_id),
            timestamp=encode_timestamp(timestamp),
            type=optional(type),
            user_id=optional(user_id),
            app_id=optional(app_id),
        )

    def clone(self, **kwargs):
        new_kwargs = dict(
            body=self.body,
            headers=self.headers,
            content_type=self.content_type,
            content_encoding=self.content_encoding,
            delivery_mode=self.delivery_mode,
            priority=self.priority,
            correlation_id=self.correlation_id,
            reply_to=self.reply_to,
            expiration=self.expiration,
            message_id=self.message_id,
            timestamp=self.timestamp,
            type=self.type,
            user_id=self.user_id,
            app_id=self.app_id,
        )

        if 'headers' in kwargs:
            kwargs['headers'] = format_headers(kwargs['headers'])

        if 'timestamp' in kwargs:
            kwargs['timestamp'] = encode_timestamp(kwargs['timestamp'])

        new_kwargs.update(**kwargs)
        return self.__class__(**new_kwargs)

    def update_headers(self, headers: HeadersType) -> AbstractMessage:
        formatted_headers = dict(self.headers)
        formatted_headers.update(headers)
        return self.clone(headers=formatted_headers)


class IncomingMessage(MessageMethodsMixin, AbstractIncomingMessage):
    """ Incoming message is seems like Message but has additional methods for
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

    def __init__(self, message: DeliveredMessage, no_ack: bool = False):
        """ Create an instance of :class:`IncomingMessage` """

        expiration = None
        if message.header.properties.expiration:
            expiration = decode_expiration(
                message.header.properties.expiration,
            )

        consumer_tag = None
        delivery_tag = None
        redelivered = None
        message_count = None

        if isinstance(message.delivery, aiormq.spec.Basic.GetOk):
            message_count = message.delivery.message_count
            delivery_tag = message.delivery.delivery_tag
            redelivered = message.delivery.redelivered
        elif isinstance(message.delivery, aiormq.spec.Basic.Deliver):
            consumer_tag = message.delivery.consumer_tag
            delivery_tag = message.delivery.delivery_tag
            redelivered = message.delivery.redelivered

        routing_key = message.delivery.routing_key
        exchange = message.delivery.exchange
        processed_future = asyncio.Future()

        super().__init__(
            processed_future=processed_future,
            channel=message.channel,
            no_ack=no_ack,
            app_id=message.header.properties.app_id,
            body=message.body,
            cluster_id=message.header.properties.cluster_id,
            consumer_tag=consumer_tag,
            content_encoding=message.header.properties.content_encoding,
            content_type=message.header.properties.content_type,
            correlation_id=message.header.properties.correlation_id,
            delivery_mode=message.header.properties.delivery_mode,
            delivery_tag=delivery_tag,
            exchange=exchange,
            expiration=expiration / 1000.0 if expiration else None,
            headers=message.header.properties.headers,
            message_count=message_count,
            message_id=message.header.properties.message_id,
            priority=message.header.properties.priority,
            redelivered=redelivered,
            reply_to=message.header.properties.reply_to,
            routing_key=routing_key,
            timestamp=decode_timestamp(message.header.properties.timestamp),
            type=message.header.properties.message_type,
            user_id=message.header.properties.user_id,
        )

        if no_ack or not self.delivery_tag:
            processed_future.set_result(True)

    @property
    def processed(self):
        if not self.channel:
            return True
        return self.processed_future.done()

    @asynccontextmanager
    async def process(
        self,
        requeue=False,
        reject_on_redelivered=False,
        ignore_processed=False,
    ) -> AsyncContextManager:
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

        try:
            yield self
            if not ignore_processed or not self.processed:
                await self.ack()
        except BaseException:
            if not ignore_processed or not self.processed:
                if reject_on_redelivered and self.redelivered:
                    if not self.channel.is_closed:
                        log.info(
                            "Message %r was redelivered and will be rejected",
                            self,
                        )
                        await self.reject(requeue=False)
                    log.warning(
                        "Message %r was redelivered and reject is not sent "
                        "since channel is closed",
                        self,
                    )
                else:
                    if not self.channel.is_closed:
                        await self.reject(requeue=requeue)
                    log.warning("Reject is not sent since channel is closed")
            raise

    def ack(self, multiple: bool = False) -> asyncio.Future:
        """ Send basic.ack is used for positive acknowledgements

        .. note::
            This method looks like a blocking-method, but actually it just
            sends bytes to the socket and doesn't require any responses from
            the broker.

        :param multiple: If set to True, the message's delivery tag is
                         treated as "up to and including", so that multiple
                         messages can be acknowledged with a single method.
                         If set to False, the ack refers to a single message.
        :return: None
        """
        if self.no_ack:
            raise TypeError('Can\'t ack message with "no_ack" flag')

        if self.processed:
            raise MessageProcessError("Message already processed")

        self.processed_future.set_result(True)
        return asyncio.ensure_future(
            self.channel.basic_ack(
                delivery_tag=self.delivery_tag, multiple=multiple,
            ),
        )

    def reject(self, requeue: bool = False) -> asyncio.Future:
        """ When `requeue=True` the message will be returned to queue.
        Otherwise message will be dropped.

        .. note::
            This method looks like a blocking-method, but actually it just
            sends bytes to the socket and doesn't require any responses from
            the broker.

        :param requeue: bool
        """
        if self.no_ack:
            raise TypeError('This message has "no_ack" flag.')

        if self.processed:
            raise MessageProcessError("Message already processed")

        self.processed_future.set_result(True)

        return asyncio.ensure_future(
            self.channel.basic_reject(
                delivery_tag=self.delivery_tag, requeue=requeue,
            ),
        )

    def nack(
        self, multiple: bool = False, requeue: bool = True,
    ) -> asyncio.Future:

        if not self.channel.connection.basic_nack:
            raise RuntimeError("Method not supported on server")

        if self.no_ack:
            raise TypeError('Can\'t nack message with "no_ack" flag')

        if self.processed:
            raise MessageProcessError("Message already processed")

        self.processed_future.set_result(True)
        return asyncio.ensure_future(
            self.channel.basic_nack(
                delivery_tag=self.delivery_tag,
                multiple=multiple,
                requeue=requeue,
            ),
        )

    def info(self) -> dict:
        """ Method returns dict representation of the message """
        info = super().info()
        info["cluster_id"] = self.cluster_id
        info["consumer_tag"] = self.consumer_tag
        info["delivery_tag"] = self.delivery_tag
        info["exchange"] = self.exchange
        info["redelivered"] = self.redelivered
        info["routing_key"] = self.routing_key
        return info


class ReturnedMessage(IncomingMessage):
    pass


__all__ = "Message", "IncomingMessage", "ReturnedMessage", "DeliveryMode"
