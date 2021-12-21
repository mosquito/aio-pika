import asyncio
import json
import logging
import pickle
import time
from enum import Enum
from functools import partial
from typing import Any, Callable, Dict, Hashable, Optional, TypeVar

from aiormq.tools import awaitable

from aio_pika.channel import Channel
from aio_pika.exceptions import DeliveryError
from aio_pika.exchange import ExchangeType
from aio_pika.message import (
    DeliveryMode, IncomingMessage, Message, ReturnedMessage,
)
from aio_pika.tools import shield

from .base import Base, Proxy


log = logging.getLogger(__name__)

R = TypeVar("R")
P = TypeVar("P")
CallbackType = Callable[[P], R]


class RPCMessageTypes(Enum):
    error = "error"
    result = "result"
    call = "call"


class RPC(Base):
    __slots__ = (
        "channel",
        "loop",
        "proxy",
        "result_queue",
        "result_consumer_tag",
        "routes",
        "consumer_tags",
        "dlx_exchange",
    )

    DLX_NAME = "rpc.dlx"
    DELIVERY_MODE = DeliveryMode.NOT_PERSISTENT

    __doc__ = """
    Remote Procedure Call helper.

    Create an instance ::

        rpc = await RPC.create(channel)

    Registering python function ::

        # RPC instance passes only keyword arguments
        def multiply(*, x, y):
            return x * y

        await rpc.register("multiply", multiply)

    Call function through proxy ::

        assert await rpc.proxy.multiply(x=2, y=3) == 6

    Call function explicit ::

        assert await rpc.call('multiply', dict(x=2, y=3)) == 6

    """

    def __init__(self, channel: Channel):
        self.channel = channel
        self.loop = self.channel.loop
        self.proxy = Proxy(self.call)
        self.result_queue = None
        self.futures = {}  # type Dict[int, asyncio.Future]
        self.result_consumer_tag = None
        self.routes = {}
        self.queues = {}
        self.consumer_tags = {}
        self.dlx_exchange = None

    def __remove_future(self, future: asyncio.Future):
        log.debug("Remove done future %r", future)
        self.futures.pop(id(future), None)

    def create_future(self) -> asyncio.Future:
        future = self.loop.create_future()
        log.debug("Create future for RPC call")

        self.futures[id(future)] = future
        future.add_done_callback(self.__remove_future)
        return future

    @shield
    async def close(self):
        if self.result_queue is None:
            log.warning("RPC already closed")
            return

        log.debug("Cancelling listening %r", self.result_queue)
        await self.result_queue.cancel(self.result_consumer_tag)
        self.result_consumer_tag = None

        log.debug("Unbinding %r", self.result_queue)
        await self.result_queue.unbind(
            self.dlx_exchange,
            "",
            arguments={"From": self.result_queue.name, "x-match": "any"},
        )

        log.debug("Cancelling undone futures %r", self.futures)
        for future in self.futures.values():
            if future.done():
                continue

            future.set_exception(asyncio.CancelledError)

        log.debug("Deleting %r", self.result_queue)
        await self.result_queue.delete()
        self.result_queue = None

    @shield
    async def initialize(self, auto_delete=True, durable=False, **kwargs):
        if self.result_queue is not None:
            return

        self.result_queue = await self.channel.declare_queue(
            None, auto_delete=auto_delete, durable=durable, **kwargs
        )

        self.dlx_exchange = await self.channel.declare_exchange(
            self.DLX_NAME, type=ExchangeType.HEADERS, auto_delete=True,
        )

        await self.result_queue.bind(
            self.dlx_exchange,
            "",
            arguments={"From": self.result_queue.name, "x-match": "any"},
        )

        self.result_consumer_tag = await self.result_queue.consume(
            self.on_result_message, exclusive=True, no_ack=True,
        )

        self.channel.add_close_callback(self.on_close)
        self.channel.add_on_return_callback(
            self.on_message_returned, weak=False,
        )

    def on_close(self, _: Channel, exc: Optional[BaseException] = None):
        log.debug("Closing RPC futures because %r", exc)
        for future in self.futures.values():
            if future.done():
                continue

            future.set_exception(exc or Exception)

    @classmethod
    async def create(cls, channel: Channel, **kwargs) -> "RPC":
        """ Creates a new instance of :class:`aio_pika.patterns.RPC`.
        You should use this method instead of :func:`__init__`,
        because :func:`create` returns coroutine and makes async initialize

        :param channel: initialized instance of :class:`aio_pika.Channel`
        :returns: :class:`RPC`

        """
        rpc = cls(channel)
        await rpc.initialize(**kwargs)
        return rpc

    def on_message_returned(self, sender: "RPC", message: ReturnedMessage):
        correlation_id = (
            int(message.correlation_id) if message.correlation_id else None
        )

        future = self.futures.pop(correlation_id, None)

        if not future or future.done():
            log.warning("Unknown message was returned: %r", message)
            return

        future.set_exception(DeliveryError(message, None))

    async def on_result_message(self, message: IncomingMessage):
        correlation_id = (
            int(message.correlation_id) if message.correlation_id else None
        )

        future = self.futures.pop(correlation_id, None)

        if future is None:
            log.warning("Unknown message: %r", message)
            return

        try:
            payload = self.deserialize(message.body)
        except Exception as e:
            log.error("Failed to deserialize response on message: %r", message)
            future.set_exception(e)
            return

        if message.type == RPCMessageTypes.result.value:
            future.set_result(payload)
        elif message.type == RPCMessageTypes.error.value:
            future.set_exception(payload)
        elif message.type == RPCMessageTypes.call.value:
            future.set_exception(
                asyncio.TimeoutError("Message timed-out", message),
            )
        else:
            future.set_exception(
                RuntimeError("Unknown message type %r" % message.type),
            )

    async def on_call_message(
        self, method_name: str, message: IncomingMessage
    ):
        if method_name not in self.routes:
            log.warning("Method %r not registered in %r", method_name, self)
            return

        try:
            payload = self.deserialize(message.body)
            func = self.routes[method_name]

            result = await self.execute(func, payload)
            result = self.serialize(result)
            message_type = RPCMessageTypes.result.value
        except Exception as e:
            result = self.serialize_exception(e)
            message_type = RPCMessageTypes.error.value

        if not message.reply_to:
            log.info(
                'RPC message without "reply_to" header %r call result '
                "will be lost",
                message,
            )
            await message.ack()
            return

        result_message = Message(
            result,
            content_type=self.CONTENT_TYPE,
            correlation_id=message.correlation_id,
            delivery_mode=message.delivery_mode,
            timestamp=time.time(),
            type=message_type,
        )

        try:
            await self.channel.default_exchange.publish(
                result_message, message.reply_to, mandatory=False,
            )
        except Exception:
            log.exception("Failed to send reply %r", result_message)
            await message.reject(requeue=False)
            return

        if message_type == RPCMessageTypes.error.value:
            await message.ack()
            return

        await message.ack()

    def serialize(self, data: Any) -> bytes:
        """ Serialize data to the bytes.
        Uses `pickle` by default.
        You should overlap this method when you want to change serializer

        :param data: Data which will be serialized
        :returns: bytes
        """
        return super().serialize(data)

    def deserialize(self, data: Any) -> bytes:
        """ Deserialize data from bytes.
        Uses `pickle` by default.
        You should overlap this method when you want to change serializer

        :param data: Data which will be deserialized
        :returns: :class:`Any`
        """
        return super().deserialize(data)

    def serialize_exception(self, exception: Exception) -> bytes:
        """ Serialize python exception to bytes

        :param exception: :class:`Exception`
        :return: bytes
        """
        return pickle.dumps(exception)

    async def execute(self, func: CallbackType, payload: P) -> R:
        """ Executes rpc call. Might be overlapped. """
        return await func(**payload)

    async def call(
        self,
        method_name,
        kwargs: Optional[Dict[Hashable, Any]] = None,
        *,
        expiration: Optional[int] = None,
        priority: int = 5,
        delivery_mode: DeliveryMode = DELIVERY_MODE
    ):
        """ Call remote method and awaiting result.

        :param method_name: Name of method
        :param kwargs: Methos kwargs
        :param expiration:
            If not `None` messages which staying in queue longer
            will be returned and :class:`asyncio.TimeoutError` will be raised.
        :param priority: Message priority
        :param delivery_mode: Call message delivery mode
        :raises asyncio.TimeoutError: when message expired
        :raises CancelledError: when called :func:`RPC.cancel`
        :raises RuntimeError: internal error
        """

        future = self.create_future()

        message = Message(
            body=self.serialize(kwargs or {}),
            type=RPCMessageTypes.call.value,
            timestamp=time.time(),
            priority=priority,
            correlation_id=id(future),
            delivery_mode=delivery_mode,
            reply_to=self.result_queue.name,
            headers={"From": self.result_queue.name},
        )

        if expiration is not None:
            message.expiration = expiration

        log.debug("Publishing calls for %s(%r)", method_name, kwargs)
        await self.channel.default_exchange.publish(
            message, routing_key=method_name, mandatory=True,
        )

        log.debug("Waiting RPC result for %s(%r)", method_name, kwargs)
        return await future

    async def register(self, method_name, func: CallbackType, **kwargs):
        """ Method creates a queue with name which equal of
        `method_name` argument. Then subscribes this queue.

        :param method_name: Method name
        :param func:
            target function. Function **MUST** accept only keyword arguments.
        :param kwargs: arguments which will be passed to `queue_declare`
        :raises RuntimeError:
            Function already registered in this :class:`RPC` instance
            or method_name already used.
        """
        arguments = kwargs.pop("arguments", {})
        arguments.update({"x-dead-letter-exchange": self.DLX_NAME})

        kwargs["arguments"] = arguments

        queue = await self.channel.declare_queue(method_name, **kwargs)

        if func in self.consumer_tags:
            raise RuntimeError("Function already registered")

        if method_name in self.routes:
            raise RuntimeError(
                "Method name already used for %r" % self.routes[method_name],
            )

        self.consumer_tags[func] = await queue.consume(
            partial(self.on_call_message, method_name),
        )

        self.routes[method_name] = awaitable(func)
        self.queues[func] = queue

    async def unregister(self, func):
        """ Cancels subscription to the method-queue.

        :param func: Function
        """
        if func not in self.consumer_tags:
            return

        consumer_tag = self.consumer_tags.pop(func)
        queue = self.queues.pop(func)

        await queue.cancel(consumer_tag)

        self.routes.pop(queue.name)


class JsonRPC(RPC):
    SERIALIZER = json
    CONTENT_TYPE = "application/json"

    def serialize(self, data: Any) -> bytes:
        return self.SERIALIZER.dumps(data, ensure_ascii=False, default=repr)

    def serialize_exception(self, exception: Exception) -> bytes:
        return self.serialize(
            {
                "error": {
                    "type": exception.__class__.__name__,
                    "message": repr(exception),
                    "args": exception.args,
                },
            },
        )
