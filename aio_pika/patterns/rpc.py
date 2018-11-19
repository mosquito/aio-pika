import asyncio
import logging
import pickle

import time
from functools import partial
from typing import Callable, Any, TypeVar

from aio_pika.exchange import ExchangeType
from aio_pika.channel import Channel
from aio_pika.exceptions import UnroutableError
from aio_pika.message import (
    Message, IncomingMessage, DeliveryMode, ReturnedMessage
)
from .base import Proxy, Base

log = logging.getLogger(__name__)

R = TypeVar('R')
P = TypeVar('P')
CallbackType = Callable[[P], R]


class RPC(Base):
    __slots__ = ("channel", "loop", "proxy", "result_queue",
                 "result_consumer_tag", "routes", "consumer_tags",
                 "dlx_exchange",)

    DLX_NAME = 'rpc.dlx'
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
        self.futures = dict()
        self.result_consumer_tag = None
        self.routes = {}
        self.queues = {}
        self.consumer_tags = {}
        self.dlx_exchange = None

    def create_future(self) -> asyncio.Future:
        future = self.loop.create_future()
        future_id = id(future)
        self.futures[future_id] = future
        future.add_done_callback(lambda f: self.futures.pop(future_id, None))
        return future

    def close(self) -> asyncio.Task:
        async def closer():
            nonlocal self

            if self.result_queue is None:
                return

            for future in self.futures.values():
                future.set_exception(asyncio.CancelledError)

            await self.result_queue.unbind(
                self.dlx_exchange, "",
                arguments={
                    "From": self.result_queue.name,
                    'x-match': 'any',
                }
            )

            await self.result_queue.cancel(self.result_consumer_tag)
            self.result_consumer_tag = None

            await self.result_queue.delete()
            self.result_queue = None

        return self.loop.create_task(closer())

    async def initialize(self, auto_delete=True, durable=False, **kwargs):
        if self.result_queue is not None:
            return

        self.result_queue = await self.channel.declare_queue(
            None, auto_delete=auto_delete, durable=durable, **kwargs
        )

        self.dlx_exchange = await self.channel.declare_exchange(
            self.DLX_NAME,
            type=ExchangeType.HEADERS,
            auto_delete=True,
        )

        await self.result_queue.bind(
            self.dlx_exchange, "",
            arguments={
                "From": self.result_queue.name,
                'x-match': 'any',
            }
        )

        self.result_consumer_tag = await self.result_queue.consume(
            self.on_result_message, exclusive=True, no_ack=True
        )

        self.channel.add_on_return_callback(self.on_message_returned)

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

    def on_message_returned(self, message: ReturnedMessage):
        correlation_id = int(
            message.correlation_id
        ) if message.correlation_id else None

        future = self.futures.pop(correlation_id, None)   # type: asyncio.Future

        if not future or future.done():
            log.warning("Unknown message was returned: %r", message)
            return

        future.set_exception(UnroutableError([message]))

    async def on_result_message(self, message: IncomingMessage):
        correlation_id = int(
            message.correlation_id
        ) if message.correlation_id else None

        future = self.futures.pop(correlation_id, None)  # type: asyncio.Future

        if future is None:
            log.warning("Unknown message: %r", message)
            return

        try:
            payload = self.deserialize(message.body)
        except Exception as e:
            log.error("Failed to deserialize response on message: %r", message)
            future.set_exception(e)
            return

        if message.type == 'result':
            future.set_result(payload)
        elif message.type == 'error':
            future.set_exception(payload)
        elif message.type == 'call':
            future.set_exception(
                asyncio.TimeoutError("Message timed-out", message)
            )
        else:
            future.set_exception(
                RuntimeError("Unknown message type %r" % message.type)
            )

    async def on_call_message(self, method_name: str, message: IncomingMessage):
        if method_name not in self.routes:
            log.warning("Method %r not registered in %r", method_name, self)
            return

        try:
            payload = self.deserialize(message.body)
            func = self.routes[method_name]

            result = await self.execute(func, payload)
            result = self.serialize(result)
            message_type = 'result'
        except Exception as e:
            result = self.serialize_exception(e)
            message_type = 'error'

        result_message = Message(
            result,
            content_type=self.CONTENT_TYPE,
            correlation_id=message.correlation_id,
            delivery_mode=message.delivery_mode,
            timestamp=time.time(),
            type=message_type,
        )

        await self.channel.default_exchange.publish(
            result_message,
            message.reply_to,
            mandatory=False
        )

        message.ack()

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

    async def call(self, method_name, kwargs: dict=None, *,
                   expiration: int=None, priority: int=128,
                   delivery_mode: DeliveryMode=DELIVERY_MODE):

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
            type='call',
            timestamp=time.time(),
            priority=priority,
            correlation_id=id(future),
            delivery_mode=delivery_mode,
            reply_to=self.result_queue.name,
            headers={
                'From': self.result_queue.name
            }
        )

        if expiration is not None:
            message.expiration = expiration

        await self.channel.default_exchange.publish(
            message, routing_key=method_name, mandatory=True
        )

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
        arguments = kwargs.pop('arguments', {})
        arguments.update({
            'x-dead-letter-exchange': self.DLX_NAME,
        })

        kwargs['arguments'] = arguments

        queue = await self.channel.declare_queue(method_name, **kwargs)

        if func in self.consumer_tags:
            raise RuntimeError('Function already registered')

        if method_name in self.routes:
            raise RuntimeError(
                'Method name already used for %r' % self.routes[method_name]
            )

        self.consumer_tags[func] = await queue.consume(
            partial(self.on_call_message, method_name)
        )

        self.routes[method_name] = asyncio.coroutine(func)
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
