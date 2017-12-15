import asyncio
import logging
import pickle

import time
from functools import partial
from typing import Callable

from aio_pika import ExchangeType
from aio_pika.channel import Channel
from aio_pika.exceptions import UnroutableError
from aio_pika.message import Message, IncomingMessage, DeliveryMode, ReturnedMessage
from aio_pika.queue import Queue
from aio_pika.tools import create_future
from .base import Proxy, Base

log = logging.getLogger(__name__)


class RPC(Base):
    __slots__ = ("channel", "loop", "proxy", "result_queue",
                 "result_consumer_tag", "routes", "consumer_tags",
                 "dlx_exchange",)

    DLX_NAME = 'rpc.dlx'

    def __init__(self, channel: Channel):
        self.channel = channel
        self.loop = self.channel.loop
        self.proxy = Proxy(self.call)
        self.result_queue = None  # type: Queue
        self.futures = dict()
        self.result_consumer_tag = None
        self.routes = {}
        self.queues = {}
        self.consumer_tags = {}
        self.dlx_exchange = None

    def create_future(self) -> asyncio.Future:
        future = create_future(loop=self.loop)
        future_id = id(future)
        self.futures[future_id] = future
        future.add_done_callback(lambda f: self.futures.pop(future_id, None))
        return future

    def close(self) -> asyncio.Task:
        @asyncio.coroutine
        def closer():
            nonlocal self

            if self.result_queue is None:
                return

            for future in self.futures.values():
                future.set_exception(asyncio.CancelledError)

            yield from self.result_queue.unbind(
                self.dlx_exchange, "",
                arguments={
                    "From": self.result_queue.name,
                    'x-match': 'any',
                }
            )

            yield from self.result_queue.cancel(self.result_consumer_tag)
            self.result_consumer_tag = None

            yield from self.result_queue.delete()
            self.result_queue = None

        return self.loop.create_task(closer())

    @asyncio.coroutine
    def initialize(self, **kwargs):
        if self.result_queue is not None:
            return

        self.result_queue = yield from self.channel.declare_queue(None, **kwargs)

        self.dlx_exchange = yield from self.channel.declare_exchange(
            self.DLX_NAME,
            type=ExchangeType.HEADERS,
            auto_delete=True,
        )

        yield from self.result_queue.bind(
            self.dlx_exchange, "",
            arguments={
                "From": self.result_queue.name,
                'x-match': 'any',
            }
        )

        self.result_consumer_tag = yield from self.result_queue.consume(
            self.on_result_message, exclusive=True, no_ack=True
        )

        self.channel.add_on_return_callback(self.on_message_returned)

    @classmethod
    @asyncio.coroutine
    def create(cls, channel: Channel, **kwargs):
        rpc = cls(channel)
        yield from rpc.initialize(**kwargs)
        return rpc

    @asyncio.coroutine
    def on_message_returned(self, message: ReturnedMessage):
        correlation_id = int(message.correlation_id) if message.correlation_id else None
        future = self.futures.pop(correlation_id, None)   # type: asyncio.Future

        if not future or future.done():
            log.warning("Unknown message was returned: %r", message)
            return

        future.set_exception(UnroutableError([message]))

    @asyncio.coroutine
    def on_result_message(self, message: IncomingMessage):
        correlation_id = int(message.correlation_id) if message.correlation_id else None
        future = self.futures.pop(correlation_id, None)  # type: asyncio.Future

        if future is None:
            log.warning("Unknown message: %r", message)
            return

        payload = self.deserialize(message.body)

        if message.type == 'result':
            future.set_result(payload)
        elif message.type == 'error':
            future.set_exception(payload)
        elif message.type == 'call':
            future.set_exception(
                TimeoutError("Message timed-out", message)
            )
        else:
            future.set_exception(
                RuntimeError("Unknown message type %r" % message.type)
            )

    @asyncio.coroutine
    def on_call_message(self, method_name: str, message: IncomingMessage):
        if method_name not in self.routes:
            log.warning("Method %r not registered in %r", method_name, self)
            return

        payload = self.deserialize(message.body)
        func = self.routes[method_name]

        try:
            result = yield from self.execute(func, payload)
            result = self.serialize(result)
            message_type = 'result'
        except Exception as e:
            result = self.serialize_exception(e)
            message_type = 'error'

        result_message = Message(
            result,
            delivery_mode=message.delivery_mode,
            correlation_id=message.correlation_id,
            timestamp=time.time(),
            type=message_type,
        )

        yield from self.channel.default_exchange.publish(
            result_message,
            message.reply_to,
            mandatory=False
        )

        message.ack()

    @staticmethod
    def serialize_exception(exception: Exception):
        return pickle.dumps(exception)

    @staticmethod
    @asyncio.coroutine
    def execute(func, payload):
        return (yield from func(**payload))

    @asyncio.coroutine
    def call(self, method_name, kwargs=None, *, expiration: int = None, priority: int = 128,
             delivery_mode: DeliveryMode = DeliveryMode.NOT_PERSISTENT):

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

        yield from self.channel.default_exchange.publish(
            message, routing_key=method_name, mandatory=True
        )

        return (yield from future)

    @asyncio.coroutine
    def register(self, method_name, func: Callable, **kwargs):
        arguments = kwargs.pop('arguments', {}).update({
            'x-dead-letter-exchange': self.DLX_NAME,
        })

        kwargs['arguments'] = arguments

        queue = yield from self.channel.declare_queue(method_name, **kwargs)

        if func in self.consumer_tags:
            raise RuntimeError('Function already registered')

        if method_name in self.routes:
            raise RuntimeError(
                'Method name already used for %r' % self.routes[method_name]
            )

        self.consumer_tags[func] = yield from queue.consume(
            partial(self.on_call_message, method_name)
        )

        self.routes[method_name] = asyncio.coroutine(func)
        self.queues[func] = queue

    @asyncio.coroutine
    def unregister(self, func):
        if func not in self.consumer_tags:
            return

        consumer_tag = self.consumer_tags.pop(func)
        queue = self.queues.pop(func)

        yield from queue.cancel(consumer_tag)

        self.routes.pop(queue.name)
