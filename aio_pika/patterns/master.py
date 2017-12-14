import asyncio
import pickle

from functools import partial

from typing import Callable, Any, Generator
from aio_pika.channel import Channel
from aio_pika.queue import Queue
from aio_pika.message import IncomingMessage, Message, DeliveryMode


class Worker:
    __slots__ = 'queue', 'consumer_tag', 'loop'

    def __init__(self, queue: Queue, consumer_tag: str, loop):
        self.queue = queue
        self.consumer_tag = consumer_tag
        self.loop = loop

    def close(self) -> asyncio.Task:
        @asyncio.coroutine
        def closer():
            yield from self.queue.cancel(self.consumer_tag)

        return self.loop.create_task(closer())


class _MethodProxy:
    __slots__ = 'name', 'func',

    def __init__(self, name, func):
        self.name = name
        self.func = func

    def __getattr__(self, item):
        return _MethodProxy(".".join((self.name, item)), func=self.func)

    def __call__(self, **kwargs):
        return self.func(self.name, **kwargs)


class _Proxy:
    __slots__ = 'func',

    def __init__(self, func):
        self.func = func

    def __getattr__(self, item):
        return _MethodProxy(item, self.func)


class Master:
    __slots__ = 'channel', 'loop',

    CONTENT_TYPE = 'application/python-pickle'
    DELIVERY_MODE = DeliveryMode.PERSISTENT

    def __init__(self, channel: Channel):
        self.channel = channel          # type: Channel
        self.loop = self.channel.loop   # type: asyncio.AbstractEventLoop

    @classmethod
    def serialize(cls, data: Any) -> bytes:
        return pickle.dumps(data)

    @classmethod
    def deserialize(cls, data: bytes) -> Any:
        return pickle.loads(data)

    @classmethod
    @asyncio.coroutine
    def execute(cls, func, kwargs):
        kwargs = kwargs or {}
        result = yield from func(**kwargs)
        return result

    @asyncio.coroutine
    def on_message(self, func, message: IncomingMessage):
        with message.process(requeue=True, ignore_processed=True):
            data = self.deserialize(message.body)
            yield from self.execute(func, data)

    @asyncio.coroutine
    def create_worker(self, channel_name: str, func: Callable, **kwargs) -> Generator[Any, None, Worker]:
        queue = yield from self.channel.declare_queue(channel_name, **kwargs)

        consumer_tag = yield from queue.consume(
            partial(
                self.on_message,
                asyncio.coroutine(func)
            )
        )

        return Worker(queue, consumer_tag, self.loop)

    @asyncio.coroutine
    def create_task(self, channel_name: str, **kwargs):
        message = Message(
            body=self.serialize(kwargs),
            content_type=self.CONTENT_TYPE,
            delivery_mode=self.DELIVERY_MODE
        )

        yield from self.channel.default_exchange.publish(
            message, channel_name, mandatory=True
        )

    @property
    def proxy(self):
        return _Proxy(self.create_task)
