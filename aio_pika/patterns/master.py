import asyncio
import logging

from functools import partial

from typing import Callable, Any, Generator
from aio_pika.channel import Channel
from aio_pika.queue import Queue
from aio_pika.message import IncomingMessage, Message, DeliveryMode, ReturnedMessage

from .base import Proxy, Base


log = logging.getLogger(__name__)


class Worker:
    __slots__ = 'queue', 'consumer_tag', 'loop',

    def __init__(self, queue: Queue, consumer_tag: str, loop):
        self.queue = queue
        self.consumer_tag = consumer_tag
        self.loop = loop

    def close(self) -> asyncio.Task:
        @asyncio.coroutine
        def closer():
            yield from self.queue.cancel(self.consumer_tag)

        return self.loop.create_task(closer())


class Master(Base):
    __slots__ = 'channel', 'loop', 'proxy',

    CONTENT_TYPE = 'application/python-pickle'
    DELIVERY_MODE = DeliveryMode.PERSISTENT

    def __init__(self, channel: Channel):
        self.channel = channel          # type: Channel
        self.loop = self.channel.loop   # type: asyncio.AbstractEventLoop
        self.proxy = Proxy(self.create_task)
        self.channel.add_on_return_callback(self.on_message_returned)

    @staticmethod
    def on_message_returned(message: ReturnedMessage):
        log.warning(
            "Message returned. Probably destination queue does not exists: %r",
            message
        )

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
    def create_task(self, channel_name: str, kwargs=None):
        message = Message(
            body=self.serialize(kwargs or {}),
            content_type=self.CONTENT_TYPE,
            delivery_mode=self.DELIVERY_MODE
        )

        yield from self.channel.default_exchange.publish(
            message, channel_name, mandatory=True
        )
