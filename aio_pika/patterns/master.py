import asyncio
import logging

from functools import partial

from typing import Callable, Any
from aio_pika.channel import Channel
from aio_pika.queue import Queue
from aio_pika.message import (
    IncomingMessage, Message, DeliveryMode, ReturnedMessage
)

from .base import Proxy, Base


log = logging.getLogger(__name__)


class MessageProcessingError(Exception):
    pass


class NackMessage(MessageProcessingError):
    def __init__(self, requeue=False):
        self.requeue = requeue


class RejectMessage(MessageProcessingError):
    def __init__(self, requeue=False):
        self.requeue = requeue


class Worker:
    __slots__ = 'queue', 'consumer_tag', 'loop',

    def __init__(self, queue: Queue, consumer_tag: str, loop):
        self.queue = queue
        self.consumer_tag = consumer_tag
        self.loop = loop

    def close(self) -> asyncio.Task:
        """ Cancel subscription to the channel

        :return: :class:`asyncio.Task`
        """
        async def closer():
            await self.queue.cancel(self.consumer_tag)

        return self.loop.create_task(closer())


class Master(Base):
    __slots__ = 'channel', 'loop', 'proxy',

    DELIVERY_MODE = DeliveryMode.PERSISTENT

    __doc__ = """
    Implements Master/Worker pattern.
    Usage example:

    `worker.py` ::

        master = Master(channel)
        worker = await master.create_worker('test_worker', lambda x: print(x))

    `master.py` ::

        master = Master(channel)
        await master.proxy.test_worker('foo')
    """

    def __init__(self, channel: Channel):
        """ Creates a new :class:`Master` instance.

        :param channel: Initialized instance of :class:`aio_pika.Channel`
        """
        self.channel = channel          # type: Channel
        self.loop = self.channel.loop   # type: asyncio.AbstractEventLoop
        self.proxy = Proxy(self.create_task)

        self.channel.add_on_return_callback(self.on_message_returned)

    @property
    def exchange(self):
        return self.channel.default_exchange

    def on_message_returned(self, message: ReturnedMessage):
        log.warning(
            "Message returned. Probably destination queue does not exists: %r",
            message
        )

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

    @classmethod
    async def execute(cls, func, kwargs):
        kwargs = kwargs or {}
        result = await func(**kwargs)
        return result

    async def on_message(self, func, message: IncomingMessage):
        with message.process(requeue=True, ignore_processed=True):
            data = self.deserialize(message.body)

            try:
                await self.execute(func, data)
            except RejectMessage as e:
                message.reject(requeue=e.requeue)
            except NackMessage as e:
                message.nack(requeue=e.requeue)

    async def create_queue(self, channel_name, **kwargs) -> Queue:
        return await self.channel.declare_queue(channel_name, **kwargs)

    async def create_worker(self, channel_name: str,
                            func: Callable, **kwargs) -> Worker:
        """ Creates a new :class:`Worker` instance. """

        queue = await self.create_queue(channel_name, **kwargs)

        if hasattr(func, "_is_coroutine"):
            fn = func
        else:
            fn = asyncio.coroutine(func)
        consumer_tag = await queue.consume(
            partial(
                self.on_message,
                fn
            )
        )

        return Worker(queue, consumer_tag, self.loop)

    async def create_task(self, channel_name: str,
                          kwargs=None, **message_kwargs):

        """ Creates a new task for the worker """
        message = Message(
            body=self.serialize(kwargs or {}),
            content_type=self.CONTENT_TYPE,
            delivery_mode=self.DELIVERY_MODE,
            **message_kwargs
        )

        await self.exchange.publish(
            message, channel_name, mandatory=True
        )
