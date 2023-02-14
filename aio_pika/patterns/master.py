import asyncio
import gzip
import json
import logging
from functools import partial
from types import MappingProxyType
from typing import Any, Awaitable, Callable, Mapping, Optional, TypeVar

import aiormq
from aiormq.tools import awaitable

from aio_pika.abc import (
    AbstractChannel, AbstractExchange, AbstractIncomingMessage, AbstractQueue,
    ConsumerTag, DeliveryMode,
)
from aio_pika.message import Message, ReturnedMessage

from ..tools import create_task
from .base import Base, Proxy


log = logging.getLogger(__name__)
T = TypeVar("T")


class MessageProcessingError(Exception):
    pass


class NackMessage(MessageProcessingError):
    def __init__(self, requeue: bool = False):
        self.requeue = requeue


class RejectMessage(MessageProcessingError):
    def __init__(self, requeue: bool = False):
        self.requeue = requeue


class Worker:
    __slots__ = (
        "queue",
        "consumer_tag",
        "loop",
    )

    def __init__(
        self, queue: AbstractQueue, consumer_tag: ConsumerTag,
        loop: asyncio.AbstractEventLoop,
    ):
        self.queue = queue
        self.consumer_tag = consumer_tag
        self.loop = loop

    def close(self) -> Awaitable[None]:
        """ Cancel subscription to the channel

        :return: :class:`asyncio.Task`
        """

        async def closer() -> None:
            await self.queue.cancel(self.consumer_tag)

        return create_task(closer)


class Master(Base):
    __slots__ = (
        "channel",
        "loop",
        "proxy",
    )

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

    def __init__(
        self,
        channel: AbstractChannel,
        requeue: bool = True,
        reject_on_redelivered: bool = False,
    ):
        """ Creates a new :class:`Master` instance.

        :param channel: Initialized instance of :class:`aio_pika.Channel`
        """
        self.channel: AbstractChannel = channel
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.proxy = Proxy(self.create_task)

        self.channel.return_callbacks.add(self.on_message_returned)

        self._requeue = requeue
        self._reject_on_redelivered = reject_on_redelivered

    @property
    def exchange(self) -> AbstractExchange:
        return self.channel.default_exchange

    @staticmethod
    def on_message_returned(
        channel: AbstractChannel,
        message: ReturnedMessage,
    ) -> None:
        log.warning(
            "Message returned. Probably destination queue does not exists: %r",
            message,
        )

    def serialize(self, data: Any) -> bytes:
        """ Serialize data to the bytes.
        Uses `pickle` by default.
        You should overlap this method when you want to change serializer

        :param data: Data which will be serialized
        :returns: bytes
        """
        return super().serialize(data)

    def deserialize(self, data: bytes) -> Any:
        """ Deserialize data from bytes.
        Uses `pickle` by default.
        You should overlap this method when you want to change serializer

        :param data: Data which will be deserialized
        :returns: :class:`Any`
        """
        return super().deserialize(data)

    @classmethod
    async def execute(
        cls, func: Callable[..., Awaitable[T]], kwargs: Any,
    ) -> T:
        kwargs = kwargs or {}

        if not isinstance(kwargs, dict):
            raise RejectMessage(requeue=False)

        return await func(**kwargs)

    async def on_message(
        self, func: Callable[..., Any],
        message: AbstractIncomingMessage,
    ) -> None:
        async with message.process(
            requeue=self._requeue,
            reject_on_redelivered=self._reject_on_redelivered,
            ignore_processed=True,
        ):
            try:
                await self.execute(func, self.deserialize(message.body))
            except RejectMessage as e:
                await message.reject(requeue=e.requeue)
            except NackMessage as e:
                await message.nack(requeue=e.requeue)

    async def create_queue(
        self, channel_name: str, **kwargs: Any,
    ) -> AbstractQueue:
        return await self.channel.declare_queue(channel_name, **kwargs)

    async def create_worker(
        self, channel_name: str, func: Callable[..., Any], **kwargs: Any,
    ) -> Worker:
        """ Creates a new :class:`Worker` instance. """

        queue = await self.create_queue(channel_name, **kwargs)

        if hasattr(func, "_is_coroutine"):
            fn = func
        else:
            fn = awaitable(func)
        consumer_tag = await queue.consume(partial(self.on_message, fn))

        return Worker(queue, consumer_tag, self.loop)

    async def create_task(
        self, channel_name: str,
        kwargs: Mapping[str, Any] = MappingProxyType({}),
        **message_kwargs: Any,
    ) -> Optional[aiormq.abc.ConfirmationFrameType]:

        """ Creates a new task for the worker """
        message = Message(
            body=self.serialize(kwargs),
            content_type=self.CONTENT_TYPE,
            delivery_mode=self.DELIVERY_MODE,
            **message_kwargs,
        )

        return await self.exchange.publish(
            message, channel_name, mandatory=True,
        )


class JsonMaster(Master):
    SERIALIZER = json
    CONTENT_TYPE = "application/json"

    def serialize(self, data: Any) -> bytes:
        return self.SERIALIZER.dumps(data, ensure_ascii=False).encode()


class CompressedJsonMaster(Master):
    SERIALIZER = json
    CONTENT_TYPE = "application/json;compression=gzip"
    COMPRESS_LEVEL = 6

    def serialize(self, data: Any) -> bytes:
        return gzip.compress(
            self.SERIALIZER.dumps(data, ensure_ascii=False).encode(),
            compresslevel=self.COMPRESS_LEVEL,
        )

    def deserialize(self, data: bytes) -> Any:
        return self.SERIALIZER.loads(gzip.decompress(data))
