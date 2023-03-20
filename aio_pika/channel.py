import asyncio
import warnings
from abc import ABC
from types import TracebackType
from typing import Any, AsyncContextManager, Generator, Optional, Type, Union
from warnings import warn

import aiormq
import aiormq.abc
from pamqp.common import Arguments

from .abc import (
    AbstractChannel, AbstractExchange, AbstractQueue, TimeoutType,
    UnderlayChannel,
)
from .exchange import Exchange, ExchangeType
from .log import get_logger
from .message import IncomingMessage
from .queue import Queue
from .tools import CallbackCollection
from .transaction import Transaction


log = get_logger(__name__)


class ChannelContext(AsyncContextManager, AbstractChannel, ABC):
    async def __aenter__(self) -> "AbstractChannel":
        if not self.is_initialized:
            await self.initialize()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        return await self.close(exc_val)

    def __await__(self) -> Generator[Any, Any, AbstractChannel]:
        yield from self.initialize().__await__()
        return self


class Channel(ChannelContext):
    """Channel abstraction"""

    QUEUE_CLASS = Queue
    EXCHANGE_CLASS = Exchange

    _channel: Optional[UnderlayChannel]

    def __init__(
        self,
        connection: aiormq.abc.AbstractConnection,
        channel_number: Optional[int] = None,
        publisher_confirms: bool = True,
        on_return_raises: bool = False,
    ):
        """

        :param connection: :class:`aio_pika.adapter.AsyncioConnection` instance
        :param loop: Event loop (:func:`asyncio.get_event_loop()`
                when :class:`None`)
        :param future_store: :class:`aio_pika.common.FutureStore` instance
        :param publisher_confirms: False if you don't need delivery
                confirmations (in pursuit of performance)
        """

        if not publisher_confirms and on_return_raises:
            raise RuntimeError(
                '"on_return_raises" not applicable '
                'without "publisher_confirms"',
            )

        self._connection: aiormq.abc.AbstractConnection = connection

        # That's means user closed channel instance explicitly
        self._closed: bool = False

        self._channel: Optional[UnderlayChannel] = None
        self._channel_number = channel_number

        self.close_callbacks = CallbackCollection(self)
        self.return_callbacks = CallbackCollection(self)

        self.publisher_confirms = publisher_confirms
        self.on_return_raises = on_return_raises

    @property
    def is_initialized(self) -> bool:
        """Returns True when the channel has been opened
        and ready for interaction"""
        return self._channel is not None

    @property
    def is_closed(self) -> bool:
        """Returns True when the channel has been closed from the broker
        side or after the close() method has been called."""
        if not self.is_initialized or self._closed:
            return True
        if not self._channel:
            return True
        return self._channel.channel.is_closed

    async def close(
        self,
        exc: Optional[aiormq.abc.ExceptionType] = None,
    ) -> None:
        if not self.is_initialized:
            log.warning("Channel not opened")
            return

        if not self._channel:
            log.warning("Transport is not ready")
            return

        log.debug("Closing channel %r", self)
        self._closed = True
        await self._channel.close()

    async def get_underlay_channel(self) -> aiormq.abc.AbstractChannel:

        if not self.is_initialized or not self._channel:
            raise aiormq.exceptions.ChannelInvalidStateError(
                "Channel was not opened",
            )

        if self.is_closed:
            raise aiormq.exceptions.ChannelInvalidStateError(
                "Channel has been closed",
            )

        return self._channel.channel

    @property
    def channel(self) -> Optional[aiormq.abc.AbstractChannel]:
        warnings.warn("This property is deprecated, do not use this anymore.")
        if self._channel is None:
            raise aiormq.exceptions.ChannelInvalidStateError
        return self._channel.channel

    @property
    def number(self) -> Optional[int]:
        if self._channel is None:
            return self._channel_number

        underlay_channel: UnderlayChannel = self._channel
        return underlay_channel.channel.number

    def __str__(self) -> str:
        return "{}".format(self.number or "Not initialized channel")

    async def _open(self) -> None:
        await self._connection.ready()

        channel = await UnderlayChannel.create(
            self._connection,
            self._on_close,
            publisher_confirms=self.publisher_confirms,
            on_return_raises=self.on_return_raises,
            channel_number=self._channel_number,
        )

        await self._on_open(channel.channel)
        self._channel = channel
        self._closed = False

    async def initialize(self, timeout: TimeoutType = None) -> None:
        if self.is_initialized:
            raise RuntimeError("Already initialized")
        elif self._closed:
            raise RuntimeError("Can't initialize closed channel")

        await self._open()
        await self._on_initialized()

    async def _on_open(self, channel: aiormq.abc.AbstractChannel) -> None:
        self.default_exchange: Exchange = self.EXCHANGE_CLASS(
            channel=channel,
            arguments=None,
            auto_delete=False,
            durable=False,
            internal=False,
            name="",
            passive=False,
            type=ExchangeType.DIRECT,
        )

    async def _on_close(self, closing: asyncio.Future) -> None:
        try:
            exc = closing.exception()
        except asyncio.CancelledError as e:
            exc = e
        await self.close_callbacks(exc)

        if self._channel and self._channel.channel:
            self._channel.channel.on_return_callbacks.discard(self._on_return)

    async def _on_initialized(self) -> None:
        channel = await self.get_underlay_channel()
        channel.on_return_callbacks.add(self._on_return)

    def _on_return(self, message: aiormq.abc.DeliveredMessage) -> None:
        self.return_callbacks(IncomingMessage(message, no_ack=True))

    async def reopen(self) -> None:
        log.debug("Start reopening channel %r", self)
        await self._open()

    def __del__(self) -> None:
        self._closed = True
        self._channel = None

    async def declare_exchange(
        self,
        name: str,
        type: Union[ExchangeType, str] = ExchangeType.DIRECT,
        *,
        durable: bool = False,
        auto_delete: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
    ) -> AbstractExchange:
        """
        Declare an exchange.

        :param name: string with exchange name or
            :class:`aio_pika.exchange.Exchange` instance
        :param type: Exchange type. Enum ExchangeType value or string.
            String values must be one of 'fanout', 'direct', 'topic',
            'headers', 'x-delayed-message', 'x-consistent-hash'.
        :param durable: Durability (exchange survive broker restart)
        :param auto_delete: Delete queue when channel will be closed.
        :param internal: Do not send it to broker just create an object
        :param passive: Do not fail when entity was declared
            previously but has another params. Raises
            :class:`aio_pika.exceptions.ChannelClosed` when exchange
            doesn't exist.
        :param arguments: additional arguments
        :param timeout: execution timeout
        :return: :class:`aio_pika.exchange.Exchange` instance
        """

        if auto_delete and durable is None:
            durable = False

        channel = await self.get_underlay_channel()

        exchange = self.EXCHANGE_CLASS(
            channel=channel,
            name=name,
            type=type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            passive=passive,
            arguments=arguments,
        )

        await exchange.declare(timeout=timeout)

        log.debug("Exchange declared %r", exchange)

        return exchange

    async def get_exchange(
        self, name: str, *, ensure: bool = True,
    ) -> AbstractExchange:
        """
        With ``ensure=True``, it's a shortcut for
        ``.declare_exchange(..., passive=True)``; otherwise, it returns an
        exchange instance without checking its existence.

        When the exchange does not exist, if ``ensure=True``, will raise
        :class:`aio_pika.exceptions.ChannelClosed`.

        Use this method in a separate channel (or as soon as channel created).
        This is only a way to get an exchange without declaring a new one.

        :param name: exchange name
        :param ensure: ensure that the exchange exists
        :return: :class:`aio_pika.exchange.Exchange` instance
        :raises: :class:`aio_pika.exceptions.ChannelClosed` instance
        """

        if ensure:
            return await self.declare_exchange(name=name, passive=True)
        else:
            return self.EXCHANGE_CLASS(
                channel=await self.get_underlay_channel(),
                name=name,
                durable=False,
                auto_delete=False,
                internal=False,
                passive=True,
                arguments=None,
            )

    async def declare_queue(
        self,
        name: Optional[str] = None,
        *,
        durable: bool = False,
        exclusive: bool = False,
        passive: bool = False,
        auto_delete: bool = False,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
    ) -> AbstractQueue:
        """

        :param name: queue name
        :param durable: Durability (queue survive broker restart)
        :param exclusive: Makes this queue exclusive. Exclusive queues may only
            be accessed by the current connection, and are deleted when that
            connection closes. Passive declaration of an exclusive queue by
            other connections are not allowed.
        :param passive: Do not fail when entity was declared
            previously but has another params. Raises
            :class:`aio_pika.exceptions.ChannelClosed` when queue
            doesn't exist.
        :param auto_delete: Delete queue when channel will be closed.
        :param arguments: additional arguments
        :param timeout: execution timeout
        :return: :class:`aio_pika.queue.Queue` instance
        :raises: :class:`aio_pika.exceptions.ChannelClosed` instance
        """

        queue: AbstractQueue = self.QUEUE_CLASS(
            channel=await self.get_underlay_channel(),
            name=name,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments,
            passive=passive,
        )

        await queue.declare(timeout=timeout)
        self.close_callbacks.add(queue.close_callbacks, weak=True)
        return queue

    async def get_queue(
        self, name: str, *, ensure: bool = True,
    ) -> AbstractQueue:
        """
        With ``ensure=True``, it's a shortcut for
        ``.declare_queue(..., passive=True)``; otherwise, it returns a
        queue instance without checking its existence.

        When the queue does not exist, if ``ensure=True``, will raise
        :class:`aio_pika.exceptions.ChannelClosed`.

        Use this method in a separate channel (or as soon as channel created).
        This is only a way to get a queue without declaring a new one.

        :param name: queue name
        :param ensure: ensure that the queue exists
        :return: :class:`aio_pika.queue.Queue` instance
        :raises: :class:`aio_pika.exceptions.ChannelClosed` instance
        """

        if ensure:
            return await self.declare_queue(name=name, passive=True)
        else:
            return self.QUEUE_CLASS(
                channel=await self.get_underlay_channel(),
                name=name,
                durable=False,
                exclusive=False,
                auto_delete=False,
                arguments=None,
                passive=True,
            )

    async def set_qos(
        self,
        prefetch_count: int = 0,
        prefetch_size: int = 0,
        global_: bool = False,
        timeout: TimeoutType = None,
        all_channels: Optional[bool] = None,
    ) -> aiormq.spec.Basic.QosOk:
        if all_channels is not None:
            warn('Use "global_" instead of "all_channels"', DeprecationWarning)
            global_ = all_channels

        channel = await self.get_underlay_channel()

        return await channel.basic_qos(
            prefetch_count=prefetch_count,
            prefetch_size=prefetch_size,
            global_=global_,
            timeout=timeout,
        )

    async def queue_delete(
        self,
        queue_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        if_empty: bool = False,
        nowait: bool = False,
    ) -> aiormq.spec.Queue.DeleteOk:
        channel = await self.get_underlay_channel()
        return await channel.queue_delete(
            queue=queue_name,
            if_unused=if_unused,
            if_empty=if_empty,
            nowait=nowait,
            timeout=timeout,
        )

    async def exchange_delete(
        self,
        exchange_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        nowait: bool = False,
    ) -> aiormq.spec.Exchange.DeleteOk:
        channel = await self.get_underlay_channel()
        return await channel.exchange_delete(
            exchange=exchange_name,
            if_unused=if_unused,
            nowait=nowait,
            timeout=timeout,
        )

    def transaction(self) -> Transaction:
        if self.publisher_confirms:
            raise RuntimeError(
                "Cannot create transaction when publisher "
                "confirms are enabled",
            )

        return Transaction(self)

    async def flow(self, active: bool = True) -> aiormq.spec.Channel.FlowOk:
        channel = await self.get_underlay_channel()
        return await channel.flow(active=active)


__all__ = ("Channel",)
