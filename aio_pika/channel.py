import asyncio
from enum import Enum, unique
from logging import getLogger
from typing import Optional, Union
from warnings import warn

import aiormq
import aiormq.types

from .exchange import Exchange, ExchangeType
from .message import IncomingMessage
from .pool import PoolInstance
from .queue import Queue
from .tools import CallbackCollection
from .transaction import Transaction
from .types import CloseCallbackType, ReturnCallbackType, TimeoutType


log = getLogger(__name__)


@unique
class ConfirmationTypes(Enum):
    ACK = "ack"
    NACK = "nack"


class Channel(PoolInstance):
    """ Channel abstraction """

    QUEUE_CLASS = Queue
    EXCHANGE_CLASS = Exchange

    def __init__(
        self,
        connection,
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

        self.loop = connection.loop

        self._connection = connection
        self._done_callbacks = CallbackCollection(self)
        self._return_callbacks = CallbackCollection(self)
        self._channel = None  # type: Optional[aiormq.Channel]
        self._channel_number = channel_number
        self._on_return_raises = on_return_raises
        self._publisher_confirms = publisher_confirms

        self._delivery_tag = 0

        # noinspection PyTypeChecker
        self.default_exchange = None  # type: Optional[Exchange]

    @property
    def done_callbacks(self) -> CallbackCollection:
        return self._done_callbacks

    @property
    def return_callbacks(self) -> CallbackCollection:
        return self._return_callbacks

    @property
    def is_closed(self):
        if not self._channel or self._is_closed_by_user:
            return True
        return self._channel.is_closed

    @property
    def _is_closed_by_user(self):
        return self._channel == ()

    @_is_closed_by_user.setter
    def _is_closed_by_user(self, value: bool):
        if value:
            self._channel = ()

    async def close(self, exc=None):
        if not self._channel:
            log.warning("Channel already closed")
            return

        if self.is_closed:
            return

        # noinspection PyTypeChecker
        channel = self._channel  # type: aiormq.Channel
        self._is_closed_by_user = True
        await channel.close()

        self._done_callbacks(exc)

    @property
    def channel(self) -> aiormq.Channel:
        if self._channel is None:
            raise aiormq.exceptions.ChannelInvalidStateError(
                "Channel was not opened"
            )

        if self.is_closed:
            raise aiormq.exceptions.ChannelInvalidStateError(
                "Channel has been closed"
            )

        return self._channel

    @property
    def number(self):
        return self._channel.number if self._channel else None

    def __str__(self):
        return "{0}".format(self.number or "Not initialized channel")

    def __repr__(self):
        conn = None

        if self._channel:
            conn = self._channel.connection

        return '<%s "%s#%s">' % (self.__class__.__name__, conn, self)

    def __iter__(self):
        return (yield from self.__await__())

    def __await__(self):
        yield from self.initialize().__await__()
        return self

    async def __aenter__(self):
        if not self._channel:
            await self.initialize()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def add_close_callback(
        self, callback: CloseCallbackType, weak: bool = False
    ) -> None:
        self._done_callbacks.add(callback, weak=weak)

    def remove_close_callback(
        self, callback: CloseCallbackType
    ) -> None:
        self._done_callbacks.remove(callback)

    def add_on_return_callback(
        self, callback: ReturnCallbackType, weak: bool = False,
    ) -> None:
        self._return_callbacks.add(callback, weak=weak)

    def remove_on_return_callback(
        self, callback: ReturnCallbackType, weak: bool = False
    ) -> None:
        self._return_callbacks.remove(callback)

    async def _create_channel(self) -> aiormq.Channel:
        await self._connection.ready()

        return await self._connection.connection.channel(
            publisher_confirms=self._publisher_confirms,
            on_return_raises=self._on_return_raises,
            channel_number=self._channel_number,
        )

    async def initialize(self, timeout: TimeoutType = None) -> None:
        if self._channel is not None:
            raise RuntimeError("Can't initialize channel")

        self._channel = await asyncio.wait_for(
            self._create_channel(), timeout=timeout,
        )

        self._delivery_tag = 0

        if self.default_exchange is None:
            self.default_exchange = self.EXCHANGE_CLASS(
                connection=self._connection,
                channel=self.channel,
                arguments=None,
                auto_delete=None,
                durable=None,
                internal=None,
                name="",
                passive=None,
                type=ExchangeType.DIRECT,
            )

        self.channel.on_return_callbacks.add(self._on_return)
        self.channel.closing.add_done_callback(self._done_callbacks)

    def _on_return(self, message: aiormq.types.DeliveredMessage):
        self._return_callbacks(IncomingMessage(message, no_ack=True))

    async def reopen(self):
        self._channel = None
        await self.initialize()

    async def declare_exchange(
        self,
        name: str,
        type: Union[ExchangeType, str] = ExchangeType.DIRECT,
        durable: bool = None,
        auto_delete: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None,
    ) -> Exchange:
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

        exchange = self.EXCHANGE_CLASS(
            connection=self._connection,
            channel=self.channel,
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

    async def get_exchange(self, name: str, *, ensure: bool = True) -> Exchange:
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
                connection=self._connection,
                channel=self.channel,
                name=name,
                durable=None,
                auto_delete=False,
                internal=False,
                passive=True,
                arguments=None,
            )

    async def declare_queue(
        self,
        name: str = None,
        *,
        durable: bool = None,
        exclusive: bool = False,
        passive: bool = False,
        auto_delete: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None
    ) -> Queue:
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

        queue = self.QUEUE_CLASS(
            connection=self,
            channel=self.channel,
            name=name,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments,
            passive=passive,
        )

        await queue.declare(timeout=timeout)

        return queue

    async def get_queue(self, name: str, *, ensure: bool = True) -> Queue:
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
                connection=self,
                channel=self.channel,
                name=name,
                durable=None,
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
        all_channels: bool = None,
    ) -> aiormq.spec.Basic.QosOk:
        if all_channels is not None:
            warn('Use "global_" instead of "all_channels"', DeprecationWarning)
            global_ = all_channels

        return await asyncio.wait_for(
            self.channel.basic_qos(
                prefetch_count=prefetch_count,
                prefetch_size=prefetch_size,
                global_=global_,
            ),
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
        return await asyncio.wait_for(
            self.channel.queue_delete(
                queue=queue_name,
                if_unused=if_unused,
                if_empty=if_empty,
                nowait=nowait,
            ),
            timeout=timeout,
        )

    async def exchange_delete(
        self,
        exchange_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        nowait: bool = False,
    ) -> aiormq.spec.Exchange.DeleteOk:
        return await asyncio.wait_for(
            self.channel.exchange_delete(
                exchange=exchange_name, if_unused=if_unused, nowait=nowait,
            ),
            timeout=timeout,
        )

    def transaction(self) -> Transaction:
        if self._publisher_confirms:
            raise RuntimeError(
                "Cannot create transaction when publisher "
                "confirms are enabled",
            )

        return Transaction(self._channel)

    async def flow(self, active: bool = True) -> aiormq.spec.Channel.FlowOk:
        return await self.channel.flow(active=active)

    def __del__(self):
        log.debug("%r deleted", self)


__all__ = ("Channel", "ConfirmationTypes")
