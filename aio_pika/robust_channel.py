import asyncio
from collections import defaultdict
from logging import getLogger
from typing import DefaultDict, Optional, Set, Type, Union
from warnings import warn

import aiormq

from .abc import TimeoutType
from .channel import Channel
from .exchange import Exchange, ExchangeType
from .queue import Queue
from .robust_exchange import RobustExchange
from .robust_queue import RobustQueue


log = getLogger(__name__)


class RobustChannel(Channel):
    """ Channel abstraction """

    QUEUE_CLASS: Type[Queue] = RobustQueue
    EXCHANGE_CLASS: Type[Exchange] = RobustExchange

    _exchanges: DefaultDict[str, Set[RobustExchange]]
    _queues: DefaultDict[str, Set[RobustQueue]]
    default_exchange: Optional[RobustExchange]

    def __init__(
        self,
        connection,
        channel_number: int = None,
        publisher_confirms: bool = True,
        on_return_raises: bool = False,
    ):
        """

        :param connection: :class:`aio_pika.adapter.AsyncioConnection` instance
        :param loop:
            Event loop (:func:`asyncio.get_event_loop()` when :class:`None`)
        :param future_store: :class:`aio_pika.common.FutureStore` instance
        :param publisher_confirms:
            False if you don't need delivery confirmations
            (in pursuit of performance)
        """

        super().__init__(
            connection=connection,
            channel_number=channel_number,
            publisher_confirms=publisher_confirms,
            on_return_raises=on_return_raises,
        )

        self._exchanges = defaultdict(set)
        self._queues = defaultdict(set)
        self._prefetch_count: int = 0
        self._prefetch_size: int = 0
        self._global_qos: bool = False

    async def reopen(self) -> None:
        log.debug("Reopening channel %r", self)
        await super().reopen()
        await self.restore()

    async def restore(self) -> None:
        await self.set_qos(
            prefetch_count=self._prefetch_count,
            prefetch_size=self._prefetch_size,
            global_=self._global_qos,
        )

        await self.default_exchange.restore(self)

        for exchanges in self._exchanges.values():
            for exchange in exchanges:
                await exchange.restore(self)

        for queues in self._queues.values():
            for queue in queues:
                await queue.restore(self)

    def _on_initialized(self):
        self.channel.on_return_callbacks.add(self._on_return)

    async def set_qos(
        self,
        prefetch_count: int = 0,
        prefetch_size: int = 0,
        global_: bool = False,
        timeout: TimeoutType = None,
        all_channels: bool = None,
    ):
        if all_channels is not None:
            warn('Use "global_" instead of "all_channels"', DeprecationWarning)
            global_ = all_channels

        self._prefetch_count = prefetch_count
        self._prefetch_size = prefetch_size
        self._global_qos = global_

        return await super().set_qos(
            prefetch_count=prefetch_count,
            prefetch_size=prefetch_size,
            global_=global_,
            timeout=timeout,
        )

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
        robust: bool = True,
    ) -> RobustExchange:
        exchange = await super().declare_exchange(
            name=name,
            type=type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            passive=passive,
            arguments=arguments,
            timeout=timeout,
        )

        if not internal and robust:
            self._exchanges[name].add(exchange)

        return exchange

    async def exchange_delete(
        self,
        exchange_name: str,
        timeout: TimeoutType = None,
        if_unused=False,
        nowait=False,
    ) -> aiormq.spec.Exchange.DeleteOk:

        result = await super().exchange_delete(
            exchange_name=exchange_name,
            timeout=timeout,
            if_unused=if_unused,
            nowait=nowait,
        )

        self._exchanges.pop(exchange_name, None)

        return result

    async def declare_queue(
        self,
        name: str = None,
        *,
        durable: bool = None,
        exclusive: bool = False,
        passive: bool = False,
        auto_delete: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None,
        robust: bool = True
    ) -> Queue:

        queue = await super().declare_queue(
            name=name,
            durable=durable,
            exclusive=exclusive,
            passive=passive,
            auto_delete=auto_delete,
            arguments=arguments,
            timeout=timeout,
        )

        if robust:
            self._queues[name].add(queue)

        return queue

    async def queue_delete(
        self,
        queue_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        if_empty: bool = False,
        nowait: bool = False,
    ):
        result = await super().queue_delete(
            queue_name=queue_name,
            timeout=timeout,
            if_unused=if_unused,
            if_empty=if_empty,
            nowait=nowait,
        )

        self._queues.pop(queue_name, None)
        return result


__all__ = ("RobustChannel",)
