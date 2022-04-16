from collections import defaultdict
from itertools import chain
from logging import getLogger
from typing import DefaultDict, Set, Type, Union
from warnings import warn

import aiormq

from .abc import (
    AbstractRobustChannel, AbstractRobustExchange, AbstractRobustQueue,
    TimeoutType, UnderlayConnection,
)
from .channel import Channel
from .exchange import Exchange, ExchangeType
from .queue import Queue
from .robust_exchange import RobustExchange
from .robust_queue import RobustQueue
from .tools import CallbackCollection


log = getLogger(__name__)


class RobustChannel(Channel, AbstractRobustChannel):
    """ Channel abstraction """

    QUEUE_CLASS: Type[Queue] = RobustQueue
    EXCHANGE_CLASS: Type[Exchange] = RobustExchange

    _exchanges: DefaultDict[str, Set[AbstractRobustExchange]]
    _queues: DefaultDict[str, Set[RobustQueue]]
    default_exchange: RobustExchange

    def __init__(
        self,
        transport: UnderlayConnection,
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
            transport=transport,
            channel_number=channel_number,
            publisher_confirms=publisher_confirms,
            on_return_raises=on_return_raises,
        )

        self._exchanges = defaultdict(set)
        self._queues = defaultdict(set)
        self._prefetch_count: int = 0
        self._prefetch_size: int = 0
        self._global_qos: bool = False
        self.reopen_callbacks: CallbackCollection = CallbackCollection(self)
        self.close_callbacks.add(self.__close_callback)

    async def __close_callback(self, *_) -> None:
        if self._closed or not self.ready.is_set():
            return
        await self.reopen()

    async def reopen(self) -> None:
        await super().reopen()
        await self.ready.wait()
        await self.reopen_callbacks()

    async def _on_open(self, channel: aiormq.abc.AbstractChannel) -> None:
        await self.set_qos(
            prefetch_count=self._prefetch_count,
            prefetch_size=self._prefetch_size,
            global_=self._global_qos,
        )

        async with self._operation_lock:
            exchanges = tuple(chain(*self._exchanges.values()))
            queues = tuple(chain(*self._queues.values()))

            for exchange in exchanges:
                await exchange.restore(self)

            for queue in queues:
                await queue.restore(self)

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

        await self._transport.ready()

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
        durable: bool = False,
        auto_delete: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None,
        robust: bool = True,
    ) -> AbstractRobustExchange:
        await self._transport.ready()
        exchange = (
            await super().declare_exchange(
                name=name,
                type=type,
                durable=durable,
                auto_delete=auto_delete,
                internal=internal,
                passive=passive,
                arguments=arguments,
                timeout=timeout,
            )
        )

        if not internal and robust:
            # noinspection PyTypeChecker
            self._exchanges[name].add(exchange)     # type: ignore

        return exchange     # type: ignore

    async def exchange_delete(
        self,
        exchange_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        nowait: bool = False,
    ) -> aiormq.spec.Exchange.DeleteOk:
        await self._transport.ready()
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
        durable: bool = False,
        exclusive: bool = False,
        passive: bool = False,
        auto_delete: bool = False,
        arguments: dict = None,
        timeout: TimeoutType = None,
        robust: bool = True
    ) -> AbstractRobustQueue:
        await self._transport.ready()
        queue: RobustQueue = await super().declare_queue(   # type: ignore
            name=name,
            durable=durable,
            exclusive=exclusive,
            passive=passive,
            auto_delete=auto_delete,
            arguments=arguments,
            timeout=timeout,
        )

        if robust:
            self._queues[queue.name].add(queue)

        return queue

    async def queue_delete(
        self,
        queue_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        if_empty: bool = False,
        nowait: bool = False,
    ) -> aiormq.spec.Queue.DeleteOk:
        await self._transport.ready()
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
