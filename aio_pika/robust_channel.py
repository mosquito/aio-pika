import asyncio
import warnings
from collections import defaultdict
from itertools import chain
from typing import (
    Literal,
    overload,
    Any,
    DefaultDict,
    Dict,
    Optional,
    Set,
    Type,
    Union,
    cast,
)
from warnings import warn

import aiormq

from .abc import (
    AbstractConnection,
    AbstractRobustChannel,
    AbstractRobustExchange,
    AbstractRobustQueue,
    TimeoutType,
)
from .channel import Channel
from .exchange import Exchange, ExchangeType
from .log import get_logger
from .queue import Queue
from .robust_exchange import RobustExchange
from .robust_queue import RobustQueue
from .tools import CallbackCollection


log = get_logger(__name__)


class RobustChannel(Channel, AbstractRobustChannel):
    """Channel abstraction"""

    QUEUE_CLASS: Type[Queue] = RobustQueue
    EXCHANGE_CLASS: Type[Exchange] = RobustExchange

    RESTORE_RETRY_DELAY: int = 2

    _exchanges: DefaultDict[str, AbstractRobustExchange]
    _queues: DefaultDict[str, Set[RobustQueue]]
    default_exchange: RobustExchange

    def __init__(
        self,
        connection: AbstractConnection,
        channel_number: Optional[int] = None,
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

        self._exchanges = defaultdict()
        self._queues = defaultdict(set)
        self._prefetch_count: int = 0
        self._prefetch_size: int = 0
        self._global_qos: bool = False
        self.reopen_callbacks = CallbackCollection(self)
        self.__restore_lock = asyncio.Lock()
        self.__restored = asyncio.Event()
        self._close_called = False

        self.close_callbacks.remove(self._set_closed_callback)

    async def ready(self) -> None:
        await self._connection.ready()
        await self.__restored.wait()

    async def get_underlay_channel(self) -> aiormq.abc.AbstractChannel:
        await self._connection.ready()
        return await super().get_underlay_channel()

    async def restore(self, channel: Any = None) -> None:
        if channel is not None:
            warnings.warn(
                "Channel argument will be ignored because you "
                "don't need to pass this anymore.",
                DeprecationWarning,
            )

        async with self.__restore_lock:
            if (
                self.__restored.is_set()
                or self._close_called
                or self._connection.close_called
                or self._connection.is_closed
            ):
                return

            await self.reopen()
            self.__restored.set()

    async def _on_close(self, closing: asyncio.Future) -> BaseException | None:
        exc = await super()._on_close(closing)

        if isinstance(exc, asyncio.CancelledError):
            # Connection forced close (e.g., stuck connection).
            # Clear restored but do NOT set _closed - the robust connection
            # will restore this channel via restore() -> reopen() -> _open()
            # which properly handles _closed state reset.
            self.__restored.clear()
            return exc

        in_restore_state = not self.__restored.is_set()
        self.__restored.clear()

        if (
            self._closed.done()
            or in_restore_state
            or self._connection.close_called
            or self._connection.is_closed
        ):
            return exc

        try:
            await self.restore()
        except Exception:
            # Shutdown may start while reopening is awaiting the transport.
            # Recovery is no longer needed once the connection is closing.
            if not (
                self._connection.close_called or self._connection.is_closed
            ):
                raise

        return exc

    async def close(
        self,
        exc: aiormq.abc.ExceptionType | None = None,
    ) -> None:
        self._close_called = True
        self.__restored.clear()
        await super().close(exc)

    async def reopen(self) -> None:
        self._close_called = False
        await super().reopen()
        await self.reopen_callbacks()

    async def _on_open(self) -> None:
        if not hasattr(self, "default_exchange"):
            await super()._on_open()

        exchanges = self._exchanges.values()
        queues = tuple(chain(*self._queues.values()))
        channel = await self.get_underlay_channel()
        channel.on_consumer_cancel_callbacks.add(self._on_consumer_cancel)

        await channel.basic_qos(
            prefetch_count=self._prefetch_count,
            prefetch_size=self._prefetch_size,
            global_=self._global_qos,
        )

        for exchange in exchanges:
            await exchange.restore()

        for queue in queues:
            await queue.restore()

        if hasattr(self, "default_exchange"):
            self.default_exchange.channel = self

        self.__restored.set()

    def _on_consumer_cancel(self, frame: aiormq.spec.Basic.Cancel) -> None:
        from .robust_connection import RobustConnection

        if isinstance(self._connection, RobustConnection) and any(
            frame.consumer_tag in queue._consumers
            for queue in chain(*self._queues.values())
        ):
            self._connection._request_recovery()

    async def _restore_consumers(self) -> bool:
        if self._close_called or self._closed.done():
            return False
        try:
            await self.restore()
        except Exception:
            log.debug("Channel recovery failed; retrying", exc_info=True)
            return True
        channel = cast(aiormq.Channel, await self.get_underlay_channel())
        recovery = asyncio.create_task(self._restore_missing_consumers(channel))
        try:
            await asyncio.wait(
                (recovery, self._closed, channel.closing),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._close_called or self._closed.done():
                return False
            if channel.closing.done():
                return True
            return await recovery
        finally:
            recovery.cancel()
            await asyncio.gather(recovery, return_exceptions=True)

    async def _restore_missing_consumers(self, channel: aiormq.Channel) -> bool:
        retry = False
        async with self.__restore_lock:
            for queue in tuple(chain(*self._queues.values())):
                tags = queue._consumers.keys() - channel.consumers.keys()
                if not tags:
                    continue
                try:
                    probe = await channel.connection.channel(
                        publisher_confirms=False,
                        timeout=5,
                    )
                    try:
                        await queue._probe(probe)
                    finally:
                        await probe.close()
                    if not self.__restored.is_set() or channel.is_closed:
                        return False
                    for tag in tags:
                        kwargs = queue._consumers.get(tag)
                        if kwargs is None or tag in channel.consumers:
                            continue
                        await Queue.consume(
                            queue,
                            consumer_tag=tag,
                            timeout=5,
                            **kwargs,
                        )
                        if queue._consumers.get(tag) is not kwargs:
                            await channel.basic_cancel(tag)
                except Exception:
                    log.debug(
                        "Consumer recovery failed for %r; retrying",
                        queue,
                        exc_info=True,
                    )
                    retry = True
        return retry

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

        await self.ready()

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
        arguments: Optional[Dict[str, Any]] = None,
        timeout: TimeoutType = None,
        robust: bool = True,
    ) -> AbstractRobustExchange:
        """
        :param robust: If True, the exchange will be re-declared during
        reconnection.
        Set to False for temporary exchanges that should not be restored.
        """
        await self.ready()
        # Passive is True so expecting the exchange to be already declared
        # if we can just return it instead of creating a new class instance
        if passive and name in self._exchanges:
            return self._exchanges[name]

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
        exchange = cast(AbstractRobustExchange, exchange)

        if not internal and robust:
            self._exchanges[name] = exchange

        return exchange

    @overload
    async def exchange_delete(
        self,
        exchange_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        nowait: Literal[False] = False,
    ) -> aiormq.spec.Exchange.DeleteOk: ...

    @overload
    async def exchange_delete(
        self,
        exchange_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        nowait: Literal[True] = True,
    ) -> None: ...

    @overload
    async def exchange_delete(
        self,
        exchange_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        nowait: bool = False,
    ) -> aiormq.spec.Exchange.DeleteOk | None: ...

    async def exchange_delete(
        self,
        exchange_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        nowait: bool = False,
    ) -> aiormq.spec.Exchange.DeleteOk | None:
        await self.ready()
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
        name: Optional[str] = None,
        *,
        durable: bool = False,
        exclusive: bool = False,
        passive: bool = False,
        auto_delete: bool = False,
        arguments: Optional[Dict[str, Any]] = None,
        timeout: TimeoutType = None,
        robust: bool = True,
    ) -> AbstractRobustQueue:
        """
        :param robust: If True, the queue will be re-declared during
        reconnection.
        Set to False for temporary queues that should not be restored.
        """
        await self.ready()
        # Passive is True so expecting the queue to be already declared
        # if we can just return it instead of creating a new class instance
        if passive and name and name in self._queues:
            return list(self._queues[name])[0]

        queue = await super().declare_queue(
            name=name,
            durable=durable,
            exclusive=exclusive,
            passive=passive,
            auto_delete=auto_delete,
            arguments=arguments,
            timeout=timeout,
        )
        queue = cast(RobustQueue, queue)

        if robust:
            self._queues[queue.name].add(queue)
        return queue

    @overload
    async def queue_delete(
        self,
        queue_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        if_empty: bool = False,
        nowait: Literal[False] = False,
    ) -> aiormq.spec.Queue.DeleteOk: ...

    @overload
    async def queue_delete(
        self,
        queue_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        if_empty: bool = False,
        nowait: Literal[True] = True,
    ) -> None: ...

    @overload
    async def queue_delete(
        self,
        queue_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        if_empty: bool = False,
        nowait: bool = False,
    ) -> aiormq.spec.Queue.DeleteOk | None: ...

    async def queue_delete(
        self,
        queue_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        if_empty: bool = False,
        nowait: bool = False,
    ) -> aiormq.spec.Queue.DeleteOk | None:
        await self.ready()
        queues = self._queues.pop(queue_name, set())
        for queue in queues:
            queue._consumers.clear()
        result = await super().queue_delete(
            queue_name=queue_name,
            timeout=timeout,
            if_unused=if_unused,
            if_empty=if_empty,
            nowait=nowait,
        )
        return result


__all__ = ("RobustChannel",)
