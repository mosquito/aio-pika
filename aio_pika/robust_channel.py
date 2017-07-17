#!/usr/bin/env python
# encoding: utf-8
import asyncio
import logging

from . import Channel, Connection, ExchangeType
from .common import FutureStore, State
from .robust_queue import RobustQueue
from .robust_exchange import RobustExchange

log = logging.getLogger(__name__)


class RobustChannel(Channel):
    __slots__ = '__exchanges', '__queues', '_closing', '_connection', '__channel', '__qos', '_state'

    QUEUE_CLASS = RobustQueue
    EXCHANGE_CLASS = RobustExchange

    def __init__(self, connection, loop: asyncio.AbstractEventLoop, future_store: FutureStore):
        super().__init__(connection, loop, future_store)
        self.__queues = dict()
        self.__exchanges = dict()
        self.__qos = dict()

        self.__exchanges[self.default_exchange.name] = self.default_exchange

    @asyncio.coroutine
    def set_connection(self, connection: Connection):
        log.debug("Setting a new connection %r for channel %r", connection, self)

        self.state = State.INITIALIZED

        self._connection = connection
        self.__channel = yield from self._create_channel()

        self.state = State.READY

        for exchange in self.__exchanges.values():     # type: RobustExchange
            yield from exchange.set_channel(self.__channel)

        for queue in self.__queues.values():     # type: RobustQueue
            yield from queue.set_channel(self.__channel)

    @asyncio.coroutine
    def declare_exchange(self, name: str, type: ExchangeType = ExchangeType.DIRECT,
                         durable: bool = None, auto_delete: bool = False,
                         internal: bool = False, arguments: dict = None, timeout: int = None):
        exchange = yield from super().declare_exchange(
            name=name, type=type, durable=durable, auto_delete=auto_delete,
            internal=internal, arguments=arguments, timeout=timeout
        )

        self.__exchanges[exchange.name] = exchange
        return exchange

    @asyncio.coroutine
    def declare_queue(self, name: str = None, *, durable: bool = None, exclusive: bool = False,
                      passive: bool = False, auto_delete: bool = False,
                      arguments: dict = None, timeout: int = None):
        queue = yield from super().declare_queue(
            name=name, durable=durable, exclusive=exclusive, passive=passive,
            auto_delete=auto_delete, arguments=arguments, timeout=timeout
        )

        self.__queues[queue.name] = queue
        return queue

    @asyncio.coroutine
    def exchange_delete(self, exchange_name: str, timeout: int = None,
                        if_unused=False, nowait=False):
        result = yield from super().exchange_delete(
            exchange_name=exchange_name, timeout=timeout,
            if_unused=if_unused, nowait=nowait
        )

        self.__exchanges.pop(exchange_name)
        return result

    @asyncio.coroutine
    def queue_delete(self, queue_name: str, timeout: int = None,
                     if_unused: bool = False, if_empty: bool = False,
                     nowait: bool = False):
        result = yield from super().queue_delete(
            queue_name=queue_name, timeout=timeout,
            if_empty=if_empty, if_unused=if_unused,
            nowait=nowait
        )

        self.__queues.pop(queue_name)
        return result

    @asyncio.coroutine
    def set_qos(self, prefetch_count: int = 0, prefetch_size: int = 0, timeout: int = None, **_):
        self.__qos = dict(
            prefetch_count=prefetch_count,
            prefetch_size=prefetch_size,
            all_channels=False,
            timeout=timeout,
        )

        return (yield from self.set_qos(**self.__qos))
