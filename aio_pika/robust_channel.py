#!/usr/bin/env python
# encoding: utf-8
import asyncio

import pika.channel

from aio_pika import Channel, Connection, ExchangeType
from aio_pika.common import FutureStore
from aio_pika.tools import create_future


class RobustChannel(Channel):
    __slots__ = '__exchanges', '__queues', '_closing', '__connection', '__channel', '__qos'

    def __init__(self, connection, loop: asyncio.AbstractEventLoop, future_store: FutureStore):
        super().__init__(connection, loop, future_store)
        self.__queues = dict()
        self.__exchanges = dict()
        self.__qos = dict()

    @asyncio.coroutine
    def _on_channel_close(self, channel: pika.channel.Channel, code: int, reason):
        super()._on_channel_close(channel, code, reason)

    @asyncio.coroutine
    def set_connection(self, connection: Connection):
        self._closing = create_future(loop=self.loop)
        self.__connection = connection
        self.__channel = yield from self._create_channel()

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
