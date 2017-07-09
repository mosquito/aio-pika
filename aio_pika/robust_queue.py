import asyncio
from types import FunctionType

import shortuuid

from . import Channel, Exchange
from .common import FutureStore
from .queue import Queue


class RobustQueue(Queue):
    def __init__(self, loop: asyncio.AbstractEventLoop, future_store: FutureStore,
                 channel: Channel, name, durable, exclusive, auto_delete, arguments):

        # Have to create queue-name for reconnection
        if not name:
            name = 'tmp-{}'.format(shortuuid.uuid())

        super().__init__(loop, future_store, channel, name, durable, exclusive, auto_delete, arguments)
        self._consumers = dict()
        self._bindings = dict()

    @asyncio.coroutine
    def set_channel(self, channel: Channel):
        self._channel = channel
        result = yield from self.declare(passive=self._passive)

        for exc_rk, arguments in tuple(self._bindings.items()):
            exchange, routing_key = exc_rk
            yield from self.bind(exchange=exchange, routing_key=routing_key, arguments=arguments)

        for consumer, kwargs in tuple(self._consumers.items()):
            self.consume(consumer, **kwargs)

        return result

    def consume(self, callback: FunctionType, no_ack: bool = False,
                exclusive: bool = False, arguments: dict = None):
        super().consume(callback=callback, no_ack=no_ack,
                        exclusive=exclusive, arguments=arguments)

        self._consumers[callback] = dict(no_ack=no_ack, exclusive=exclusive, arguments=arguments)

    @asyncio.coroutine
    def bind(self, exchange: Exchange,
             routing_key: str=None, *, arguments=None, timeout: int = None):
        result = yield from super().bind(
            exchange,
            routing_key=routing_key,
            arguments=arguments,
            timeout=timeout
        )
        self._bindings[(exchange, routing_key)] = arguments
        return result

    @asyncio.coroutine
    def unbind(self, exchange: Exchange, routing_key: str,
               arguments: dict = None, timeout: int = None):

        result = yield from super().unbind(
            exchange,
            routing_key=routing_key,
            timeout=timeout
        )
        self._bindings.pop((exchange, routing_key))
        return result
