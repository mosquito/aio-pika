import asyncio
from collections import namedtuple
from logging import getLogger
from types import FunctionType

import shortuuid

from .common import FutureStore
from .channel import Channel
from .queue import ExchangeType_, Queue, ConsumerTag

log = getLogger(__name__)


DeclarationResult = namedtuple(
    'DeclarationResult', ('message_count', 'consumer_count')
)


class RobustQueue(Queue):
    __slots__ = ('_consumers', '_bindings')

    def __init__(self, loop: asyncio.AbstractEventLoop,
                 future_store: FutureStore, channel: Channel,
                 name, durable, exclusive, auto_delete, arguments,
                 passive: bool = False):

        super().__init__(loop, future_store, channel,
                         name or "amq_%s" % shortuuid.uuid(),
                         durable, exclusive, auto_delete, arguments,
                         passive=passive)

        self._consumers = {}
        self._bindings = {}

    async def on_reconnect(self, channel: Channel):
        self._futures.reject_all(ConnectionError("Auto Reconnect Error"))
        self._channel = channel._channel

        await self.declare()

        for item, kwargs in self._bindings.items():
            exchange, routing_key = item
            await self.bind(exchange, routing_key, **kwargs)

        for consumer_tag, kwargs in tuple(self._consumers.items()):
            await self.consume(consumer_tag=consumer_tag, **kwargs)

    async def bind(self, exchange: ExchangeType_, routing_key: str=None, *,
                   arguments=None, timeout: int=None):

        kwargs = dict(arguments=arguments, timeout=timeout)

        result = await super().bind(
            exchange=exchange,
            routing_key=routing_key,
            **kwargs
        )

        self._bindings[(exchange, routing_key)] = kwargs

        return result

    async def unbind(self, exchange: ExchangeType_, routing_key: str,
                     arguments: dict=None, timeout: int=None):

        result = await super().unbind(exchange, routing_key, arguments, timeout)
        self._bindings.pop((exchange, routing_key), None)

        return result

    async def consume(self, callback: FunctionType, no_ack: bool=False,
                      exclusive: bool=False, arguments: dict=None,
                      consumer_tag=None, timeout=None) -> ConsumerTag:

        kwargs = dict(
            callback=callback,
            no_ack=no_ack,
            exclusive=exclusive,
            arguments=arguments,
        )

        consumer_tag = await super().consume(
            consumer_tag=consumer_tag, **kwargs
        )

        self._consumers[consumer_tag] = kwargs

        return consumer_tag

    async def cancel(self, consumer_tag: ConsumerTag, timeout=None,
                     nowait: bool = False):

        result = await super().cancel(consumer_tag, timeout, nowait)
        self._consumers.pop(consumer_tag, None)
        return result


__all__ = 'RobustQueue',
