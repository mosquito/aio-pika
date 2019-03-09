import os
from base64 import b32encode
from collections import namedtuple
from logging import getLogger
from types import FunctionType

import aiormq

from .channel import Channel
from .exchange import ExchangeType_
from .queue import Queue, ConsumerTag

log = getLogger(__name__)


DeclarationResult = namedtuple(
    'DeclarationResult', ('message_count', 'consumer_count')
)


class RobustQueue(Queue):
    __slots__ = ('_consumers', '_bindings')

    @staticmethod
    def _get_random_queue_name():
        rb = os.urandom(16)
        return "amq_%s" % b32encode(rb).decode().replace("=", "").lower()

    def __init__(self, connection, channel: aiormq.Channel, name,
                 durable, exclusive, auto_delete, arguments,
                 passive: bool = False):

        super().__init__(
            connection=connection,
            channel=channel,
            name=name or self._get_random_queue_name(),
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments,
            passive=passive
        )

        self._consumers = {}
        self._bindings = {}

    async def on_reconnect(self, channel: Channel):
        self._channel = channel._channel

        await self.declare()

        for item, kwargs in self._bindings.items():
            exchange, routing_key = item
            await self.bind(exchange, routing_key, **kwargs)

        for consumer_tag, kwargs in tuple(self._consumers.items()):
            await self.consume(consumer_tag=consumer_tag, **kwargs)

    async def bind(self, exchange: ExchangeType_, routing_key: str=None, *,
                   arguments=None, timeout: int=None):

        if routing_key is None:
            routing_key = self.name

        kwargs = dict(arguments=arguments, timeout=timeout)

        result = await super().bind(
            exchange=exchange,
            routing_key=routing_key,
            **kwargs
        )

        self._bindings[(exchange, routing_key)] = kwargs

        return result

    async def unbind(self, exchange: ExchangeType_, routing_key: str=None,
                     arguments: dict=None, timeout: int=None):

        if routing_key is None:
            routing_key = self.name

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
