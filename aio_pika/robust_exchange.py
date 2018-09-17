import asyncio
from logging import getLogger
from typing import Optional

from .common import FutureStore
from .exchange import Exchange, ExchangeType
from .channel import Channel


log = getLogger(__name__)


class RobustExchange(Exchange):
    """ Exchange abstraction """

    def __init__(self, channel: Channel, publish_method, name: str,
                 type: ExchangeType=ExchangeType.DIRECT, *,
                 auto_delete: Optional[bool], durable: Optional[bool],
                 internal: Optional[bool], passive: Optional[bool],
                 arguments: dict=None, loop: asyncio.AbstractEventLoop,
                 future_store: FutureStore):

        super().__init__(
            channel=channel,
            publish_method=publish_method,
            name=name,
            type=type,
            auto_delete=auto_delete,
            durable=durable,
            internal=internal,
            passive=passive,
            arguments=arguments,
            loop=loop,
            future_store=future_store
        )

        self._bindings = dict()

    async def on_reconnect(self, channel: Channel):
        self._futures.reject_all(ConnectionError("Auto Reconnect Error"))
        self._channel = channel._channel

        await self.declare()

        for exchange, kwargs in self._bindings.items():
            await self.bind(exchange, **kwargs)

    async def bind(self, exchange, routing_key: str='', *,
                   arguments=None, timeout: int=None):
        result = await super().bind(
            exchange, routing_key=routing_key,
            arguments=arguments, timeout=timeout
        )

        self._bindings[exchange] = dict(
            routing_key=routing_key, arguments=arguments
        )

        return result

    async def unbind(self, exchange, routing_key: str = '',
                     arguments: dict=None, timeout: int=None):

        result = await super().unbind(exchange, routing_key,
                                      arguments=arguments, timeout=timeout)
        self._bindings.pop(exchange, None)
        return result


__all__ = ('Exchange',)
