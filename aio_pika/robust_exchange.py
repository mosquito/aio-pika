import asyncio
from typing import Optional

from .common import FutureStore
from .exchange import Exchange, ExchangeType
from .channel import Channel


class RobustExchange(Exchange):
    def __init__(self, channel: Channel, publish_method, name: str,
                 type: ExchangeType = ExchangeType.DIRECT, *, auto_delete: Optional[bool],
                 durable: Optional[bool], internal: Optional[bool],
                 arguments: dict = None, loop: asyncio.AbstractEventLoop,
                 future_store: FutureStore):

        super().__init__(channel=channel, publish_method=publish_method,
                         name=name, type=type, auto_delete=auto_delete,
                         durable=durable, internal=internal,
                         arguments=arguments, loop=loop,
                         future_store=future_store)

        self._bindings = dict()

    @asyncio.coroutine
    def set_channel(self, channel: Channel):
        self._channel = channel
        result = yield from self.declare()

        return result

    @asyncio.coroutine
    def bind(self, exchange,
             routing_key: str = '', *, arguments=None, timeout: int = None):

        result = yield from super().bind(
            exchange,
            routing_key=routing_key,
            arguments=arguments,
            timeout=timeout
        )
        self._bindings[(exchange, routing_key)] = arguments
        return result

    @asyncio.coroutine
    def unbind(self, exchange, routing_key: str = '',
               arguments: dict = None, timeout: int = None):

        result = yield from super().unbind(
            exchange,
            routing_key=routing_key,
            timeout=timeout
        )
        self._bindings.pop((exchange, routing_key))
        return result
