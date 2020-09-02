from logging import getLogger
from typing import Optional

import aiormq

from .channel import Channel
from .exchange import Exchange, ExchangeType


log = getLogger(__name__)


class RobustExchange(Exchange):
    """ Exchange abstraction """

    def __init__(
        self,
        connection,
        channel: aiormq.Channel,
        name: str,
        type: ExchangeType = ExchangeType.DIRECT,
        *,
        auto_delete: Optional[bool],
        durable: Optional[bool],
        internal: Optional[bool],
        passive: Optional[bool],
        arguments: dict = None
    ):

        super().__init__(
            connection=connection,
            channel=channel,
            name=name,
            type=type,
            auto_delete=auto_delete,
            durable=durable,
            internal=internal,
            passive=passive,
            arguments=arguments,
        )

        self._bindings = dict()

    async def restore(self, channel: Channel):
        self._channel = channel._channel

        if self.name == "":
            return

        await self.declare()

        for exchange, kwargs in self._bindings.items():
            await self.bind(exchange, **kwargs)

    async def bind(
        self,
        exchange,
        routing_key: str = "",
        *,
        arguments=None,
        timeout: int = None,
        robust: bool = True
    ):
        result = await super().bind(
            exchange,
            routing_key=routing_key,
            arguments=arguments,
            timeout=timeout,
        )

        if robust:
            self._bindings[exchange] = dict(
                routing_key=routing_key, arguments=arguments,
            )

        return result

    async def unbind(
        self,
        exchange,
        routing_key: str = "",
        arguments: dict = None,
        timeout: int = None,
    ):

        result = await super().unbind(
            exchange, routing_key, arguments=arguments, timeout=timeout,
        )
        self._bindings.pop(exchange, None)
        return result


__all__ = ("RobustExchange",)
