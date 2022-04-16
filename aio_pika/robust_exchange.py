from logging import getLogger
from typing import Any, Dict, Union

import aiormq
from pamqp.common import Arguments

from .abc import (
    AbstractExchange, AbstractRobustExchange, ExchangeParamType, TimeoutType,
)
from .exchange import Exchange, ExchangeType
from .tools import RLock

log = getLogger(__name__)


class RobustExchange(Exchange, AbstractRobustExchange):
    """ Exchange abstraction """

    _bindings: Dict[Union[AbstractExchange, str], Dict[str, Any]]

    def __init__(
        self,
        channel: aiormq.abc.AbstractChannel,
        name: str,
        type: Union[ExchangeType, str] = ExchangeType.DIRECT,
        *,
        auto_delete: bool = False,
        durable: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: Arguments = None
    ):
        super().__init__(
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
        self._operation_lock = RLock()

    async def restore(self, channel: aiormq.abc.AbstractChannel) -> None:
        async with self._operation_lock:
            self.channel = channel

            if self.name == "":
                return

            await self.declare()

            for exchange, kwargs in tuple(self._bindings.items()):
                await self.bind(exchange, **kwargs)

    async def bind(
        self,
        exchange: ExchangeParamType,
        routing_key: str = "",
        *,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
        robust: bool = True
    ) -> aiormq.spec.Exchange.BindOk:
        result = await super().bind(
            exchange,
            routing_key=routing_key,
            arguments=arguments,
            timeout=timeout,
        )

        if robust:
            self._bindings[exchange] = dict(
                routing_key=routing_key,
                arguments=arguments,
            )

        return result

    async def unbind(
        self,
        exchange: ExchangeParamType,
        routing_key: str = "",
        arguments: Arguments = None,
        timeout: TimeoutType = None,
    ) -> aiormq.spec.Exchange.UnbindOk:
        result = await super().unbind(
            exchange, routing_key, arguments=arguments, timeout=timeout,
        )
        self._bindings.pop(exchange, None)
        return result


__all__ = ("RobustExchange",)
