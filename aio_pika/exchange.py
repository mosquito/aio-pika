from typing import Optional, Union

import aiormq
from pamqp.common import Arguments

from .abc import (
    AbstractExchange, AbstractMessage, ExchangeParamType, ExchangeType,
    TimeoutType, get_exchange_name,
)
from .log import get_logger


log = get_logger(__name__)


class Exchange(AbstractExchange):
    """ Exchange abstraction """
    channel: aiormq.abc.AbstractChannel

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
        arguments: Arguments = None,
    ):
        self._type = type.value if isinstance(type, ExchangeType) else type
        self.channel = channel
        self.name = name
        self.auto_delete = auto_delete
        self.durable = durable
        self.internal = internal
        self.passive = passive
        self.arguments = arguments or {}

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return "<Exchange(%s): auto_delete=%s, durable=%s, arguments=%r)>" % (
            self,
            self.auto_delete,
            self.durable,
            self.arguments,
        )

    async def declare(
        self, timeout: TimeoutType = None,
    ) -> aiormq.spec.Exchange.DeclareOk:
        return await self.channel.exchange_declare(
            self.name,
            exchange_type=self._type,
            durable=self.durable,
            auto_delete=self.auto_delete,
            internal=self.internal,
            passive=self.passive,
            arguments=self.arguments,
            timeout=timeout,
        )

    async def bind(
        self,
        exchange: ExchangeParamType,
        routing_key: str = "",
        *,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
    ) -> aiormq.spec.Exchange.BindOk:

        """ A binding can also be a relationship between two exchanges.
        This can be simply read as: this exchange is interested in messages
        from another exchange.

        Bindings can take an extra routing_key parameter. To avoid the confusion
        with a basic_publish parameter we're going to call it a binding key.

        .. code-block:: python

            client = await connect()

            routing_key = 'simple_routing_key'
            src_exchange_name = "source_exchange"
            dest_exchange_name = "destination_exchange"

            channel = await client.channel()
            src_exchange = await channel.declare_exchange(
                src_exchange_name, auto_delete=True
            )
            dest_exchange = await channel.declare_exchange(
                dest_exchange_name, auto_delete=True
            )
            queue = await channel.declare_queue(auto_delete=True)

            await queue.bind(dest_exchange, routing_key)
            await dest_exchange.bind(src_exchange, routing_key)

        :param exchange: :class:`aio_pika.exchange.Exchange` instance
        :param routing_key: routing key
        :param arguments: additional arguments
        :param timeout: execution timeout
        :return: :class:`None`
        """

        log.debug(
            "Binding exchange %r to exchange %r, routing_key=%r, arguments=%r",
            self,
            exchange,
            routing_key,
            arguments,
        )

        return await self.channel.exchange_bind(
            arguments=arguments,
            destination=self.name,
            routing_key=routing_key,
            source=get_exchange_name(exchange),
            timeout=timeout,
        )

    async def unbind(
        self,
        exchange: ExchangeParamType,
        routing_key: str = "",
        arguments: Arguments = None,
        timeout: TimeoutType = None,
    ) -> aiormq.spec.Exchange.UnbindOk:

        """ Remove exchange-to-exchange binding for this
        :class:`Exchange` instance

        :param exchange: :class:`aio_pika.exchange.Exchange` instance
        :param routing_key: routing key
        :param arguments: additional arguments
        :param timeout: execution timeout
        :return: :class:`None`
        """

        log.debug(
            "Unbinding exchange %r from exchange %r, "
            "routing_key=%r, arguments=%r",
            self,
            exchange,
            routing_key,
            arguments,
        )

        return await self.channel.exchange_unbind(
            arguments=arguments,
            destination=self.name,
            routing_key=routing_key,
            source=get_exchange_name(exchange),
            timeout=timeout,
        )

    async def publish(
        self,
        message: AbstractMessage,
        routing_key: str,
        *,
        mandatory: bool = True,
        immediate: bool = False,
        timeout: TimeoutType = None,
    ) -> Optional[aiormq.abc.ConfirmationFrameType]:

        """ Publish the message to the queue. `aio-pika` uses
        `publisher confirms`_ extension for message delivery.

        .. _publisher confirms: https://www.rabbitmq.com/confirms.html

        """

        log.debug(
            "Publishing message with routing key %r via exchange %r: %r",
            routing_key,
            self,
            message,
        )

        if self.internal:
            # Caught on the client side to prevent channel closure
            raise ValueError(
                f"Can not publish to internal exchange: '{self.name}'!",
            )

        return await self.channel.basic_publish(
            exchange=self.name,
            routing_key=routing_key,
            body=message.body,
            properties=message.properties,
            mandatory=mandatory,
            immediate=immediate,
            timeout=timeout,
        )

    async def delete(
        self, if_unused: bool = False, timeout: TimeoutType = None,
    ) -> aiormq.spec.Exchange.DeleteOk:

        """ Delete the queue

        :param timeout: operation timeout
        :param if_unused: perform deletion when queue has no bindings.
        """

        log.info("Deleting %r", self)
        return await self.channel.exchange_delete(
            self.name, if_unused=if_unused, timeout=timeout,
        )


__all__ = ("Exchange", "ExchangeType", "ExchangeParamType")
