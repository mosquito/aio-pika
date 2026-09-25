from typing import Optional, Union, cast

import aiormq
from pamqp.common import Arguments

from .abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractMessage,
    ExchangeParamType,
    ExchangeType,
    PublishResultType,
    TimeoutType,
    get_exchange_name,
)
from .log import get_logger


log = get_logger(__name__)


class Exchange(AbstractExchange):
    """Exchange abstraction"""

    channel: AbstractChannel

    def __init__(
        self,
        channel: AbstractChannel,
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
        return (
            f"<{self.__class__.__name__}({self}):"
            f" auto_delete={self.auto_delete},"
            f" durable={self.durable},"
            f" arguments={self.arguments!r})>"
        )

    async def declare(
        self,
        timeout: TimeoutType = None,
    ) -> aiormq.spec.Exchange.DeclareOk:
        channel = await self.channel.get_underlay_channel()
        return await channel.exchange_declare(
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
        """A binding can also be a relationship between two exchanges.
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

        channel = await self.channel.get_underlay_channel()
        return await channel.exchange_bind(
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
        """Remove exchange-to-exchange binding for this
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

        channel = await self.channel.get_underlay_channel()
        return await channel.exchange_unbind(
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
    ) -> Optional[PublishResultType]:
        """Publish the message to the exchange. `aio-pika` uses the
        `publisher confirms`_ extension for message delivery.

        The result depends on how the channel was opened:

        * ``publisher_confirms=False``: returns ``None`` as soon as the
          message is written to the socket. The broker does not confirm it.
        * ``publisher_confirms=True`` (default): waits for the broker and
          returns the ``Basic.Ack`` frame. When the broker answers with
          ``Basic.Nack`` or ``Basic.Reject``, raises
          :class:`aio_pika.exceptions.DeliveryError`.
        * ``mandatory=True`` and the broker can not route the message: with
          ``on_return_raises=True`` raises
          :class:`aio_pika.exceptions.PublishError`. Otherwise calls
          ``channel.return_callbacks`` and returns the returned
          :class:`aiormq.abc.DeliveredMessage`.

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

        if self.channel.is_closed:
            raise aiormq.exceptions.ChannelInvalidStateError(
                "%r closed" % self.channel,
            )

        channel = await self.channel.get_underlay_channel()
        result = await channel.basic_publish(
            exchange=self.name,
            routing_key=routing_key,
            body=message.body,
            properties=message.properties,
            mandatory=mandatory,
            immediate=immediate,
            timeout=timeout,
        )
        # aiormq annotates the result with all confirmation frames, but
        # Basic.Nack and Basic.Reject are raised as DeliveryError, and a
        # Basic.Return yields the DeliveredMessage instead of a frame.
        return cast(Optional[PublishResultType], result)

    async def delete(
        self,
        if_unused: bool = False,
        timeout: TimeoutType = None,
    ) -> aiormq.spec.Exchange.DeleteOk:
        """Delete the queue

        :param timeout: operation timeout
        :param if_unused: perform deletion when queue has no bindings.
        """

        log.info("Deleting %r", self)
        channel = await self.channel.get_underlay_channel()
        result = await channel.exchange_delete(
            self.name,
            if_unused=if_unused,
            timeout=timeout,
        )
        del self.channel
        return result


__all__ = ("Exchange", "ExchangeType", "ExchangeParamType")
