import asyncio
from enum import Enum, unique
from logging import getLogger
from typing import Optional, Union

import aiormq

from .message import Message

log = getLogger(__name__)


ExchangeType_ = Union['Exchange', str]


@unique
class ExchangeType(Enum):
    FANOUT = 'fanout'
    DIRECT = 'direct'
    TOPIC = 'topic'
    HEADERS = 'headers'
    X_DELAYED_MESSAGE = 'x-delayed-message'
    X_CONSISTENT_HASH = 'x-consistent-hash'


class Exchange:
    """ Exchange abstraction """

    def __init__(self, connection, channel: aiormq.Channel, name: str,
                 type: ExchangeType=ExchangeType.DIRECT, *,
                 auto_delete: Optional[bool], durable: Optional[bool],
                 internal: Optional[bool], passive: Optional[bool],
                 arguments: dict=None):

        self.loop = connection.loop

        if not arguments:
            arguments = {}

        self._channel = channel
        self.__type = type.value
        self.name = name
        self.auto_delete = auto_delete
        self.durable = durable
        self.internal = internal
        self.passive = passive
        self.arguments = arguments

    @property
    def channel(self) -> aiormq.Channel:
        if self._channel is None:
            raise RuntimeError("Channel not opened")

        return self._channel

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<Exchange(%s): auto_delete=%s, durable=%s, arguments=%r)>" % (
            self, self.auto_delete, self.durable, self.arguments
        )

    async def declare(
        self, timeout=None
    ) -> aiormq.spec.Exchange.DeclareOk:
        return await asyncio.wait_for(self.channel.exchange_declare(
            self.name,
            exchange_type=self.__type,
            durable=self.durable,
            auto_delete=self.auto_delete,
            internal=self.internal,
            passive=self.passive,
            arguments=self.arguments,
        ), timeout=timeout, loop=self.loop)

    @staticmethod
    def _get_exchange_name(exchange: ExchangeType_):
        if isinstance(exchange, Exchange):
            return exchange.name
        elif isinstance(exchange, str):
            return exchange
        else:
            raise ValueError(
                'exchange argument must be an exchange instance or str')

    async def bind(
        self, exchange: ExchangeType_, routing_key: str='', *,
        arguments=None, timeout: int=None
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
        :param arguments: additional arguments (will be passed to `pika`)
        :param timeout: execution timeout
        :return: :class:`None`
        """

        log.debug(
            "Binding exchange %r to exchange %r, routing_key=%r, arguments=%r",
            self, exchange, routing_key, arguments
        )

        return await asyncio.wait_for(
            self.channel.exchange_bind(
                arguments=arguments,
                destination=self.name,
                routing_key=routing_key,
                source=self._get_exchange_name(exchange),
            ), timeout=timeout, loop=self.loop
        )

    async def unbind(
        self, exchange: ExchangeType_, routing_key: str = '',
        arguments: dict=None, timeout: int=None
    ) -> aiormq.spec.Exchange.UnbindOk:

        """ Remove exchange-to-exchange binding for this
        :class:`Exchange` instance

        :param exchange: :class:`aio_pika.exchange.Exchange` instance
        :param routing_key: routing key
        :param arguments: additional arguments (will be passed to `pika`)
        :param timeout: execution timeout
        :return: :class:`None`
        """

        log.debug(
            "Unbinding exchange %r from exchange %r, "
            "routing_key=%r, arguments=%r",
            self, exchange, routing_key, arguments
        )

        return await asyncio.wait_for(
            self.channel.exchange_unbind(
                arguments=arguments,
                destination=self.name,
                routing_key=routing_key,
                source=self._get_exchange_name(exchange),
            ), timeout=timeout, loop=self.loop
        )

    async def publish(
        self, message: Message, routing_key, *, mandatory=True,
        immediate=False, timeout=None
    ) -> Optional[aiormq.types.ConfirmationFrameType]:

        """ Publish the message to the queue. `aio_pika` use
        `publisher confirms`_ extension for message delivery.

        .. _publisher confirms: https://www.rabbitmq.com/confirms.html

        """

        log.debug(
            "Publishing message with routing key %r via exchange %r: %r",
            routing_key, self, message
        )

        if self.internal:
            # Caught on the client side to prevent channel closure
            raise ValueError(
                "Can not publish to internal exchange: '%s'!" % self.name
            )

        return await asyncio.wait_for(
            self.channel.basic_publish(
                exchange=self.name,
                routing_key=routing_key,
                body=message.body,
                properties=message.properties,
                mandatory=mandatory,
                immediate=immediate
            ),
            loop=self.loop, timeout=timeout
        )

    async def delete(self, if_unused=False,
                     timeout=None) -> aiormq.spec.Exchange.DeleteOk:

        """ Delete the queue

        :param timeout: operation timeout
        :param if_unused: perform deletion when queue has no bindings.
        """

        log.info("Deleting %r", self)
        return await asyncio.wait_for(
            self.channel.exchange_delete(self.name, if_unused=if_unused),
            timeout=timeout,
            loop=self.loop
        )


__all__ = ('Exchange', 'ExchangeType', 'ExchangeType_')
