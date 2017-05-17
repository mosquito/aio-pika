import asyncio
from enum import Enum, unique
from logging import getLogger
from typing import Optional

from pika.channel import Channel
from .common import BaseChannel, FutureStore
from .message import Message
from .tools import create_future

log = getLogger(__name__)


@unique
class ExchangeType(Enum):
    FANOUT = 'fanout'
    DIRECT = 'direct'
    TOPIC = 'topic'
    HEADERS = 'headers'


class Exchange(BaseChannel):
    """ Exchange abstraction """

    __slots__ = 'name', '__type', '__publish_method', 'arguments', 'durable', 'auto_delete', 'internal', '_channel'

    def __init__(self, channel: Channel, publish_method, name: str,
                 type: ExchangeType=ExchangeType.DIRECT, *, auto_delete: Optional[bool],
                 durable: Optional[bool], internal: Optional[bool],
                 arguments: dict=None, loop: asyncio.AbstractEventLoop,
                 future_store: FutureStore):

        super().__init__(loop, future_store)

        if not arguments:
            arguments = {}

        self._channel = channel
        self.__publish_method = publish_method
        self.__type = type.value
        self.name = name
        self.auto_delete = auto_delete
        self.durable = durable
        self.internal = internal
        self.arguments = arguments

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<Exchange(%s): auto_delete=%s, durable=%s, arguments=%r)>" % (
            self, self.auto_delete, self.durable, self.arguments
        )

    @BaseChannel._ensure_channel_is_open
    def bind(self, exchange,
             routing_key: str='', *, arguments=None, timeout: int = None) -> asyncio.Future:

        """ A binding can also be a relationship between two exchanges. This can be
        simply read as: this exchange is interested in messages from another exchange.

        Bindings can take an extra routing_key parameter. To avoid the confusion
        with a basic_publish parameter we're going to call it a binding key.

        .. code-block:: python

            client = await connect()

            routing_key = 'simple_routing_key'
            src_exchange_name = "source_exchange"
            dest_exchange_name = "destination_exchange"

            channel = await client.channel()
            src_exchange = await channel.declare_exchange(src_exchange_name, auto_delete=True)
            dest_exchange = await channel.declare_exchange(dest_exchange_name, auto_delete=True)
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

        f = self._create_future(timeout)

        self._channel.exchange_bind(
            f.set_result,
            self.name,
            exchange.name,
            routing_key=routing_key,
            arguments=arguments
        )

        return f

    @BaseChannel._ensure_channel_is_open
    def unbind(self, exchange, routing_key: str = '',
               arguments: dict = None, timeout: int = None) -> asyncio.Future:

        """ Remove exchange-to-exchange binding for this :class:`Exchange` instance

        :param exchange: :class:`aio_pika.exchange.Exchange` instance
        :param routing_key: routing key
        :param arguments: additional arguments (will be passed to `pika`)
        :param timeout: execution timeout
        :return: :class:`None`
        """

        log.debug(
            "Unbinding exchange %r from exchange %r, routing_key=%r, arguments=%r",
            self, exchange, routing_key, arguments
        )

        f = self._create_future(timeout)

        self._channel.exchange_unbind(
            f.set_result,
            self.name,
            exchange.name,
            routing_key=routing_key,
            arguments=arguments
        )

        return f

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def publish(self, message: Message, routing_key, *, mandatory=True, immediate=False):
        """ Publish the message to the queue. `aio_pika` use `publisher confirms`_
        extension for message delivery.

        .. _publisher confirms: https://www.rabbitmq.com/confirms.html

        """

        log.debug("Publishing message via exchange %s: %r", self, message)
        if self.internal:
            # Caught on the client side to prevent channel closure
            raise ValueError("cannot publish to internal exchange: '%s'!" % self.name)

        return (
            yield from self.__publish_method(
                self.name,
                routing_key,
                message.body,
                properties=message.properties,
                mandatory=mandatory,
                immediate=immediate
            )
        )

    @BaseChannel._ensure_channel_is_open
    def delete(self, if_unused=False) -> asyncio.Future:
        """ Delete the queue

        :param if_unused: perform deletion when queue has no bindings.
        """
        log.info("Deleting %r", self)
        self._futures.reject_all(RuntimeError("Exchange was deleted"))
        future = create_future(loop=self.loop)
        self._channel.exchange_delete(future.set_result, self.name, if_unused=if_unused)
        return future


__all__ = ('Exchange',)
