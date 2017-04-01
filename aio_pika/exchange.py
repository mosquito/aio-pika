import asyncio
from enum import Enum, unique
from logging import getLogger
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

    __slots__ = 'name', '__type', '__publish_method', 'arguments', 'durable', 'auto_delete', '_channel'

    def __init__(self, channel: Channel, publish_method, name: str,
                 type: ExchangeType=ExchangeType.DIRECT, *, auto_delete: bool,
                 durable: bool, arguments: dict, loop: asyncio.AbstractEventLoop, future_store: FutureStore):

        super().__init__(loop, future_store)

        self._channel = channel
        self.__publish_method = publish_method
        self.__type = type.value
        self.name = name
        self.auto_delete = auto_delete
        self.durable = durable
        self.arguments = arguments

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<Exchange(%s): auto_delete=%s, durable=%s, arguments=%r)>" % (
            self, self.auto_delete, self.durable, self.arguments
        )

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def publish(self, message: Message, routing_key, *, mandatory=True, immediate=False):
        """ Publish the message to the queue. `aio_pika` use `publisher confirms`_
        extension for message delivery.

        .. _publisher confirms: https://www.rabbitmq.com/confirms.html

        """

        log.debug("Publishing message via exchange %s: %r", self, message)

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
        log.warning("Deleting %r", self)
        self._futures.reject_all(RuntimeError("Exchange was deleted"))
        future = create_future(loop=self.loop)
        self._channel.exchange_delete(future.set_result, self.name, if_unused=if_unused)
        return future


__all__ = ('Exchange',)
