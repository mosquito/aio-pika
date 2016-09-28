import asyncio
from logging import getLogger
from types import FunctionType
from pika.channel import Channel
from .exchange import Exchange
from .message import IncomingMessage
from .common import BaseChannel, FutureStore

log = getLogger(__name__)


class Queue(BaseChannel):
    __slots__ = ('name', 'durable', 'exclusive',
                 'auto_delete', 'arguments',
                 '_channel', '__closing')

    def __init__(self, loop: asyncio.AbstractEventLoop, future_store: FutureStore,
                 channel: Channel, name, durable, exclusive, auto_delete, arguments):

        super().__init__(loop, future_store)

        self._channel = channel
        self.name = name
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments

    def __str__(self):
        return "%s" % self.name

    def __repr__(self):
        return "<Queue(%s): auto_delete=%s, durable=%s, exclusive=%s, arguments=%r>" % (
            self, self.auto_delete, self.durable,
            self.exclusive, self.arguments,
        )

    @BaseChannel._ensure_channel_is_open
    def declare(self, timeout: int = None) -> asyncio.Future:
        log.debug("Declaring queue: %r", self)

        f = self._create_future(timeout)

        self._channel.queue_declare(
            f.set_result,
            self.name, durable=self.durable,
            auto_delete=self.auto_delete, arguments=self.arguments,
            exclusive=self.exclusive
        )

        return f

    @BaseChannel._ensure_channel_is_open
    def bind(self, exchange: Exchange,
             routing_key: str, *, arguments=None, timeout: int = None) -> asyncio.Future:

        log.debug(
            "Binding queue %r: exchange=%r, routing_key=%r, arguments=%r",
            self, exchange, routing_key, arguments
        )

        f = self._create_future(timeout)

        self._channel.queue_bind(
            f.set_result,
            self.name,
            exchange.name,
            routing_key=routing_key,
            arguments=arguments
        )

        return f

    @BaseChannel._ensure_channel_is_open
    def unbind(self, exchange: Exchange, routing_key: str,
               arguments: dict = None, timeout: int = None) -> asyncio.Future:

        log.debug(
            "Unbinding queue %r: exchange=%r, routing_key=%r, arguments=%r",
            self, exchange, routing_key, arguments
        )

        f = self._create_future(timeout)

        self._channel.queue_unbind(
            f.set_result,
            self.name,
            exchange.name,
            routing_key=routing_key,
            arguments=arguments
        )

        return f

    @BaseChannel._ensure_channel_is_open
    def consume(self, callback: FunctionType,
                no_ack: bool = False, exclusive: bool = False, arguments: dict = None):

        log.debug("Start to consuming queue: %r", self)

        def consumer(channel: Channel, envelope, properties, body: bytes):
            message = IncomingMessage(
                channel=channel,
                body=body,
                envelope=envelope,
                properties=properties,
                no_ack=no_ack
            )

            self.loop.create_task(asyncio.coroutine(callback)(message))

        self._channel.basic_consume(
            consumer_callback=consumer,
            queue=self.name,
            no_ack=no_ack,
            exclusive=exclusive,
            arguments=arguments
        )

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def get(self, *, no_ack=False, timeout=None) -> IncomingMessage:

        log.debug("Awaiting message from queue: %r", self)

        f = self._create_future(timeout)

        self._channel.basic_get(
            lambda *a: f.set_result(a),
            self.name,
            no_ack=no_ack,
        )

        channel, envelope, props, body = yield from f

        return IncomingMessage(
            channel,
            envelope,
            props,
            body,
            no_ack=no_ack,
        )

    @BaseChannel._ensure_channel_is_open
    def purge(self, timeout=None) -> asyncio.Future:

        log.warning("Purging queue: %r", self)

        f = self._create_future(timeout)
        self._channel.queue_purge(f.set_result, self.name)
        return f

    @BaseChannel._ensure_channel_is_open
    def delete(self, *, if_unused=True, if_empty=True, timeout=None) -> asyncio.Future:
        log.warning("Deleting %r", self)

        self._futures.reject_all(RuntimeError("Queue was deleted"))

        future = self._create_future(timeout)

        self._channel.queue_delete(
            future.set_result,
            self.name,
            if_unused=if_unused,
            if_empty=if_empty
        )

        return future


__all__ = ('Queue',)
