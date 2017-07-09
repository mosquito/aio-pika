import asyncio
from collections import namedtuple
from logging import getLogger
from types import FunctionType
from pika.channel import Channel
from .exchange import Exchange
from .message import IncomingMessage
from .common import BaseChannel, FutureStore
from .tools import create_task, iscoroutinepartial

log = getLogger(__name__)


ConsumerTag = str
DeclarationResult = namedtuple('DeclarationResult', ('message_count', 'consumer_count'))


class Queue(BaseChannel):
    """ AMQP queue abstraction """

    __slots__ = ('name', 'durable', 'exclusive',
                 'auto_delete', 'arguments',
                 '_channel', '__closing', 'declaration_result')

    def __init__(self, loop: asyncio.AbstractEventLoop, future_store: FutureStore,
                 channel: Channel, name, durable, exclusive, auto_delete, arguments):

        super().__init__(loop, future_store)

        self._channel = channel
        self.name = name or ''
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments
        self.declaration_result = None      # type: DeclarationResult

    def __str__(self):
        return "%s" % self.name

    def __repr__(self):
        return "<Queue(%s): auto_delete=%s, durable=%s, exclusive=%s, arguments=%r>" % (
            self, self.auto_delete, self.durable,
            self.exclusive, self.arguments,
        )

    @BaseChannel._ensure_channel_is_open
    def declare(self, timeout: int = None, passive: bool = False) -> asyncio.Future:
        """ Declare queue.

        :param timeout: execution timeout
        :param passive: Only check to see if the queue exists.
        :return: :class:`None`
        """

        log.debug("Declaring queue: %r", self)

        f = self._create_future(timeout)

        self._channel.queue_declare(
            f.set_result,
            self.name, durable=self.durable,
            auto_delete=self.auto_delete, passive=passive,
            arguments=self.arguments,
            exclusive=self.exclusive
        )

        def on_queue_declared(result):
            res = result.result()
            self.name = res.method.queue
            self.declaration_result = DeclarationResult(
                message_count=res.method.message_count,
                consumer_count=res.method.consumer_count,
            )

        f.add_done_callback(on_queue_declared)

        return f

    @BaseChannel._ensure_channel_is_open
    def bind(self, exchange: Exchange,
             routing_key: str=None, *, arguments=None, timeout: int = None) -> asyncio.Future:

        """ A binding is a relationship between an exchange and a queue. This can be
        simply read as: the queue is interested in messages from this exchange.

        Bindings can take an extra routing_key parameter. To avoid the confusion
        with a basic_publish parameter we're going to call it a binding key.

        :param exchange: :class:`aio_pika.exchange.Exchange` instance
        :param routing_key: routing key
        :param arguments: additional arguments (will be passed to `pika`)
        :param timeout: execution timeout
        :return: :class:`None`
        """

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

        """ Remove binding from exchange for this :class:`Queue` instance

        :param exchange: :class:`aio_pika.exchange.Exchange` instance
        :param routing_key: routing key
        :param arguments: additional arguments (will be passed to `pika`)
        :param timeout: execution timeout
        :return: :class:`None`
        """

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
                no_ack: bool = False, exclusive: bool = False, arguments: dict = None) -> ConsumerTag:
        """ Start to consuming the :class:`Queue`.

        :param callback: Consuming callback
        :param no_ack: if :class:`True` you don't need to call :func:`aio_pika.message.IncomingMessage.ack`
        :param exclusive: Makes this queue exclusive. Exclusive queues may only be accessed by the current
                          connection, and are deleted when that connection closes. Passive declaration of an
                          exclusive queue by other connections are not allowed.
        :param arguments: extended arguments for pika
        :return: consumer tag :class:`str`

        """

        log.debug("Start to consuming queue: %r", self)

        def consumer(channel: Channel, envelope, properties, body: bytes):
            message = IncomingMessage(
                channel=channel,
                body=body,
                envelope=envelope,
                properties=properties,
                no_ack=no_ack
            )

            if iscoroutinepartial(callback):
                create_task(loop=self.loop)(callback(message))
            else:
                self.loop.call_soon(callback, message)

        return self._channel.basic_consume(
            consumer_callback=consumer,
            queue=self.name,
            no_ack=no_ack,
            exclusive=exclusive,
            arguments=arguments
        )

    @BaseChannel._ensure_channel_is_open
    def cancel(self, consumer_tag: ConsumerTag, timeout=None, nowait: bool=False):
        """ This method cancels a consumer. This does not affect already
        delivered messages, but it does mean the server will not send any more
        messages for that consumer. The client may receive an arbitrary number
        of messages in between sending the cancel method and receiving the
        cancel-ok reply. It may also be sent from the server to the client in
        the event of the consumer being unexpectedly cancelled (i.e. cancelled
        for any reason other than the server receiving the corresponding
        basic.cancel from the client). This allows clients to be notified of
        the loss of consumers due to events such as queue deletion.

        :param consumer_tag: consumer tag returned by :func:`~aio_pika.Queue.consume`
        :param timeout: execution timeout
        :param bool nowait: Do not expect a Basic.CancelOk response
        :return: Basic.CancelOk when operation completed successfully
        """
        f = self._create_future(timeout)
        self._channel.basic_cancel(
            None if nowait else f.set_result,
            consumer_tag=consumer_tag,
            nowait=nowait
        )

        if nowait:
            f.set_result(None)

        return f

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def get(self, *, no_ack=False, timeout=None) -> IncomingMessage:

        """ Get message from the queue.

        :param no_ack: if :class:`True` you don't need to call :func:`aio_pika.message.IncomingMessage.ack`
        :param timeout: execution timeout
        :return: :class:`aio_pika.message.IncomingMessage`
        """

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
        """ Purge all messages from the queue.

        :param timeout: execution timeout
        :return: :class:`None`
        """

        log.info("Purging queue: %r", self)

        f = self._create_future(timeout)
        self._channel.queue_purge(f.set_result, self.name)
        return f

    @BaseChannel._ensure_channel_is_open
    def delete(self, *, if_unused=True, if_empty=True, timeout=None) -> asyncio.Future:
        """ Delete the queue.

        :param if_unused: Perform delete only when unused
        :param if_empty: Perform delete only when empty
        :param timeout: execution timeout
        :return: :class:`None`
        """

        log.info("Deleting %r", self)

        self._futures.reject_all(RuntimeError("Queue was deleted"))

        future = self._create_future(timeout)

        self._channel.queue_delete(
            future.set_result,
            self.name,
            if_unused=if_unused,
            if_empty=if_empty
        )

        return future


__all__ = 'Queue',
