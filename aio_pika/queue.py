import asyncio
from collections import namedtuple
from logging import getLogger
from types import FunctionType
from typing import Any, Generator, Optional

from .adapter import Channel
from .exchange import Exchange, ExchangeType_
from .message import IncomingMessage
from .common import BaseChannel, FutureStore
from .tools import create_task, iscoroutinepartial
from .exceptions import QueueEmpty

log = getLogger(__name__)


ConsumerTag = str
DeclarationResult = namedtuple('DeclarationResult', ('message_count', 'consumer_count'))


class Queue(BaseChannel):
    """ AMQP queue abstraction """

    __slots__ = ('name', 'durable', 'exclusive',
                 'auto_delete', 'arguments', '_get_lock',
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
        self._get_lock = asyncio.Lock(loop=self.loop)

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
    def bind(self, exchange: ExchangeType_, routing_key: str=None, *,
             arguments=None, timeout: int = None) -> asyncio.Future:

        """ A binding is a relationship between an exchange and a queue. This can be
        simply read as: the queue is interested in messages from this exchange.

        Bindings can take an extra routing_key parameter. To avoid the confusion
        with a basic_publish parameter we're going to call it a binding key.

        :param exchange: :class:`aio_pika.exchange.Exchange` instance
        :param routing_key: routing key
        :param arguments: additional arguments (will be passed to `pika`)
        :param timeout: execution timeout
        :raises asyncio.TimeoutError: when the binding timeout period has elapsed.
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
            Exchange._get_exchange_name(exchange),
            routing_key=routing_key,
            arguments=arguments
        )

        return f

    @BaseChannel._ensure_channel_is_open
    def unbind(self, exchange: ExchangeType_, routing_key: str,
               arguments: dict = None, timeout: int = None) -> asyncio.Future:

        """ Remove binding from exchange for this :class:`Queue` instance

        :param exchange: :class:`aio_pika.exchange.Exchange` instance
        :param routing_key: routing key
        :param arguments: additional arguments (will be passed to `pika`)
        :param timeout: execution timeout
        :raises asyncio.TimeoutError: when the unbinding timeout period has elapsed.
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
            Exchange._get_exchange_name(exchange),
            routing_key=routing_key,
            arguments=arguments
        )

        return f

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def consume(self, callback: FunctionType, no_ack: bool = False,
                exclusive: bool = False, arguments: dict = None,
                consumer_tag=None, timeout=None) -> Generator[Any, None, ConsumerTag]:
        """ Start to consuming the :class:`Queue`.

        :param timeout: :class:`asyncio.TimeoutError` will be raises when the
                        Future was not finished after this time.
        :param callback: Consuming callback. Could be a coroutine.
        :param no_ack: if :class:`True` you don't need to call :func:`aio_pika.message.IncomingMessage.ack`
        :param exclusive: Makes this queue exclusive. Exclusive queues may only be accessed by the current
                          connection, and are deleted when that connection closes. Passive declaration of an
                          exclusive queue by other connections are not allowed.
        :param arguments: extended arguments for pika
        :param consumer_tag: optional consumer tag

        :raises asyncio.TimeoutError: when the consuming timeout period has elapsed.
        :return str: consumer tag :class:`str`

        """

        log.debug("Start to consuming queue: %r", self)
        future = self._futures.create_future(timeout=timeout)

        def consumer(channel: Channel, envelope, properties, body: bytes):
            message = IncomingMessage(
                channel=channel,
                body=body,
                envelope=envelope,
                properties=properties,
                no_ack=no_ack,
            )

            if iscoroutinepartial(callback):
                create_task(loop=self.loop)(callback(message))
            else:
                self.loop.call_soon(callback, message)

        consumer_tag = self._channel.basic_consume(
            consumer_callback=consumer,
            queue=self.name,
            no_ack=no_ack,
            exclusive=exclusive,
            arguments=arguments,
            consumer_tag=consumer_tag,
            result_callback=future.set_result,
        )

        yield from future

        return consumer_tag

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
    def get(self, *, no_ack=False, timeout=None, fail=True) -> Generator[Any, None, Optional[IncomingMessage]]:

        """ Get message from the queue.

        :param no_ack: if :class:`True` you don't need to call
                       :func:`aio_pika.message.IncomingMessage.ack`
        :param timeout: execution timeout
        :param fail: Should return :class:`None` instead of raise an
                     exception :class:`aio_pika.exceptions.QueueEmpty`.
        :return: :class:`aio_pika.message.IncomingMessage`
        """

        f = self._create_future(timeout)

        def _on_getempty(method_frame, *a, **kw):
            if fail:
                f.set_exception(QueueEmpty(method_frame))
            else:
                f.set_result(None)

        def _on_getok(channel, envelope, props, body):
            message = IncomingMessage(
                channel,
                envelope,
                props,
                body,
                no_ack=no_ack,
            )

            f.set_result(message)

        with (yield from self._get_lock), self._channel.set_get_empty_callback(_on_getempty):
            log.debug("Awaiting message from queue: %r", self)

            self._channel.basic_get(_on_getok, self.name, no_ack=no_ack)

            try:
                message = yield from f
                return message
            finally:
                self._channel._on_getempty = None

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

    def __iter__(self) -> 'QueueIterator':
        """ Return the :class:`QueueIterator` which might be used with `async for` syntax
        before use it we are strongly recommended call :method:`set_qos` with argument `1`. """
        iterator = self.iterator()
        self.loop.create_task(iterator.consume())
        return iterator

    @asyncio.coroutine
    def __aiter__(self) -> 'QueueIterator':
        iterator = self.iterator()
        yield from iterator.consume()
        return iterator

    def iterator(self) -> 'QueueIterator':
        """ Returns an iterator for async for expression.

        Full example:

        .. code-block:: python

            import aio_pika

            async def main():
                connection = await aio_pika.connect()

                async with connection:
                    channel = await connection.channel()

                    queue = await channel.declare_queue('test')

                    async with queue.iterator() as q:
                        async for message in q:
                            print(message.body)

        When your program runs with run_forever the iterator will be closed
        in background. In this case the context processor for iterator might
        be skipped and the queue might be used in the "async for"
        expression directly.

        .. code-block:: python

            import aio_pika

            async def main():
                connection = await aio_pika.connect()

                async with connection:
                    channel = await connection.channel()

                    queue = await channel.declare_queue('test')

                    async for message in queue:
                        print(message.body)

        :return: QueueIterator
        """

        return QueueIterator(self)


class QueueIterator:
    def __init__(self, queue: Queue):
        self._amqp_queue = queue
        self._queue = asyncio.Queue(loop=self.loop)
        self._consumer_tag = None

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._amqp_queue.loop

    def on_message(self, message: IncomingMessage):
        self._queue.put_nowait(message)

    @asyncio.coroutine
    def consume(self):
        self._consumer_tag = yield from self._amqp_queue.consume(self.on_message)

    @asyncio.coroutine
    def _close(self):
        yield from self._amqp_queue.cancel(self._consumer_tag)
        self._consumer_tag = None

        def get_msg():
            try:
                return self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return

        # Reject all messages
        msg = get_msg()     # type: IncomingMessage
        while msg:
            msg.reject(requeue=True)
            msg = get_msg()  # type: IncomingMessage

    def close(self) -> asyncio.Future:
        if not self._consumer_tag or self._amqp_queue._channel.is_closed:
            f = asyncio.Future(loop=self.loop)
            f.set_result(None)
            return f

        return self.loop.create_task(self._close())

    def __del__(self):
        self.close()

    def __iter__(self):
        return self

    __aiter__ = asyncio.coroutine(__iter__)

    @asyncio.coroutine
    def __next__(self) -> IncomingMessage:
        try:
            return (yield from self._queue.get())
        except asyncio.CancelledError:
            yield from self.close()
            raise

    @asyncio.coroutine
    def __aenter__(self):
        if self._consumer_tag is None:
            yield from self.consume()
        return self

    @asyncio.coroutine
    def __aexit__(self, exc_type, exc_val, exc_tb):
        yield from self.close()

    __anext__ = __next__


__all__ = 'Queue', 'QueueIterator'
