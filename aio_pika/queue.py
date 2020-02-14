import asyncio
from collections import namedtuple
from functools import partial
from logging import getLogger
from typing import Optional, Callable, Any

import aiormq
from aiormq.types import DeliveredMessage

from .exceptions import QueueEmpty
from .exchange import Exchange
from .types import ExchangeType as ExchangeType_, TimeoutType
from .message import IncomingMessage
from .tools import OPERATION_TIMEOUT, create_task, shield

log = getLogger(__name__)


ConsumerTag = str
DeclarationResult = namedtuple(
    'DeclarationResult', ('message_count', 'consumer_count')
)


async def consumer(callback, msg: DeliveredMessage, *, no_ack, loop):
    message = IncomingMessage(msg, no_ack=no_ack)
    return await create_task(callback, message, loop=loop)


class Queue:
    """ AMQP queue abstraction """

    def __init__(self, connection, channel: aiormq.Channel, name,
                 durable, exclusive, auto_delete, arguments,
                 passive: bool = False):

        self.loop = connection.loop

        self._connection = connection
        self._channel = channel
        self.name = name or ''
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.arguments = arguments
        self.passive = passive
        self.declaration_result = None  # type: aiormq.spec.Queue.DeclareOk
        self._get_lock = asyncio.Lock()

    @property
    def channel(self) -> aiormq.Channel:
        if self._channel is None:
            raise RuntimeError("Channel not opened")
        return self._channel

    def _get_operation_timeout(self, timeout: TimeoutType):
        return (
            self._connection.operation_timeout if timeout is OPERATION_TIMEOUT
            else timeout
        )

    def __str__(self):
        return "%s" % self.name

    def __repr__(self):
        return (
           "<Queue(%s): "
           "auto_delete=%s, "
           "durable=%s, "
           "exclusive=%s, "
           "arguments=%r>"
        ) % (
            self,
            self.auto_delete,
            self.durable,
            self.exclusive,
            self.arguments,
        )

    async def declare(
            self, timeout: TimeoutType = OPERATION_TIMEOUT
    ) -> aiormq.spec.Queue.DeclareOk:
        """ Declare queue.

        :param timeout: execution timeout
        :param passive: Only check to see if the queue exists.
        :return: :class:`None`
        """

        log.debug("Declaring queue: %r", self)
        self.declaration_result = await asyncio.wait_for(
            self._channel.queue_declare(
                queue=self.name, durable=self.durable,
                exclusive=self.exclusive, auto_delete=self.auto_delete,
                arguments=self.arguments, passive=self.passive,
            ), timeout=self._get_operation_timeout(timeout)
        )  # type: aiormq.spec.Queue.DeclareOk

        self.name = self.declaration_result.queue
        return self.declaration_result

    async def bind(
        self, exchange: ExchangeType_, routing_key: str=None, *,
        arguments=None, timeout: TimeoutType = OPERATION_TIMEOUT
    ) -> aiormq.spec.Queue.BindOk:

        """ A binding is a relationship between an exchange and a queue.
        This can be simply read as: the queue is interested in messages
        from this exchange.

        Bindings can take an extra routing_key parameter. To avoid
        the confusion with a basic_publish parameter we're going to
        call it a binding key.

        :param exchange: :class:`aio_pika.exchange.Exchange` instance
        :param routing_key: routing key
        :param arguments: additional arguments
        :param timeout: execution timeout
        :raises asyncio.TimeoutError:
            when the binding timeout period has elapsed.
        :return: :class:`None`
        """

        if routing_key is None:
            routing_key = self.name

        log.debug(
            "Binding queue %r: exchange=%r, routing_key=%r, arguments=%r",
            self, exchange, routing_key, arguments
        )

        return await asyncio.wait_for(
            self.channel.queue_bind(
                self.name,
                exchange=Exchange._get_exchange_name(exchange),
                routing_key=routing_key,
                arguments=arguments
            ), timeout=self._get_operation_timeout(timeout)
        )

    async def unbind(
        self, exchange: ExchangeType_, routing_key: str=None,
        arguments: dict=None, timeout: TimeoutType = OPERATION_TIMEOUT
    ) -> aiormq.spec.Queue.UnbindOk:

        """ Remove binding from exchange for this :class:`Queue` instance

        :param exchange: :class:`aio_pika.exchange.Exchange` instance
        :param routing_key: routing key
        :param arguments: additional arguments
        :param timeout: execution timeout
        :raises asyncio.TimeoutError:
            when the unbinding timeout period has elapsed.
        :return: :class:`None`
        """

        if routing_key is None:
            routing_key = self.name

        log.debug(
            "Unbinding queue %r: exchange=%r, routing_key=%r, arguments=%r",
            self, exchange, routing_key, arguments
        )

        return await asyncio.wait_for(
            self.channel.queue_unbind(
                queue=self.name,
                exchange=Exchange._get_exchange_name(exchange),
                routing_key=routing_key,
                arguments=arguments
            ), timeout=self._get_operation_timeout(timeout)
        )

    async def consume(
        self, callback: Callable[[IncomingMessage], Any], no_ack: bool = False,
        exclusive: bool = False, arguments: dict = None,
        consumer_tag=None, timeout: TimeoutType = OPERATION_TIMEOUT
    ) -> ConsumerTag:

        """ Start to consuming the :class:`Queue`.

        :param timeout: :class:`asyncio.TimeoutError` will be raises when the
                        Future was not finished after this time.
        :param callback: Consuming callback. Could be a coroutine.
        :param no_ack:
            if :class:`True` you don't need to call
            :func:`aio_pika.message.IncomingMessage.ack`
        :param exclusive:
            Makes this queue exclusive. Exclusive queues may only
            be accessed by the current connection, and are deleted
            when that connection closes. Passive declaration of an
            exclusive queue by other connections are not allowed.
        :param arguments: additional arguments
        :param consumer_tag: optional consumer tag

        :raises asyncio.TimeoutError:
            when the consuming timeout period has elapsed.
        :return str: consumer tag :class:`str`

        """

        log.debug("Start to consuming queue: %r", self)

        return (await asyncio.wait_for(
            self.channel.basic_consume(
                queue=self.name,
                consumer_callback=partial(
                    consumer, callback, no_ack=no_ack, loop=self.loop
                ),
                exclusive=exclusive,
                no_ack=no_ack,
                arguments=arguments,
                consumer_tag=consumer_tag,
            ),
            timeout=self._get_operation_timeout(timeout)
        )).consumer_tag

    async def cancel(
        self, consumer_tag: ConsumerTag,
        timeout: TimeoutType = OPERATION_TIMEOUT, nowait: bool=False
    ) -> aiormq.spec.Basic.CancelOk:
        """ This method cancels a consumer. This does not affect already
        delivered messages, but it does mean the server will not send any more
        messages for that consumer. The client may receive an arbitrary number
        of messages in between sending the cancel method and receiving the
        cancel-ok reply. It may also be sent from the server to the client in
        the event of the consumer being unexpectedly cancelled (i.e. cancelled
        for any reason other than the server receiving the corresponding
        basic.cancel from the client). This allows clients to be notified of
        the loss of consumers due to events such as queue deletion.

        :param consumer_tag:
            consumer tag returned by :func:`~aio_pika.Queue.consume`
        :param timeout: execution timeout
        :param bool nowait: Do not expect a Basic.CancelOk response
        :return: Basic.CancelOk when operation completed successfully
        """

        return await asyncio.wait_for(
            self.channel.basic_cancel(
                consumer_tag=consumer_tag,
                nowait=nowait
            ),
            timeout=self._get_operation_timeout(timeout)
        )

    async def get(
        self, *, no_ack=False, fail=True,
        timeout: TimeoutType = OPERATION_TIMEOUT
    ) -> Optional[IncomingMessage]:

        """ Get message from the queue.

        :param no_ack: if :class:`True` you don't need to call
                       :func:`aio_pika.message.IncomingMessage.ack`
        :param timeout: execution timeout
        :param fail: Should return :class:`None` instead of raise an
                     exception :class:`aio_pika.exceptions.QueueEmpty`.
        :return: :class:`aio_pika.message.IncomingMessage`
        """

        msg = await asyncio.wait_for(self.channel.basic_get(
                self.name, no_ack=no_ack
            ), timeout=self._get_operation_timeout(timeout)
        )   # type: Optional[DeliveredMessage]

        if msg is None:
            if fail:
                raise QueueEmpty
            return

        return IncomingMessage(msg, no_ack=no_ack)

    async def purge(
        self, no_wait=False, timeout: TimeoutType = OPERATION_TIMEOUT
    ) -> aiormq.spec.Queue.PurgeOk:
        """ Purge all messages from the queue.

        :param no_wait: no wait response
        :param timeout: execution timeout
        :return: :class:`None`
        """

        log.info("Purging queue: %r", self)

        return await asyncio.wait_for(
            self.channel.queue_purge(
                self.name,
                nowait=no_wait,
            ), timeout=self._get_operation_timeout(timeout)
        )

    async def delete(
        self, *, if_unused=True, if_empty=True,
        timeout: TimeoutType = OPERATION_TIMEOUT
    ) -> aiormq.spec.Queue.DeclareOk:

        """ Delete the queue.

        :param if_unused: Perform delete only when unused
        :param if_empty: Perform delete only when empty
        :param timeout: execution timeout
        :return: :class:`None`
        """

        log.info("Deleting %r", self)

        return await asyncio.wait_for(
            self.channel.queue_delete(
                self.name, if_unused=if_unused, if_empty=if_empty
            ), timeout=self._get_operation_timeout(timeout)
        )

    def __aiter__(self) -> 'QueueIterator':
        return self.iterator()

    def iterator(self, **kwargs) -> 'QueueIterator':
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

        return QueueIterator(self, **kwargs)


class QueueIterator:
    @shield
    async def close(self):
        if not self._consumer_tag:
            return

        await self._amqp_queue.cancel(self._consumer_tag)
        self._consumer_tag = None

        def get_msg():
            try:
                return self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return

        # Reject all messages
        msg = get_msg()  # type: IncomingMessage
        while msg and not self._amqp_queue.channel.closing.done():
            await msg.reject(requeue=True)
            msg = get_msg()  # type: IncomingMessage

    def __str__(self):
        return 'queue[%s](...)' % self._amqp_queue.name

    def __init__(self, queue: Queue, **kwargs):
        self.loop = queue.loop
        self._amqp_queue = queue
        self._queue = asyncio.Queue()
        self._consumer_tag = None
        self._consume_kwargs = kwargs

    async def on_message(self, message: IncomingMessage):
        await self._queue.put(message)

    async def consume(self):
        self._consumer_tag = await self._amqp_queue.consume(
            self.on_message,
            **self._consume_kwargs
        )

    def __aiter__(self):
        return self

    @shield
    async def __aenter__(self):
        if self._consumer_tag is None:
            await self.consume()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def __anext__(self) -> IncomingMessage:
        if not self._consumer_tag:
            await self.consume()
        try:
            return await self._queue.get()
        except asyncio.CancelledError:
            await self.close()
            raise


__all__ = 'Queue', 'QueueIterator', 'DeclarationResult', 'ConsumerTag'
