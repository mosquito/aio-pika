import asyncio
from typing import Callable, Any, Union

import pika.channel
from logging import getLogger
from types import FunctionType

from . import exceptions
from .compat import Awaitable
from .exchange import Exchange, ExchangeType
from .message import IncomingMessage, ReturnedMessage
from .queue import Queue
from .common import BaseChannel, FutureStore, ConfirmationTypes


log = getLogger(__name__)

FunctionOrCoroutine = Union[Callable[[IncomingMessage], Any], Awaitable[IncomingMessage]]


class Channel(BaseChannel):
    """ Channel abstraction """

    __slots__ = ('__connection', '__closing', '__confirmations', '__delivery_tag',
                 'loop', '_futures', '__channel', '__on_return_callbacks',
                 'default_exchange', '__write_lock')

    def __init__(self, connection,
                 loop: asyncio.AbstractEventLoop, future_store: FutureStore):
        """

        :param connection: :class:`aio_pika.adapter.AsyncioConnection` instance
        :param loop: Event loop (:func:`asyncio.get_event_loop()` when :class:`None`)
        :param future_store: :class:`aio_pika.common.FutureStore` instance
        """
        super().__init__(loop, future_store.get_child())

        self.__channel = None  # type: pika.channel.Channel
        self.__connection = connection
        self.__confirmations = {}
        self.__on_return_callbacks = set()
        self.__delivery_tag = 0
        self.__write_lock = asyncio.Lock(loop=self.loop)

        self.default_exchange = Exchange(
            self.__channel,
            self._publish,
            '',
            ExchangeType.DIRECT,
            durable=None,
            auto_delete=None,
            internal=None,
            arguments=None,
            loop=self.loop,
            future_store=self._futures.get_child(),
        )

    def __str__(self):
        return "{0}".format(self.__channel.channel_number if self.__channel else "Not initialized chanel")

    def __repr__(self):
        return '<Channel "%s#%s">' % (self.__connection, self)

    def _on_channel_close(self, channel: pika.channel.Channel, code: int, reason):
        exc = exceptions.ChannelClosed(code, reason)
        log.error("Channel %r closed: %d - %s", channel, code, reason)

        self._futures.reject_all(exc)

    def _on_return(self, channel, message, properties, body):
        msg = ReturnedMessage(channel=channel, body=body, envelope=message, properties=properties)

        for cb in self.__on_return_callbacks:
            try:
                result = cb(msg)
                if asyncio.iscoroutine(result):
                    self.loop.create_task(result)
            except:
                log.exception("Error when handling callback: %r", cb)

    def add_close_callback(self, callback: FunctionType) -> None:
        self._closing.add_done_callback(lambda r: callback(r))

    def add_on_return_callback(self, callback: FunctionOrCoroutine) -> None:
        self.__on_return_callbacks.add(callback)

    @asyncio.coroutine
    def initialize(self, timeout=None) -> None:
        with (yield from self.__write_lock):
            if self._closing.done():
                raise RuntimeError("Can't initialize closed channel")

            future = self._create_future(timeout=timeout)

            self.__connection._connection.channel(future.set_result)

            channel = yield from future  # type: pika.channel.Channel
            channel.confirm_delivery(self._on_delivery_confirmation)
            channel.add_on_close_callback(self._on_channel_close)
            channel.add_on_return_callback(self._on_return)

            self.__channel = channel

    def _on_delivery_confirmation(self, method_frame):
        future = self.__confirmations.pop(method_frame.method.delivery_tag, None)

        if not future:
            log.info(
                "Unknown delivery tag %d for message confirmation \"%s\"",
                method_frame.method.delivery_tag, method_frame.method.NAME)
            return

        try:
            confirmation_type = ConfirmationTypes(method_frame.method.NAME.split('.')[1].lower())

            if confirmation_type == ConfirmationTypes.ACK:
                future.set_result(True)
            elif confirmation_type == ConfirmationTypes.NACK:
                future.set_exception(exceptions.NackError(method_frame))
        except ValueError:
            future.set_exception(RuntimeError('Unknown method frame', method_frame))
        except Exception as e:
            future.set_exception(e)

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def declare_exchange(self, name: str, type: ExchangeType = ExchangeType.DIRECT,
                         durable: bool = None, auto_delete: bool = False,
                         internal: bool = False, arguments: dict = None, timeout: int = None) -> Exchange:

        with (yield from self.__write_lock):
            if auto_delete and durable is None:
                durable = False

            f = self._create_future(timeout=timeout)

            self.__channel.exchange_declare(
                f.set_result,
                name, ExchangeType(type).value, durable=durable,
                auto_delete=auto_delete, internal=internal, arguments=arguments
            )

            yield from f

            exchange = Exchange(
                self.__channel, self._publish, name, type,
                durable=durable, auto_delete=auto_delete, internal=internal,
                arguments=arguments, loop=self.loop, future_store=self._futures.get_child(),
            )

            log.debug("Exchange declared %r", exchange)

            return exchange

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def _publish(self, queue_name, routing_key, body, properties, mandatory, immediate):
        with (yield from self.__write_lock):
            while self.__connection.is_closed:
                log.debug("Can't publish message because connection is inactive")
                yield from asyncio.sleep(1, loop=self.loop)

            f = self._create_future()

            try:
                self.__channel.basic_publish(queue_name, routing_key, body, properties, mandatory, immediate)
            except (AttributeError, RuntimeError) as exc:
                log.exception("Failed to send data to client. Conection unexpected closed.")
                self._on_channel_close(self.__channel, -1, exc)
                self.__connection.close(reply_code=500, reply_text="Incorrect state")
            else:
                self.__delivery_tag += 1
                self.__confirmations[self.__delivery_tag] = f

            return (yield from f)

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def declare_queue(self, name: str = None, *, durable: bool = None, exclusive: bool = False, passive: bool = False,
                      auto_delete: bool = False, arguments: dict = None, timeout: int = None) -> Queue:
        """

        :param name: queue name
        :param durable: Durability (queue survive broker restart)
        :param exclusive: Makes this queue exclusive. Exclusive queues may only be \
        accessed by the current connection, and are deleted when that connection \
        closes. Passive declaration of an exclusive queue by other connections are not allowed.
        :param passive: Only check to see if the queue exists.
        :param auto_delete: Delete queue when channel will be closed.
        :param arguments: pika additional arguments
        :param timeout: execution timeout
        :return: :class:`aio_pika.queue.Queue` instance
        """

        with (yield from self.__write_lock):
            if auto_delete and durable is None:
                durable = False

            queue = Queue(
                self.loop, self._futures.get_child(), self.__channel, name,
                durable, exclusive, auto_delete, arguments
            )

            yield from queue.declare(timeout, passive=passive)
            return queue

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def close(self) -> None:
        with (yield from self.__write_lock):
            self.__channel.close()

            if not self._closing.done():
                self._closing.set_result(self)

            self.__channel = None

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def set_qos(self, prefetch_count: int = 0, prefetch_size: int = 0, all_channels=False, timeout: int = None):
        with (yield from self.__write_lock):
            f = self._create_future(timeout=timeout)

            self.__channel.basic_qos(
                f.set_result,
                prefetch_count=prefetch_count,
                prefetch_size=prefetch_size,
                all_channels=all_channels
            )

            return (yield from f)

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def queue_delete(self, queue_name: str, timeout: int = None,
                     if_unused: bool = False, if_empty: bool = False, nowait: bool = False):

        with (yield from self.__write_lock):
            f = self._create_future(timeout=timeout)

            self.__channel.queue_delete(
                callback=f.set_result,
                queue=queue_name,
                if_unused=if_unused,
                if_empty=if_empty,
                nowait=nowait
            )

            return (yield from f)

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def exchange_delete(self, exchange_name: str, timeout: int = None, if_unused=False, nowait=False):
        with (yield from self.__write_lock):
            f = self._create_future(timeout=timeout)

            self.__channel.exchange_delete(
                callback=f.set_result, exchange=exchange_name, if_unused=if_unused, nowait=nowait
            )

            return (yield from f)


__all__ = ('Channel',)
