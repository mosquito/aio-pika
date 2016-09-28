import asyncio
import pika.channel
from functools import partial
from logging import getLogger
from types import FunctionType
from . import exceptions
from .exchange import Exchange, ExchangeType
from .queue import Queue
from .common import BaseChannel, FutureStore, ConfirmationTypes


log = getLogger(__name__)


class Channel(BaseChannel):
    __slots__ = ('__connection', '__closing', '__confirmations', '__delivery_tag',
                 'loop', '_futures', '__channel',)

    def __init__(self, connection,
                 loop: asyncio.AbstractEventLoop, future_store: FutureStore):

        super().__init__(loop, future_store.get_child())

        self.__channel = None  # type: pika.channel.Channel
        self.__connection = connection
        self.__confirmations = {}
        self.__delivery_tag = 0

    def __str__(self):
        return "{0}".format(self.__channel.channel_number)

    def __repr__(self):
        return '<Channel "%s#%s">' % (self.__connection, self)

    def _on_channel_close(self, channel: pika.channel.Channel, code: int, reason):
        exc = exceptions.ChannelClosed(code, reason)
        log.error("Channel %r closed: %d - %s", channel, code, reason)

        self._futures.reject_all(exc)
        self._closing.set_exception(exc)

    def add_close_callback(self, callback: FunctionType):
        self._closing.add_done_callback(lambda r: callback(r.result()))

    @asyncio.coroutine
    def initialize(self, timeout=None) -> None:
        if self._closing.done():
            raise RuntimeError("Can't initialize closed channel")

        future = self._create_future(timeout=timeout)

        self.__connection._connection.channel(future.set_result)

        channel = yield from future  # type: pika.channel.Channel
        channel.confirm_delivery(self._on_delivery_confirmation)
        channel.add_on_close_callback(self._on_channel_close)

        self.__channel = channel

    def _on_delivery_confirmation(self, method_frame):
        future = self.__confirmations.pop(method_frame.method.delivery_tag, None)

        if not future:
            log.warning(
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
                         arguments: dict = None, timeout: int = None) -> Exchange:

        if auto_delete and durable is None:
            durable = False

        f = self._create_future(timeout=timeout)

        self.__channel.exchange_declare(
            f.set_result,
            name, ExchangeType(type).value, durable=durable,
            auto_delete=auto_delete, arguments=arguments
        )

        yield from f

        exchange = Exchange(
            self.__channel, self._publish, name, type,
            durable=durable, auto_delete=auto_delete, arguments=arguments,
            loop=self.loop, future_store=self._futures.get_child(),
        )

        log.debug("Exchange declared %r", exchange)

        return exchange

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def _publish(self, queue_name, routing_key, body, properties, mandatory, immediate):
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
    def declare_queue(self, name: str = '', *, durable: bool = None, exclusive: bool = False,
                      auto_delete: bool = False, arguments: dict = None, timeout: int = None) -> Queue:

        if auto_delete and durable is None:
            durable = False

        queue = Queue(
            self.loop, self._futures.get_child(), self.__channel, name,
            durable, exclusive, auto_delete, arguments
        )

        yield from queue.declare(timeout)
        return queue

    @BaseChannel._ensure_channel_is_open
    @asyncio.coroutine
    def close(self) -> None:
        self.__channel.close()

        if not self._closing.done():
            self._closing.set_result(self)

        self.__channel = None

    @BaseChannel._ensure_channel_is_open
    def set_qos(self, prefetch_count: int = 0, prefetch_size: int = 0, all_channels=False, timeout: int = None):
        f = self._create_future(timeout=timeout)

        self.__channel.basic_qos(
            f.set_result,
            prefetch_count=prefetch_count,
            prefetch_size=prefetch_size,
            all_channels=all_channels
        )

        return f

    @BaseChannel._ensure_channel_is_open
    def queue_delete(self, queue_name: str, timeout: int = None,
                     if_unused: bool = False, if_empty: bool = False, nowait: bool = False):

        f = self._create_future(timeout=timeout)

        self.__channel.queue_delete(
            callback=f.set_result,
            queue=queue_name,
            if_unused=if_unused,
            if_empty=if_empty,
            nowait=nowait
        )

        return f

    @BaseChannel._ensure_channel_is_open
    def exchange_delete(self, exchange_name: str, timeout: int = None, if_unused=False, nowait=False):

        f = self._create_future(timeout=timeout)

        self.__channel.exchange_delete(
            callback=f.set_result, exchange=exchange_name, if_unused=if_unused, nowait=nowait
        )

        return f


__all__ = ('Channel',)
