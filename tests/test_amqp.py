import asyncio
import logging
import os
import time
import unittest
import uuid
from copy import copy
from datetime import datetime
from typing import Callable
from unittest import mock

import pytest
import shortuuid

import aiormq.exceptions

import aio_pika
import aio_pika.exceptions
from aio_pika import connect, Message, DeliveryMode, Channel
from aio_pika.exceptions import (
    MessageProcessError,
    ProbableAuthenticationError,
    DeliveryError,
)
from aio_pika.exchange import ExchangeType


log = logging.getLogger(__name__)


class TestCase:
    def get_random_name(self, *args):
        prefix = ["test"]
        for item in args:
            prefix.append(item)
        prefix.append(shortuuid.uuid())

        return ".".join(prefix)

    async def test_properties(self, loop, connection: aio_pika.Connection):
        assert not connection.is_closed
        assert connection.heartbeat_last < loop.time()

    async def test_channel_close(self, connection: aio_pika.Connection):
        event = asyncio.Event()

        closed = False

        def on_close(sender, ch):
            nonlocal event, closed
            log.info("Close called")
            closed = True
            event.set()

        channel = await connection.channel()
        channel.add_close_callback(on_close)
        await channel.close()

        await event.wait()

        assert closed

        with pytest.raises(RuntimeError):
            await channel.initialize()

        async with connection.channel() as ch:
            assert not ch.is_closed

    async def test_channel_reopen(self, connection: aio_pika.Connection):
        channel = await connection.channel()

        await channel.close()
        assert channel.is_closed

        await channel.reopen()
        assert not channel.is_closed

    async def test_delete_queue_and_exchange(
        self, connection: aio_pika.Connection
    ):
        queue_name = self.get_random_name("test_connection")
        exchange = self.get_random_name()

        channel = await connection.channel()
        await channel.declare_exchange(exchange, auto_delete=True)
        await channel.declare_queue(queue_name, auto_delete=True)

        await channel.queue_delete(queue_name)
        await channel.exchange_delete(exchange)

    async def test_temporary_queue(self, connection: aio_pika.Connection):
        channel = await connection.channel()
        queue = await channel.declare_queue(auto_delete=True)

        assert queue.name != ""

        body = os.urandom(32)

        await channel.default_exchange.publish(
            Message(body=body), routing_key=queue.name
        )

        message = await queue.get()

        assert message.body == body

        await channel.queue_delete(queue.name)

    async def test_internal_exchange(self, channel: aio_pika.Channel):
        routing_key = self.get_random_name()
        exchange_name = self.get_random_name("internal", "exchange")

        exchange = await channel.declare_exchange(
            exchange_name, auto_delete=True, internal=True,
        )

        queue = await channel.declare_queue(auto_delete=True)

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        with pytest.raises(ValueError):
            f = exchange.publish(
                Message(
                    body, content_type="text/plain", headers={"foo": "bar"}
                ),
                routing_key,
            )
            await f

        await queue.unbind(exchange, routing_key)

    async def test_declare_exchange_with_passive_flag(
        self, connection: aio_pika.Connection, declare_exchange: Callable
    ):
        exchange_name = self.get_random_name()
        channel = await connection.channel()

        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            await declare_exchange(
                exchange_name, auto_delete=True, passive=True, channel=channel
            )

        channel1 = await connection.channel()
        channel2 = await connection.channel()

        await declare_exchange(
            exchange_name, auto_delete=True, passive=False, channel=channel1
        )

        # Check ignoring different exchange options
        await declare_exchange(
            exchange_name, auto_delete=False, passive=True, channel=channel2
        )

    async def test_declare_queue_with_passive_flag(
        self,
        connection,
        channel,
        declare_exchange: Callable,
        declare_queue: Callable,
    ):
        queue_name = self.get_random_name()

        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            await declare_queue(
                queue_name, auto_delete=True, passive=True, channel=channel
            )

        channel1 = await connection.channel()
        channel2 = await connection.channel()

        await declare_queue(
            queue_name, auto_delete=True, passive=False, channel=channel1
        )

        # Check ignoring different queue options
        await declare_queue(
            queue_name, auto_delete=False, passive=True, channel=channel2
        )

    async def test_simple_publish_and_receive(
        self, channel, declare_queue: Callable, declare_exchange: Callable
    ):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel
        )

        queue = await declare_queue(
            queue_name, auto_delete=True, channel=channel
        )

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        result = await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )
        assert result

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        assert incoming_message.body == body

        await queue.unbind(exchange, routing_key)

    async def test_simple_publish_without_confirm(
        self, connection, declare_exchange: Callable, declare_queue: Callable
    ):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await connection.channel(publisher_confirms=False)

        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel
        )
        queue = await declare_queue(
            queue_name, auto_delete=True, channel=channel
        )

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        result = await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )
        assert result is None

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        assert incoming_message.body == body

        await queue.unbind(exchange, routing_key)

    async def test_simple_publish_and_receive_delivery_mode_explicitly(
        self, channel, declare_queue: Callable, declare_exchange: Callable
    ):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel
        )
        queue = await declare_queue(
            queue_name, auto_delete=True, channel=channel
        )

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        await exchange.publish(
            Message(
                body,
                content_type="text/plain",
                headers={"foo": "bar"},
                delivery_mode=None,
            ),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        assert incoming_message.body == body

        await queue.unbind(exchange, routing_key)

    async def test_simple_publish_and_receive_to_bound_exchange(
        self,
        channel,
        declare_exchange: Callable,
        declare_queue: Callable,
        add_cleanup: Callable,
    ):
        routing_key = self.get_random_name()
        src_name = self.get_random_name("source", "exchange")
        dest_name = self.get_random_name("destination", "exchange")

        src_exchange = await declare_exchange(src_name, auto_delete=True)
        dest_exchange = await declare_exchange(dest_name, auto_delete=True)
        queue = await declare_queue(auto_delete=True)

        await queue.bind(dest_exchange, routing_key)

        await dest_exchange.bind(src_exchange, routing_key)
        add_cleanup(dest_exchange.unbind, src_exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        await src_exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        assert incoming_message.body == body

        await queue.unbind(dest_exchange, routing_key)

    async def test_incoming_message_info(
        self,
        channel,
        declare_queue: Callable,
        declare_exchange: Callable,
        add_cleanup: Callable,
    ):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)
        add_cleanup(queue.unbind, exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        self.maxDiff = None

        info = {
            "headers": {"foo": b"bar"},
            "content_type": "application/json",
            "content_encoding": "text",
            "delivery_mode": DeliveryMode.PERSISTENT.value,
            "priority": 0,
            "correlation_id": "1",
            "reply_to": "test",
            "expiration": 1.5,
            "message_id": shortuuid.uuid(),
            "timestamp": datetime.utcfromtimestamp(int(time.time())),
            "type": "0",
            "user_id": "guest",
            "app_id": "test",
            "body_size": len(body),
        }

        msg = Message(
            body=body,
            headers={"foo": b"bar"},
            content_type="application/json",
            content_encoding="text",
            delivery_mode=DeliveryMode.PERSISTENT,
            priority=0,
            correlation_id=1,
            reply_to="test",
            expiration=1.5,
            message_id=info["message_id"],
            timestamp=info["timestamp"],
            type="0",
            user_id="guest",
            app_id="test",
        )

        await exchange.publish(msg, routing_key)

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        info["routing_key"] = incoming_message.routing_key
        info["redelivered"] = incoming_message.redelivered
        info["exchange"] = incoming_message.exchange
        info["delivery_tag"] = incoming_message.delivery_tag
        info["consumer_tag"] = incoming_message.consumer_tag
        info["cluster_id"] = incoming_message.cluster_id

        assert incoming_message.body == body
        assert incoming_message.info() == info

    async def test_context_process(
        self,
        channel: aio_pika.Channel,
        declare_queue: Callable,
        declare_exchange: Callable,
        add_cleanup: Callable,
    ):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)
        add_cleanup(queue.unbind, exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5)

        with pytest.raises(AssertionError):
            async with incoming_message.process(requeue=True):
                raise AssertionError

        assert incoming_message.locked

        incoming_message = await queue.get(timeout=5)

        async with incoming_message.process():
            pass

        assert incoming_message.body == body

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5)

        with pytest.raises(MessageProcessError):
            async with incoming_message.process():
                incoming_message.reject(requeue=True)

        assert incoming_message.locked

        incoming_message = await queue.get(timeout=5)

        async with incoming_message.process(ignore_processed=True):
            incoming_message.reject(requeue=False)

        assert incoming_message.body == body

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5)
        with pytest.raises(AssertionError):
            async with incoming_message.process(
                requeue=True, reject_on_redelivered=True
            ):
                raise AssertionError

        incoming_message = await queue.get(timeout=5)
        with pytest.raises(AssertionError):
            async with incoming_message.process(
                requeue=True, reject_on_redelivered=True
            ):
                raise AssertionError

        assert incoming_message.locked

    async def test_context_process_redelivery(
        self,
        channel: aio_pika.Channel,
        declare_exchange: Callable,
        declare_queue: Callable,
        add_cleanup: Callable,
    ):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)
        add_cleanup(queue.unbind, exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5)

        with pytest.raises(AssertionError):
            async with incoming_message.process(
                requeue=True, reject_on_redelivered=True
            ):
                raise AssertionError

        incoming_message = await queue.get(timeout=5)

        with mock.patch("aio_pika.message.log") as message_logger:
            with pytest.raises(Exception):
                async with incoming_message.process(
                    requeue=True, reject_on_redelivered=True
                ):
                    raise Exception

            assert message_logger.info.called
            assert (
                message_logger.info.mock_calls[0][1][1].body
                == incoming_message.body
            )

        assert incoming_message.body == body

    async def test_no_ack_redelivery(
        self, connection: aio_pika.Connection, add_cleanup: Callable
    ):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await connection.channel()
        exchange = await channel.declare_exchange("direct", auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=False)

        await queue.bind(exchange, routing_key)

        # publish 2 messages
        for _ in range(2):
            body = bytes(shortuuid.uuid(), "utf-8")
            msg = Message(body)
            await exchange.publish(msg, routing_key)

        # ack 1 message out of 2
        first_message = await queue.get(timeout=5)

        last_message = await queue.get(timeout=5)
        last_message.ack()

        # close channel, not acked message should be redelivered
        await channel.close()

        channel = await connection.channel()
        exchange = await channel.declare_exchange("direct", auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=False)

        # receive not acked message
        message = await queue.get(timeout=5)
        assert message.body == first_message.body
        message.ack()

        await queue.unbind(exchange, routing_key)

    async def test_ack_multiple(self, connection: aio_pika.Connection):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await connection.channel()
        exchange = await channel.declare_exchange("direct", auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=False)

        await queue.bind(exchange, routing_key)

        # publish 2 messages
        for _ in range(2):
            body = bytes(shortuuid.uuid(), "utf-8")
            msg = Message(body)
            await exchange.publish(msg, routing_key)

        # ack only last mesage with multiple flag, first
        # message should be acked too
        await queue.get(timeout=5)
        last_message = await queue.get(timeout=5)
        last_message.ack(multiple=True)

        # close channel, no messages should be redelivered
        await channel.close()

        channel = await connection.channel()
        exchange = await channel.declare_exchange("direct", auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=False)

        with pytest.raises(aio_pika.exceptions.QueueEmpty):
            await queue.get()

        await queue.unbind(exchange, routing_key)
        await queue.delete()
        await asyncio.wait((connection.close(), connection.closing))

    async def test_ack_twice(self, connection):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await connection.channel()
        exchange = await channel.declare_exchange("direct", auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        with pytest.raises(MessageProcessError):
            incoming_message.ack()

        assert incoming_message.body == body

        await queue.unbind(exchange, routing_key)
        await queue.delete()

    async def test_reject_twice(
        self,
        channel: aio_pika.Channel,
        add_cleanup: Callable,
        declare_queue: Callable,
        declare_exchange: Callable,
    ):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)
        add_cleanup(queue.unbind, exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5)
        incoming_message.reject(requeue=False)

        with pytest.raises(MessageProcessError):
            incoming_message.reject(requeue=False)

        assert incoming_message.body == body

    async def test_consuming(
        self,
        loop,
        channel: aio_pika.Channel,
        declare_exchange: Callable,
        declare_queue: Callable,
        add_cleanup: Callable,
    ):
        queue_name = self.get_random_name("tc2")
        routing_key = self.get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)
        add_cleanup(queue.unbind, exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        f = loop.create_future()

        async def handle(message):
            message.ack()
            assert message.body == body
            assert message.routing_key == routing_key
            f.set_result(True)

        await queue.consume(handle)

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        if not f.done():
            await f

    async def test_consuming_not_coroutine(
        self,
        channel: aio_pika.Channel,
        loop,
        declare_exchange,
        declare_queue,
        add_cleanup,
    ):

        queue_name = self.get_random_name("tc2")
        routing_key = self.get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)
        add_cleanup(queue.unbind, exchange, routing_key)

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        f = loop.create_future()

        def handle(message):
            message.ack()
            assert message.body == body
            assert message.routing_key == routing_key
            f.set_result(True)

        await queue.consume(handle)

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        if not f.done():
            await f

    async def test_ack_reject(
        self,
        channel: aio_pika.Channel,
        declare_exchange,
        declare_queue,
        add_cleanup,
    ):
        queue_name = self.get_random_name("test_connection3")
        routing_key = self.get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)
        add_cleanup(queue.unbind, exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5, no_ack=True)

        with pytest.raises(TypeError):
            incoming_message.ack()

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5)

        incoming_message.reject()

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5, no_ack=True)

        with pytest.raises(TypeError):
            await incoming_message.reject()

        assert incoming_message.body == body

    async def test_purge_queue(self, declare_queue, declare_exchange, channel):
        queue_name = self.get_random_name("test_connection4")
        routing_key = self.get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)

        try:
            body = bytes(shortuuid.uuid(), "utf-8")

            await exchange.publish(
                Message(
                    body, content_type="text/plain", headers={"foo": "bar"}
                ),
                routing_key,
            )

            await queue.purge()

            with pytest.raises(asyncio.TimeoutError):
                await queue.get(timeout=1)
        except aio_pika.exceptions.QueueEmpty:
            await queue.unbind(exchange, routing_key)
            await queue.delete()

    async def test_connection_refused(self):
        with pytest.raises(ConnectionError):
            await connect("amqp://guest:guest@localhost:9999")

    async def test_wrong_credentials(self, amqp_url):
        amqp_url = amqp_url.with_user(uuid.uuid4().hex).with_password(
            uuid.uuid4().hex
        )

        with pytest.raises(ProbableAuthenticationError):
            await connect(str(amqp_url))

    async def test_set_qos(self, channel: aio_pika.Channel):
        await channel.set_qos(prefetch_count=1, global_=True)

    async def test_exchange_delete(self, channel: aio_pika.Channel):
        exchange = await channel.declare_exchange("test", auto_delete=True)
        await exchange.delete()

    async def test_dlx(
        self,
        channel: aio_pika.Channel,
        declare_exchange,
        declare_queue,
        add_cleanup,
        loop,
    ):
        suffix = self.get_random_name()
        routing_key = "%s_routing_key" % suffix
        dlx_routing_key = "%s_dlx_routing_key" % suffix

        f = loop.create_future()

        async def dlx_handle(message):
            message.ack()
            assert message.body == body
            assert message.routing_key == dlx_routing_key
            f.set_result(True)

        direct_exchange = await declare_exchange(
            "direct", channel=channel, auto_delete=True
        )  # type: aio_pika.Exchange

        dlx_exchange = await declare_exchange(
            "dlx", ExchangeType.DIRECT, auto_delete=True
        )

        direct_queue = await declare_queue(
            "%s_direct_queue" % suffix,
            auto_delete=True,
            arguments={
                "x-message-ttl": 300,
                "x-dead-letter-exchange": "dlx",
                "x-dead-letter-routing-key": dlx_routing_key,
            },
        )

        dlx_queue = await declare_queue(
            "%s_dlx_queue" % suffix, auto_delete=True
        )

        await dlx_queue.consume(dlx_handle)
        await dlx_queue.bind(dlx_exchange, dlx_routing_key)
        await direct_queue.bind(direct_exchange, routing_key)

        add_cleanup(dlx_queue.unbind, dlx_exchange, routing_key)
        add_cleanup(direct_queue.unbind, direct_exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        await direct_exchange.publish(
            Message(
                body,
                content_type="text/plain",
                headers={
                    "x-message-ttl": 100,
                    "x-dead-letter-exchange": "dlx",
                },
            ),
            routing_key,
        )

        if not f.done():
            await f

    async def test_connection_close(self, connection, declare_exchange):
        routing_key = self.get_random_name()

        channel = await connection.channel()  # type: aio_pika.Channel
        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel
        )

        try:
            with pytest.raises(aio_pika.exceptions.ChannelPreconditionFailed):
                msg = Message(bytes(shortuuid.uuid(), "utf-8"))
                msg.delivery_mode = 8

                await exchange.publish(msg, routing_key)

            channel = await connection.channel()
            exchange = await channel.declare_exchange(
                "direct", auto_delete=True
            )
        finally:
            await exchange.delete()

    async def test_basic_return(self, connection, loop):
        channel = await connection.channel()  # type: aio_pika.Channel

        f = loop.create_future()

        def handler(sender, *args, **kwargs):
            f.set_result(*args, **kwargs)

        channel.add_on_return_callback(handler)

        body = bytes(shortuuid.uuid(), "utf-8")

        await channel.default_exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            self.get_random_name("test_basic_return"),
        )

        returned = await f

        assert returned.body == body

        # handler with exception
        f = loop.create_future()

        await channel.close()

        channel = await connection.channel()  # type: aio_pika.Channel

        def bad_handler(sender, message):
            try:
                raise ValueError
            finally:
                f.set_result(message)

        channel.add_on_return_callback(bad_handler)

        body = bytes(shortuuid.uuid(), "utf-8")

        await channel.default_exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            self.get_random_name("test_basic_return"),
        )

        returned = await f

        assert returned.body == body

    async def test_expiration(self, connection, loop):
        channel = await connection.channel()  # type: aio_pika.Channel

        dlx_queue = await channel.declare_queue(
            self.get_random_name("test_dlx")
        )  # type: aio_pika.Queue

        dlx_exchange = await channel.declare_exchange(
            self.get_random_name("dlx"),
        )  # type: aio_pika.Exchange

        await dlx_queue.bind(dlx_exchange, routing_key=dlx_queue.name)

        queue = await channel.declare_queue(
            self.get_random_name("test_expiration"),
            arguments={
                "x-message-ttl": 10000,
                "x-dead-letter-exchange": dlx_exchange.name,
                "x-dead-letter-routing-key": dlx_queue.name,
            },
        )  # type: aio_pika.Queue

        body = bytes(shortuuid.uuid(), "utf-8")

        await channel.default_exchange.publish(
            Message(
                body,
                content_type="text/plain",
                headers={"foo": "bar"},
                expiration=0.5,
            ),
            queue.name,
        )

        f = loop.create_future()

        await dlx_queue.consume(f.set_result, no_ack=True)

        message = await f

        assert message.body == body
        assert message.headers["x-death"][0]["original-expiration"] == b"500"

    async def test_add_close_callback(self, create_connection):
        connection = await create_connection()

        shared_list = []

        def share(*a, **kw):
            shared_list.append((a, kw))

        connection.add_close_callback(share)
        await connection.close()

        assert len(shared_list) == 1

    async def test_big_message(self, connection, add_cleanup):
        queue_name = self.get_random_name("test_big")
        routing_key = self.get_random_name()

        channel = await connection.channel()
        exchange = await channel.declare_exchange("direct", auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)
        add_cleanup(queue.unbind, exchange, routing_key)
        add_cleanup(queue.delete)

        body = bytes(shortuuid.uuid(), "utf-8") * 1000000

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        assert incoming_message.body == body

    async def test_unexpected_channel_close(self, connection):
        channel = await connection.channel()

        with pytest.raises(aio_pika.exceptions.ChannelClosed):
            await channel.declare_queue(
                "amq.restricted_queue_name", auto_delete=True
            )

        with pytest.raises(aiormq.exceptions.ChannelInvalidStateError):
            await channel.set_qos(100)

    async def test_declaration_result(self, channel):
        queue = await channel.declare_queue(auto_delete=True)
        assert queue.declaration_result.message_count == 0
        assert queue.declaration_result.consumer_count == 0

    async def test_declaration_result_with_consumers(self, connection):
        channel1 = await connection.channel()

        queue_name = self.get_random_name("queue", "declaration-result")
        queue1 = await channel1.declare_queue(queue_name, auto_delete=True)
        await queue1.consume(print)

        channel2 = await connection.channel()

        queue2 = await channel2.declare_queue(queue_name, passive=True)

        assert queue2.declaration_result.consumer_count == 1

    async def test_declaration_result_with_messages(self, connection):
        channel1 = await connection.channel()
        channel2 = await connection.channel()

        queue_name = self.get_random_name("queue", "declaration-result")
        queue1 = await channel1.declare_queue(queue_name, auto_delete=True)

        await channel1.default_exchange.publish(
            Message(body=b"test"), routing_key=queue1.name
        )

        queue2 = await channel2.declare_queue(queue_name, passive=True)
        await queue2.get()
        await queue2.delete()

        assert queue2.declaration_result.consumer_count == 0
        assert queue2.declaration_result.message_count == 1

    async def test_queue_empty_exception(self, connection, add_cleanup):
        queue_name = self.get_random_name("test_get_on_empty_queue")

        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, auto_delete=True)
        add_cleanup(queue.delete)

        with pytest.raises(aio_pika.exceptions.QueueEmpty):
            await queue.get(timeout=5)

        await channel.default_exchange.publish(
            Message(b"test"), queue_name,
        )

        message = await queue.get(timeout=5)
        assert message.body == b"test"

        # test again for #110
        with pytest.raises(aio_pika.exceptions.QueueEmpty):
            await queue.get(timeout=5)

    async def test_queue_empty_fail_false(self, connection, add_cleanup):
        queue_name = self.get_random_name("test_get_on_empty_queue")
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, auto_delete=True)
        add_cleanup(queue.delete)

        result = await queue.get(fail=False)
        assert result is None

    async def test_message_nack(self, connection, add_cleanup):
        queue_name = self.get_random_name("test_nack_queue")
        body = uuid.uuid4().bytes
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, auto_delete=True)
        add_cleanup(queue.delete)

        await channel.default_exchange.publish(
            Message(body=body), routing_key=queue_name
        )

        message = await queue.get()  # type: aio_pika.IncomingMessage

        assert message.body == body
        message.nack(requeue=True)

        message = await queue.get()

        assert message.redelivered
        assert message.body == body
        await message.ack()

    async def test_on_return_raises(self, connection):
        queue_name = self.get_random_name("test_on_return_raises")
        body = uuid.uuid4().bytes

        with pytest.raises(RuntimeError):
            await connection.channel(
                publisher_confirms=False, on_return_raises=True
            )

        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True
        )

        for _ in range(100):
            with pytest.raises(aio_pika.exceptions.DeliveryError):
                await channel.default_exchange.publish(
                    Message(body=body), routing_key=queue_name,
                )

    async def test_transaction_when_publisher_confirms_error(
        self, connection: aio_pika.Connection
    ):
        async with connection.channel(publisher_confirms=True) as channel:
            with pytest.raises(RuntimeError):
                channel.transaction()

    async def test_transaction_simple_commit(
        self, connection: aio_pika.Connection
    ):
        async with connection.channel(publisher_confirms=False) as channel:
            tx = channel.transaction()
            await tx.select()
            await tx.commit()

    async def test_transaction_simple_rollback(self, connection):
        async with connection.channel(publisher_confirms=False) as channel:
            tx = channel.transaction()
            await tx.select()
            await tx.rollback()

    async def test_transaction_simple_async_commit(self, connection):
        async with connection.channel(publisher_confirms=False) as channel:
            async with channel.transaction():
                pass

    async def test_transaction_simple_async_rollback(self, connection):
        async with connection.channel(publisher_confirms=False) as channel:
            with pytest.raises(ValueError):
                async with channel.transaction():
                    raise ValueError

    async def test_async_for_queue(
        self, loop, connection: aio_pika.Connection
    ):
        channel2 = await connection.channel()

        queue = await channel2.declare_queue(
            self.get_random_name("queue", "is_async", "for"), auto_delete=True
        )

        messages = 100

        async def publisher():
            channel1 = await connection.channel()

            for i in range(messages):
                await channel1.default_exchange.publish(
                    Message(body=str(i).encode()), routing_key=queue.name
                )

        loop.create_task(publisher())

        count = 0
        data = list()

        async for message in queue:
            async with message.process():
                count += 1
                data.append(message.body)

            if count >= messages:
                break

        assert data == list(map(lambda x: str(x).encode(), range(messages)))

    async def test_async_for_queue_context(self, loop, connection):
        channel2 = await connection.channel()

        queue = await channel2.declare_queue(
            self.get_random_name("queue", "is_async", "for"), auto_delete=True
        )

        messages = 100

        async def publisher():
            channel1 = await connection.channel()

            for i in range(messages):
                await channel1.default_exchange.publish(
                    Message(body=str(i).encode()), routing_key=queue.name
                )

        loop.create_task(publisher())

        count = 0
        data = list()

        async with queue.iterator() as queue_iterator:
            async for message in queue_iterator:
                async with message.process():
                    count += 1
                    data.append(message.body)

                if count >= messages:
                    break

        assert data == list(map(lambda x: str(x).encode(), range(messages)))

    async def test_async_with_connection(self, create_connection, loop):
        async with await create_connection() as connection:

            channel2 = await connection.channel()

            queue = await channel2.declare_queue(
                self.get_random_name("queue", "is_async", "for"),
                auto_delete=True,
            )

            messages = 100

            async def publisher():
                channel1 = await connection.channel()

                for i in range(messages):
                    await channel1.default_exchange.publish(
                        Message(body=str(i).encode()), routing_key=queue.name
                    )

            loop.create_task(publisher())

            count = 0
            data = list()

            async with queue.iterator() as queue_iterator:
                async for message in queue_iterator:
                    async with message.process():
                        count += 1
                        data.append(message.body)

                    if count >= messages:
                        break

            assert data == list(
                map(lambda x: str(x).encode(), range(messages))
            )

        assert channel2.is_closed

    async def test_async_with_channel(self, connection: aio_pika.Connection):
        async with connection.channel() as channel:
            assert isinstance(channel, Channel)

        assert channel.is_closed

    async def test_delivery_fail(self, channel: aio_pika.Channel):
        queue = await channel.declare_queue(exclusive=True, arguments={
            'x-max-length': 1,
            'x-overflow': 'reject-publish',
        }, auto_delete=True)

        await channel.default_exchange.publish(
            aio_pika.Message(body=b'queue me'),
            routing_key=queue.name
        )

        with pytest.raises(DeliveryError):
            for _ in range(10):
                await channel.default_exchange.publish(
                    aio_pika.Message(body=b'reject me'),
                    routing_key=queue.name
                )

    async def test_channel_locked_resource(self, connection: aio_pika.Connection):
        ch1 = await connection.channel()
        ch2 = await connection.channel()

        qname = self.get_random_name("channel", "locked", "resource")

        q1 = await ch1.declare_queue(qname, exclusive=True)
        await q1.consume(print, exclusive=True)

        with pytest.raises(aiormq.exceptions.ChannelAccessRefused):
            q2 = await ch2.declare_queue(qname, exclusive=True)
            await q2.consume(print, exclusive=True)

    async def test_queue_iterator_close_was_called_twice(self, create_connection, loop):
        future = loop.create_future()
        event = asyncio.Event()

        queue_name = self.get_random_name()

        async def task_inner():
            nonlocal future
            nonlocal event
            nonlocal create_connection

            try:
                connection = await create_connection()

                async with connection:
                    channel = await connection.channel()

                    queue = await channel.declare_queue(queue_name)

                    async with queue.iterator() as q:
                        event.set()

                        async for message in q:
                            with message.process():
                                break

            except asyncio.CancelledError as e:
                future.set_exception(e)
                raise

        task = loop.create_task(task_inner())

        await event.wait()
        loop.call_soon(task.cancel)

        with pytest.raises(asyncio.CancelledError):
            await task

        with pytest.raises(asyncio.CancelledError):
            await future

    async def test_queue_iterator_close_with_noack(self, create_connection, loop, add_cleanup):
        messages = []
        queue_name = self.get_random_name("test_queue")
        body = self.get_random_name("test_body").encode()

        async def task_inner():
            nonlocal messages
            nonlocal create_connection
            nonlocal add_cleanup

            connection = await create_connection()
            add_cleanup(connection.close)

            async with connection:
                channel = await connection.channel()

                queue = await channel.declare_queue(queue_name)

                async with queue.iterator(no_ack=True) as q:
                    async for message in q:
                        messages.append(message)
                        return

        async with await create_connection() as connection:
            channel = await connection.channel()

            await channel.declare_queue(queue_name)

            await channel.default_exchange.publish(
                Message(body),
                routing_key=queue_name,
            )

            task = loop.create_task(task_inner())

            await task

            assert messages
            assert messages[0].body == body

    async def test_passive_for_exchange(self, declare_exchange, connection, add_cleanup):
        name = self.get_random_name("passive", "exchange")

        ch1 = await connection.channel()
        ch2 = await connection.channel()
        ch3 = await connection.channel()

        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            await declare_exchange(name, passive=True, channel=ch1)

        exchange = await declare_exchange(name, auto_delete=True, channel=ch2)
        exchange_passive = await declare_exchange(name, passive=True, channel=ch3)

        assert exchange.name == exchange_passive.name

    async def test_passive_queue(self, declare_queue, connection):
        name = self.get_random_name("passive", "queue")

        ch1 = await connection.channel()
        ch2 = await connection.channel()
        ch3 = await connection.channel()

        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            await declare_queue(name, passive=True, channel=ch1)

        queue = await declare_queue(name, auto_delete=True, channel=ch2)
        queue_passive = await declare_queue(name, passive=True, channel=ch3)

        assert queue.name == queue_passive.name

    async def test_get_exchange(self, connection):
        channel = await connection.channel()
        name = self.get_random_name("passive", "exchange")

        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            await channel.get_exchange(name)

        channel = await connection.channel()
        exchange = await channel.declare_exchange(name, auto_delete=True)
        exchange_passive = await channel.get_exchange(name)

        assert exchange.name == exchange_passive.name

    async def test_get_queue(self, connection):
        channel = await connection.channel()
        name = self.get_random_name("passive", "queue")

        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            await channel.get_queue(name)

        channel = await connection.channel()
        queue = await channel.declare_queue(name, auto_delete=True)
        queue_passive = await channel.get_queue(name)

        assert queue.name, queue_passive.name


class MessageTestCase:
    def test_message_copy(self):
        msg1 = Message(
            bytes(shortuuid.uuid(), 'utf-8'),
            content_type='application/json',
            content_encoding='text',
            timestamp=datetime(2000, 1, 1),
            headers={'h1': 'v1', 'h2': 'v2'},
        )
        msg2 = copy(msg1)

        msg1.lock()

        assert not msg2.locked

    def test_message_info(self):
        body = bytes(shortuuid.uuid(), 'utf-8')

        info = {
            'headers': {"foo": b"bar"},
            'content_type': "application/json",
            'content_encoding': "text",
            'delivery_mode': DeliveryMode.PERSISTENT.value,
            'priority': 0,
            'correlation_id': '1',
            'reply_to': 'test',
            'expiration': 1.5,
            'message_id': shortuuid.uuid(),
            'timestamp': datetime.utcfromtimestamp(int(time.time())),
            'type': '0',
            'user_id': 'guest',
            'app_id': 'test',
            'body_size': len(body)
        }

        msg = Message(
            body=body,
            headers={'foo': b'bar'},
            content_type='application/json',
            content_encoding='text',
            delivery_mode=DeliveryMode.PERSISTENT,
            priority=0,
            correlation_id=1,
            reply_to='test',
            expiration=1.5,
            message_id=info['message_id'],
            timestamp=info['timestamp'],
            type='0',
            user_id='guest',
            app_id='test'
        )

        assert info == msg.info()

    def test_headers_setter(self):
        data = {'foo': 'bar'}
        data_expected = {'foo': b'bar'}

        msg = Message(b'', headers={'bar': 'baz'})
        msg.headers = data

        assert msg.headers_raw == data_expected

    def test_headers_content(self):
        data = (
            [42, 42, 42],
            ['foo', b'foo', 'foo'],
            [b'\00', b'\00', '\00'],
        )

        for src, raw, value in data:
            msg = Message(b'', headers={'value': src})
            assert msg.headers_raw['value'] == raw
            assert msg.headers['value'] == value

    def test_headers_set(self):
        msg = Message(b'', headers={'header': 'value'})

        data = (
            ['header-1', 42, 42, 42],
            ['header-2', 'foo', b'foo', 'foo'],
            ['header-3', b'\00', b'\00', '\00'],
        )

        for name, src, raw, value in data:
            msg.headers[name] = value
            assert msg.headers_raw[name] == raw
            assert msg.headers[name] == value

        assert msg.headers['header'] == 'value'
