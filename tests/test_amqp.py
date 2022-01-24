import asyncio
import logging
import os
import time
import uuid
from datetime import datetime
from typing import Awaitable, Callable
from unittest import mock

import aiormq.exceptions
import pytest
import shortuuid

import aio_pika
import aio_pika.exceptions
from aio_pika import Channel, DeliveryMode, Message
from aio_pika.exceptions import (
    DeliveryError, MessageProcessError, ProbableAuthenticationError,
)
from aio_pika.exchange import ExchangeType
from tests import get_random_name


log = logging.getLogger(__name__)


class TestCaseAmqpBase:
    @staticmethod
    def create_channel(
        connection: aio_pika.Connection,
    ) -> Awaitable[aio_pika.Channel]:
        return connection.channel()

    @staticmethod
    @pytest.fixture(name="declare_queue")
    def declare_queue_(declare_queue):
        return declare_queue

    @staticmethod
    @pytest.fixture(name="declare_exchange")
    def declare_exchange_(declare_exchange):
        return declare_exchange


class TestCaseAmqp(TestCaseAmqpBase):
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

        channel = await self.create_channel(connection)
        channel.add_close_callback(on_close)
        await channel.close()

        await event.wait()

        assert closed

        with pytest.raises(RuntimeError):
            await channel.initialize()

        async with self.create_channel(connection) as ch:
            assert not ch.is_closed

    async def test_channel_reopen(self, connection):
        channel = await self.create_channel(connection)

        await channel.close()
        assert channel.is_closed

        await channel.reopen()
        assert not channel.is_closed

    async def test_delete_queue_and_exchange(
        self, connection, declare_exchange, declare_queue
    ):
        queue_name = get_random_name("test_connection")
        exchange = get_random_name()

        channel = await self.create_channel(connection)
        await declare_exchange(exchange, auto_delete=True)
        await declare_queue(queue_name, auto_delete=True)

        await channel.queue_delete(queue_name)
        await channel.exchange_delete(exchange)

    async def test_temporary_queue(self, connection, declare_queue):
        channel = await self.create_channel(connection)
        queue = await declare_queue(auto_delete=True)

        assert queue.name != ""

        body = os.urandom(32)

        await channel.default_exchange.publish(
            Message(body=body), routing_key=queue.name,
        )

        await asyncio.sleep(1)

        message = await queue.get()

        assert message.body == body

        await channel.queue_delete(queue.name)

    async def test_internal_exchange(
        self, channel: aio_pika.Channel, declare_exchange, declare_queue
    ):
        routing_key = get_random_name()
        exchange_name = get_random_name("internal", "exchange")

        exchange = await declare_exchange(
            exchange_name, auto_delete=True, internal=True,
        )

        queue = await declare_queue(auto_delete=True)

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        with pytest.raises(ValueError):
            f = exchange.publish(
                Message(
                    body, content_type="text/plain", headers={"foo": "bar"},
                ),
                routing_key,
            )
            await f

        await queue.unbind(exchange, routing_key)

    async def test_declare_exchange_with_passive_flag(
        self, connection, declare_exchange: Callable
    ):
        exchange_name = get_random_name()
        channel = await self.create_channel(connection)

        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            await declare_exchange(
                exchange_name, auto_delete=True, passive=True, channel=channel,
            )

        channel1 = await self.create_channel(connection)
        channel2 = await self.create_channel(connection)

        await declare_exchange(
            exchange_name, auto_delete=True, passive=False, channel=channel1,
        )

        # Check ignoring different exchange options
        await declare_exchange(
            exchange_name, auto_delete=False, passive=True, channel=channel2,
        )

    async def test_declare_queue_with_passive_flag(
        self, connection, declare_exchange: Callable, declare_queue: Callable,
    ):
        queue_name = get_random_name()
        ch1 = await self.create_channel(connection)
        ch2 = await self.create_channel(connection)
        ch3 = await self.create_channel(connection)

        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            await declare_queue(
                queue_name, auto_delete=True, passive=True, channel=ch1,
            )

        await declare_queue(
            queue_name, auto_delete=True, passive=False, channel=ch2,
        )

        # Check ignoring different queue options
        await declare_queue(
            queue_name, auto_delete=False, passive=True, channel=ch3,
        )

    async def test_simple_publish_and_receive(
        self,
        channel: aio_pika.Channel,
        declare_queue: Callable,
        declare_exchange: Callable,
    ):
        queue_name = get_random_name("test_connection")
        routing_key = get_random_name()

        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel,
        )

        queue = await declare_queue(
            queue_name, auto_delete=True, channel=channel,
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
        self,
        connection: aio_pika.Connection,
        declare_exchange: Callable,
        declare_queue: Callable,
    ):
        queue_name = get_random_name("test_connection")
        routing_key = get_random_name()

        channel = await connection.channel(publisher_confirms=False)

        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel,
        )
        queue = await declare_queue(
            queue_name, auto_delete=True, channel=channel,
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
        self,
        channel: aio_pika.Channel,
        declare_queue: Callable,
        declare_exchange: Callable,
    ):
        queue_name = get_random_name("test_connection")
        routing_key = get_random_name()

        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel,
        )
        queue = await declare_queue(
            queue_name, auto_delete=True, channel=channel,
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
        channel: aio_pika.Channel,
        declare_exchange: Callable,
        declare_queue: Callable,
        add_cleanup: Callable,
    ):
        routing_key = get_random_name()
        src_name = get_random_name("source", "exchange")
        dest_name = get_random_name("destination", "exchange")

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
        channel: aio_pika.Channel,
        declare_queue: Callable,
        declare_exchange: Callable,
        add_cleanup: Callable,
    ):
        queue_name = get_random_name("test_connection")
        routing_key = get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)
        add_cleanup(queue.unbind, exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

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
        queue_name = get_random_name("test_connection")
        routing_key = get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)
        add_cleanup(queue.unbind, exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        if not channel._publisher_confirms:
            await asyncio.sleep(1)

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
                requeue=True, reject_on_redelivered=True,
            ):
                raise AssertionError

        incoming_message = await queue.get(timeout=5)
        with pytest.raises(AssertionError):
            async with incoming_message.process(
                requeue=True, reject_on_redelivered=True,
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
        queue_name = get_random_name("test_connection")
        routing_key = get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)
        add_cleanup(queue.unbind, exchange, routing_key)

        body = bytes(shortuuid.uuid(), "utf-8")

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        if not channel._publisher_confirms:
            await asyncio.sleep(1)

        incoming_message = await queue.get(timeout=5)

        with pytest.raises(AssertionError):
            async with incoming_message.process(
                requeue=True, reject_on_redelivered=True,
            ):
                raise AssertionError

        incoming_message = await queue.get(timeout=5)

        with mock.patch("aio_pika.message.log") as message_logger:
            with pytest.raises(Exception):
                async with incoming_message.process(
                    requeue=True, reject_on_redelivered=True,
                ):
                    raise Exception

            assert message_logger.info.called
            assert (
                message_logger.info.mock_calls[0][1][1].body
                == incoming_message.body
            )

        assert incoming_message.body == body

    async def test_no_ack_redelivery(
        self,
        connection,
        add_cleanup: Callable,
        declare_queue,
        declare_exchange,
    ):
        queue_name = get_random_name("test_connection")
        routing_key = get_random_name()

        channel = await self.create_channel(connection)
        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel,
        )
        queue = await declare_queue(
            queue_name, auto_delete=False, channel=channel, cleanup=False,
        )

        await queue.bind(exchange, routing_key)

        # publish 2 messages
        for _ in range(2):
            body = bytes(shortuuid.uuid(), "utf-8")
            msg = Message(body)
            await exchange.publish(msg, routing_key)

        if not channel._publisher_confirms:
            await asyncio.sleep(1)

        # ack 1 message out of 2
        first_message = await queue.get(timeout=5)

        last_message = await queue.get(timeout=5)
        last_message.ack()

        # close channel, not acked message should be redelivered
        await channel.close()

        channel = await self.create_channel(connection)
        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel,
        )
        queue = await declare_queue(
            queue_name, auto_delete=False, channel=channel,
        )

        # receive not acked message
        message = await queue.get(timeout=5)
        assert message.body == first_message.body
        message.ack()

        await queue.unbind(exchange, routing_key)

    async def test_ack_multiple(
        self,
        connection,
        declare_exchange,
        declare_queue,
        add_cleanup: Callable,
    ):
        queue_name = get_random_name("test_connection")
        routing_key = get_random_name()

        channel = await self.create_channel(connection)
        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel,
        )
        queue = await declare_queue(
            queue_name, auto_delete=False, cleanup=False, channel=channel,
        )

        await queue.bind(exchange, routing_key)

        # publish 2 messages
        for _ in range(2):
            body = bytes(shortuuid.uuid(), "utf-8")
            msg = Message(body)
            await exchange.publish(msg, routing_key)

        if not channel._publisher_confirms:
            await asyncio.sleep(1)

        # ack only last mesage with multiple flag, first
        # message should be acked too
        await queue.get(timeout=5)
        last_message = await queue.get(timeout=5)
        last_message.ack(multiple=True)

        # close channel, no messages should be redelivered
        await channel.close()

        channel = await self.create_channel(connection)
        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel,
        )
        queue = await declare_queue(
            queue_name, auto_delete=False, cleanup=False, channel=channel,
        )

        with pytest.raises(aio_pika.exceptions.QueueEmpty):
            await queue.get()

        await queue.unbind(exchange, routing_key)
        await queue.delete()

    async def test_ack_twice(
        self, channel: aio_pika.Connection, declare_queue, declare_exchange
    ):
        queue_name = get_random_name("test_connection")
        routing_key = get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)

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
        queue_name = get_random_name("test_connection")
        routing_key = get_random_name()

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
        queue_name = get_random_name("tc2")
        routing_key = get_random_name()

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
        loop,
        channel: aio_pika.Channel,
        declare_exchange: Callable,
        declare_queue: Callable,
        add_cleanup: Callable,
    ):

        queue_name = get_random_name("tc2")
        routing_key = get_random_name()

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
        declare_exchange: Callable,
        declare_queue: Callable,
        add_cleanup: Callable,
    ):
        queue_name = get_random_name("test_connection3")
        routing_key = get_random_name()

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

    async def test_purge_queue(
        self,
        declare_queue: Callable,
        declare_exchange: Callable,
        channel: aio_pika.Channel,
    ):
        queue_name = get_random_name("test_connection4")
        routing_key = get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)

        try:
            body = bytes(shortuuid.uuid(), "utf-8")

            await exchange.publish(
                Message(
                    body, content_type="text/plain", headers={"foo": "bar"},
                ),
                routing_key,
            )

            await queue.purge()

            with pytest.raises(asyncio.TimeoutError):
                await queue.get(timeout=1)
        except aio_pika.exceptions.QueueEmpty:
            await queue.unbind(exchange, routing_key)
            await queue.delete()

    async def test_connection_refused(self, connection_fabric: Callable):
        with pytest.raises(ConnectionError):
            await connection_fabric("amqp://guest:guest@localhost:9999")

    async def test_wrong_credentials(
        self, connection_fabric: Callable, amqp_url
    ):
        amqp_url = amqp_url.with_user(uuid.uuid4().hex).with_password(
            uuid.uuid4().hex,
        )

        with pytest.raises(ProbableAuthenticationError):
            await connection_fabric(amqp_url)

    async def test_set_qos(self, channel: aio_pika.Channel):
        await channel.set_qos(prefetch_count=1, global_=True)

    async def test_exchange_delete(
        self, channel: aio_pika.Channel, declare_exchange
    ):
        exchange = await declare_exchange("test", auto_delete=True)
        await exchange.delete()

    async def test_dlx(
        self,
        loop,
        channel: aio_pika.Channel,
        declare_exchange: Callable,
        declare_queue: Callable,
        add_cleanup: Callable,
    ):
        suffix = get_random_name()
        routing_key = "%s_routing_key" % suffix
        dlx_routing_key = "%s_dlx_routing_key" % suffix

        f = loop.create_future()

        async def dlx_handle(message):
            message.ack()
            assert message.body == body
            assert message.routing_key == dlx_routing_key
            f.set_result(True)

        direct_exchange = await declare_exchange(
            "direct", channel=channel, auto_delete=True,
        )  # type: aio_pika.Exchange

        dlx_exchange = await declare_exchange(
            "dlx", ExchangeType.DIRECT, auto_delete=True,
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
            "%s_dlx_queue" % suffix, auto_delete=True,
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

    async def test_expiration(
        self, channel: aio_pika.Channel, loop, declare_exchange, declare_queue
    ):

        dlx_queue = await declare_queue(
            get_random_name("test_dlx"), cleanup=False,
        )  # type: aio_pika.Queue

        dlx_exchange = await declare_exchange(
            get_random_name("dlx"), cleanup=False,
        )  # type: aio_pika.Exchange

        await dlx_queue.bind(dlx_exchange, routing_key=dlx_queue.name)

        queue = await declare_queue(
            get_random_name("test_expiration"),
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

    async def test_add_close_callback(self, create_connection: Callable):
        connection = await create_connection()

        shared_list = []

        def share(*a, **kw):
            shared_list.append((a, kw))

        connection.add_close_callback(share)

        del share
        assert len(connection.close_callbacks) == 1

        await connection.close()

        assert len(shared_list) == 1

    async def test_big_message(
        self,
        channel: aio_pika.Channel,
        add_cleanup: Callable,
        declare_queue,
        declare_exchange,
    ):
        queue_name = get_random_name("test_big")
        routing_key = get_random_name()

        exchange = await declare_exchange("direct", auto_delete=True)
        queue = await declare_queue(queue_name, auto_delete=True)

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

    async def test_unexpected_channel_close(
        self, channel: aio_pika.Channel, declare_queue
    ):
        with pytest.raises(aio_pika.exceptions.ChannelClosed):
            await declare_queue("amq.restricted_queue_name", auto_delete=True)

        with pytest.raises(aiormq.exceptions.ChannelInvalidStateError):
            await channel.set_qos(100)

    async def test_declaration_result(
        self, channel: aio_pika.Channel, declare_queue
    ):
        queue = await declare_queue(auto_delete=True)
        assert queue.declaration_result.message_count == 0
        assert queue.declaration_result.consumer_count == 0

    async def test_declaration_result_with_consumers(
        self, connection, declare_queue
    ):
        channel1 = await self.create_channel(connection)
        channel2 = await self.create_channel(connection)

        queue_name = get_random_name("queue", "declaration-result")
        queue1 = await declare_queue(
            queue_name, auto_delete=True, channel=channel1,
        )
        await queue1.consume(print)

        queue2 = await declare_queue(
            queue_name, passive=True, channel=channel2, cleanup=False,
        )

        assert queue2.declaration_result.consumer_count == 1

    async def test_declaration_result_with_messages(
        self, connection, declare_queue, declare_exchange
    ):
        channel1 = await self.create_channel(connection)
        channel2 = await self.create_channel(connection)

        queue_name = get_random_name("queue", "declaration-result")
        queue1 = await declare_queue(
            queue_name, auto_delete=True, channel=channel1,
        )

        await channel1.default_exchange.publish(
            Message(body=b"test"), routing_key=queue1.name,
        )

        await asyncio.sleep(1)

        queue2 = await declare_queue(
            queue_name, passive=True, channel=channel2,
        )
        await queue2.get()
        await queue2.delete()

        assert queue2.declaration_result.consumer_count == 0
        assert queue2.declaration_result.message_count == 1

    async def test_queue_empty_exception(
        self, channel: aio_pika.Channel, add_cleanup: Callable, declare_queue
    ):
        queue_name = get_random_name("test_get_on_empty_queue")

        queue = await declare_queue(queue_name, auto_delete=True)
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

    async def test_queue_empty_fail_false(
        self, channel: aio_pika.Channel, declare_queue
    ):
        queue_name = get_random_name("test_get_on_empty_queue")
        queue = await declare_queue(queue_name, auto_delete=True)

        result = await queue.get(fail=False)
        assert result is None

    async def test_message_nack(
        self, channel: aio_pika.Channel, declare_queue
    ):
        queue_name = get_random_name("test_nack_queue")
        body = uuid.uuid4().bytes
        queue = await declare_queue(queue_name, auto_delete=True)

        await channel.default_exchange.publish(
            Message(body=body), routing_key=queue_name,
        )

        message = await queue.get()  # type: aio_pika.IncomingMessage

        assert message.body == body
        message.nack(requeue=True)

        message = await queue.get()

        assert message.redelivered
        assert message.body == body
        await message.ack()

    async def test_on_return_raises(self, connection: aio_pika.Connection):
        queue_name = get_random_name("test_on_return_raises")
        body = uuid.uuid4().bytes

        with pytest.raises(RuntimeError):
            await connection.channel(
                publisher_confirms=False, on_return_raises=True,
            )

        channel = await connection.channel(
            publisher_confirms=True, on_return_raises=True,
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

    async def test_transaction_simple_rollback(
        self, connection: aio_pika.Connection
    ):
        async with connection.channel(publisher_confirms=False) as channel:
            tx = channel.transaction()
            await tx.select()
            await tx.rollback()

    async def test_transaction_simple_async_commit(
        self, connection: aio_pika.Connection
    ):
        async with connection.channel(publisher_confirms=False) as channel:
            async with channel.transaction():
                pass

    async def test_transaction_simple_async_rollback(
        self, connection: aio_pika.Connection
    ):
        async with connection.channel(publisher_confirms=False) as channel:
            with pytest.raises(ValueError):
                async with channel.transaction():
                    raise ValueError

    async def test_async_for_queue(self, loop, connection, declare_queue):
        channel2 = await self.create_channel(connection)

        queue = await declare_queue(
            get_random_name("queue", "is_async", "for"),
            auto_delete=True,
            channel=channel2,
        )

        messages = 100

        async def publisher():
            channel1 = await self.create_channel(connection)

            for i in range(messages):
                await channel1.default_exchange.publish(
                    Message(body=str(i).encode()), routing_key=queue.name,
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

    async def test_async_for_queue_context(
        self, loop, connection, declare_queue
    ):
        channel2 = await self.create_channel(connection)

        queue = await declare_queue(
            get_random_name("queue", "is_async", "for"),
            auto_delete=True,
            channel=channel2,
        )

        messages = 100

        async def publisher():
            channel1 = await self.create_channel(connection)

            for i in range(messages):
                await channel1.default_exchange.publish(
                    Message(body=str(i).encode()), routing_key=queue.name,
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

    async def test_async_with_connection(
        self, create_connection: Callable, connection, loop, declare_queue
    ):
        async with await create_connection() as connection:

            channel2 = await self.create_channel(connection)

            queue = await declare_queue(
                get_random_name("queue", "is_async", "for"),
                auto_delete=True,
                channel=channel2,
            )

            messages = 100

            async def publisher():
                channel1 = await self.create_channel(connection)

                for i in range(messages):
                    await channel1.default_exchange.publish(
                        Message(body=str(i).encode()), routing_key=queue.name,
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
                map(lambda x: str(x).encode(), range(messages)),
            )

        assert channel2.is_closed

    async def test_async_with_channel(self, connection: aio_pika.Connection):
        async with self.create_channel(connection) as channel:
            assert isinstance(channel, Channel)

        assert channel.is_closed

    async def test_delivery_fail(
        self, channel: aio_pika.Channel, declare_queue
    ):
        queue = await declare_queue(
            exclusive=True,
            arguments={"x-max-length": 1, "x-overflow": "reject-publish"},
            auto_delete=True,
        )

        await channel.default_exchange.publish(
            aio_pika.Message(body=b"queue me"), routing_key=queue.name,
        )

        with pytest.raises(DeliveryError):
            for _ in range(10):
                await channel.default_exchange.publish(
                    aio_pika.Message(body=b"reject me"), routing_key=queue.name,
                )

    async def test_channel_locked_resource(
        self, connection, declare_queue, add_cleanup: Callable,
    ):
        ch1 = await self.create_channel(connection)
        ch2 = await self.create_channel(connection)

        qname = get_random_name("channel", "locked", "resource")

        q1 = await declare_queue(
            qname, exclusive=True, channel=ch1, cleanup=False,
        )
        add_cleanup(q1.delete)

        tag = await q1.consume(print, exclusive=True)
        add_cleanup(q1.cancel, tag)

        with pytest.raises(aiormq.exceptions.ChannelAccessRefused):
            q2 = await declare_queue(
                qname, exclusive=True, channel=ch2, cleanup=False,
            )
            await q2.consume(print, exclusive=True)

    async def test_queue_iterator_close_was_called_twice(
        self, create_connection: Callable, loop, declare_queue
    ):
        future = loop.create_future()
        event = asyncio.Event()

        queue_name = get_random_name()

        async def task_inner():
            nonlocal future
            nonlocal event
            nonlocal create_connection

            try:
                connection = await create_connection()

                async with connection:
                    channel = await self.create_channel(connection)

                    queue = await declare_queue(
                        queue_name, channel=channel, cleanup=False,
                    )

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

    async def test_queue_iterator_close_with_noack(
        self,
        create_connection: Callable,
        loop,
        add_cleanup: Callable,
        declare_queue,
    ):
        messages = []
        queue_name = get_random_name("test_queue")
        body = get_random_name("test_body").encode()

        async def task_inner():
            nonlocal messages
            nonlocal create_connection
            nonlocal add_cleanup

            connection = await create_connection()
            add_cleanup(connection.close)

            async with connection:
                channel = await self.create_channel(connection)

                queue = await declare_queue(
                    queue_name, channel=channel, cleanup=False, passive=True,
                )

                async with queue.iterator(no_ack=True) as q:
                    async for message in q:
                        messages.append(message)
                        return

        async with await create_connection() as connection:
            channel = await self.create_channel(connection)

            queue = await declare_queue(
                queue_name, channel=channel, cleanup=False,
            )

            try:
                await channel.default_exchange.publish(
                    Message(body), routing_key=queue_name,
                )

                task = loop.create_task(task_inner())

                await task

                assert messages
                assert messages[0].body == body

            finally:
                await queue.delete()

    async def test_passive_for_exchange(
        self, declare_exchange: Callable, connection, add_cleanup: Callable,
    ):
        name = get_random_name("passive", "exchange")

        ch1 = await self.create_channel(connection)
        ch2 = await self.create_channel(connection)
        ch3 = await self.create_channel(connection)

        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            await declare_exchange(name, passive=True, channel=ch1)

        exchange = await declare_exchange(name, auto_delete=True, channel=ch2)
        exchange_passive = await declare_exchange(
            name, passive=True, channel=ch3,
        )

        assert exchange.name == exchange_passive.name

    async def test_passive_queue(
        self, declare_queue: Callable, connection: aio_pika.Connection
    ):
        name = get_random_name("passive", "queue")

        ch1 = await self.create_channel(connection)
        ch2 = await self.create_channel(connection)
        ch3 = await self.create_channel(connection)

        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            await declare_queue(name, passive=True, channel=ch1)

        queue = await declare_queue(name, auto_delete=True, channel=ch2)
        queue_passive = await declare_queue(name, passive=True, channel=ch3)

        assert queue.name == queue_passive.name

    async def test_get_exchange(self, connection, declare_exchange):
        channel = await self.create_channel(connection)
        name = get_random_name("passive", "exchange")

        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            await channel.get_exchange(name)

        channel = await self.create_channel(connection)
        exchange = await declare_exchange(
            name, auto_delete=True, channel=channel,
        )
        exchange_passive = await channel.get_exchange(name)

        assert exchange.name == exchange_passive.name

    async def test_get_queue(self, connection, declare_queue):
        channel = await self.create_channel(connection)
        name = get_random_name("passive", "queue")

        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            await channel.get_queue(name)

        channel = await self.create_channel(connection)
        queue = await declare_queue(name, auto_delete=True, channel=channel)
        queue_passive = await channel.get_queue(name)

        assert queue.name, queue_passive.name

    async def test_channel_blocking_timeout(self, connection):
        channel = await connection.channel()
        close_reasons = []
        close_event = asyncio.Event()

        def on_done(*args):
            close_reasons.append(args)
            close_event.set()
            return

        channel.add_close_callback(on_done)

        async def run():
            await channel.set_qos(1)
            time.sleep(1)
            await channel.set_qos(0)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(run(), timeout=0.2)

        await close_event.wait()

        with pytest.raises(RuntimeError):
            await channel.channel.closing

        assert channel.is_closed

        # Ensure close callback has been called
        assert close_reasons

        with pytest.raises(RuntimeError):
            await channel.set_qos(10)


class TestCaseAmqpNoConfirms(TestCaseAmqp):
    @staticmethod
    def create_channel(connection: aio_pika.Connection):
        return connection.channel(publisher_confirms=False)


class TestCaseAmqpWithConfirms(TestCaseAmqpBase):
    @staticmethod
    def create_channel(connection: aio_pika.Connection):
        return connection.channel(publisher_confirms=True)

    async def test_connection_close(
        self, connection: aio_pika.Connection, declare_exchange: Callable
    ):
        routing_key = get_random_name()

        channel = await self.create_channel(connection)
        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel,
        )

        try:
            with pytest.raises(aio_pika.exceptions.ChannelPreconditionFailed):
                msg = Message(bytes(shortuuid.uuid(), "utf-8"))
                msg.delivery_mode = 8

                await exchange.publish(msg, routing_key)

            channel = await self.create_channel(connection)
            exchange = await declare_exchange(
                "direct", auto_delete=True, channel=channel,
            )
        finally:
            await exchange.delete()

    async def test_basic_return(self, connection: aio_pika.connection, loop):
        channel = await self.create_channel(
            connection,
        )  # type: aio_pika.Channel

        f = loop.create_future()

        def handler(sender, *args, **kwargs):
            f.set_result(*args, **kwargs)

        channel.add_on_return_callback(handler)

        body = bytes(shortuuid.uuid(), "utf-8")

        await channel.default_exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            get_random_name("test_basic_return"),
        )

        returned = await f

        assert returned.body == body

        # handler with exception
        f = loop.create_future()

        await channel.close()

        channel = await self.create_channel(
            connection,
        )  # type: aio_pika.Channel

        def bad_handler(sender, message):
            try:
                raise ValueError
            finally:
                f.set_result(message)

        channel.add_on_return_callback(bad_handler)

        body = bytes(shortuuid.uuid(), "utf-8")

        await channel.default_exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            get_random_name("test_basic_return"),
        )

        returned = await f

        assert returned.body == body
