import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Callable, Optional, List
from unittest import mock

import aiormq.exceptions
import pytest
import shortuuid
from yarl import URL

import aio_pika
import aio_pika.exceptions
from aio_pika import Channel, DeliveryMode, Message
from aio_pika.abc import (
    AbstractConnection, AbstractIncomingMessage, MessageInfo, AbstractQueue,
)
from aio_pika.exceptions import (
    DeliveryError, MessageProcessError, ProbableAuthenticationError,
)
from aio_pika.exchange import ExchangeType
from aio_pika.message import ReturnedMessage
from aio_pika.queue import QueueIterator
from tests import get_random_name


log = logging.getLogger(__name__)


class TestCaseAmqpBase:
    @staticmethod
    def create_channel(
        connection: aio_pika.Connection,
    ) -> aio_pika.abc.AbstractChannel:
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
    async def test_properties(
        self, event_loop, connection: aio_pika.Connection,
    ):
        assert not connection.is_closed

    async def test_channel_close(self, connection: aio_pika.Connection):
        event = asyncio.Event()

        closed = False

        def on_close(
            ch: Optional[aio_pika.abc.AbstractChannel],
            exc: Optional[BaseException] = None,
        ):
            nonlocal closed
            log.info("Close called")
            closed = True
            assert ch is not None
            assert ch.is_closed
            event.set()

        channel = await self.create_channel(connection)
        channel.close_callbacks.add(on_close)
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
        self, connection, declare_exchange, declare_queue,
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

    @pytest.mark.skip(reason="This was deprecated in AMQP 0-9-1")
    async def test_internal_exchange(
        self, channel: aio_pika.Channel, declare_exchange, declare_queue,
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
        self, connection, declare_exchange: Callable,
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
        await incoming_message.ack()

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
        await incoming_message.ack()

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
        await incoming_message.ack()

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
        await incoming_message.ack()

        assert incoming_message.body == body

        await queue.unbind(dest_exchange, routing_key)

    async def test_simple_publish_with_closed_channel(
        self,
        connection: aio_pika.Connection,
        declare_exchange: Callable,
        declare_queue: Callable,
    ):
        routing_key = get_random_name()

        channel = await connection.channel(publisher_confirms=False)

        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel,
        )

        await connection.close()

        body = bytes(shortuuid.uuid(), "utf-8")

        with pytest.raises(aiormq.exceptions.ChannelInvalidStateError):
            await exchange.publish(
                Message(
                    body,
                    content_type="text/plain",
                    headers={"foo": "bar"},
                ),
                routing_key,
            )

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

        info = MessageInfo(
            app_id="test",
            body_size=len(body),
            content_encoding="text",
            content_type="application/json",
            correlation_id="1",
            delivery_mode=DeliveryMode.PERSISTENT,
            expiration=1.5,
            headers={"foo": "bar"},
            message_id=shortuuid.uuid(),
            priority=0,
            reply_to="test",
            timestamp=datetime.fromtimestamp(int(time.time()), tz=timezone.utc),
            type="0",
            user_id="guest",
        )

        msg = Message(
            body=body,
            headers={"foo": "bar"},
            content_type="application/json",
            content_encoding="text",
            delivery_mode=DeliveryMode.PERSISTENT,
            priority=0,
            correlation_id="1",
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
        await incoming_message.ack()

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

        if not channel.publisher_confirms:
            await asyncio.sleep(1)

        incoming_message: AbstractIncomingMessage = await queue.get(timeout=5)

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
                await incoming_message.reject(requeue=True)

        assert incoming_message.locked

        incoming_message = await queue.get(timeout=5)

        async with incoming_message.process(ignore_processed=True):
            await incoming_message.reject(requeue=False)

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

        if not channel.publisher_confirms:
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

        if not channel.publisher_confirms:
            await asyncio.sleep(1)

        # ack 1 message out of 2
        first_message = await queue.get(timeout=5)

        last_message = await queue.get(timeout=5)
        await last_message.ack()

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
        await message.ack()

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

        if not channel.publisher_confirms:
            await asyncio.sleep(1)

        # ack only last mesage with multiple flag, first
        # message should be acked too
        await queue.get(timeout=5)
        last_message = await queue.get(timeout=5)
        await last_message.ack(multiple=True)

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
        self, channel: aio_pika.Connection, declare_queue, declare_exchange,
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
        await incoming_message.ack()

        with pytest.raises(MessageProcessError):
            await incoming_message.ack()

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
        await incoming_message.reject(requeue=False)

        with pytest.raises(MessageProcessError):
            await incoming_message.reject(requeue=False)

        assert incoming_message.body == body

    async def test_consuming(
        self,
        event_loop,
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

        f = event_loop.create_future()

        async def handle(message):
            await message.ack()
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
        event_loop,
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

        f = event_loop.create_future()

        async def handle(message):
            await message.ack()
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
            await incoming_message.ack()

        await exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            routing_key,
        )

        incoming_message = await queue.get(timeout=5)

        await incoming_message.reject()

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
        self, connection_fabric: Callable, amqp_url,
    ):
        amqp_url = amqp_url.with_user(uuid.uuid4().hex).with_password(
            uuid.uuid4().hex,
        )

        with pytest.raises(ProbableAuthenticationError):
            await connection_fabric(amqp_url)

    async def test_set_qos(self, channel: aio_pika.Channel):
        await channel.set_qos(prefetch_count=1, global_=True)

    async def test_set_qos_deprecated_all_channels(
        self, channel: aio_pika.Channel,
    ):
        with pytest.deprecated_call():
            await channel.set_qos(prefetch_count=1, all_channels=True)

    async def test_exchange_delete(
        self, channel: aio_pika.Channel, declare_exchange,
    ):
        exchange = await declare_exchange("test", auto_delete=True)
        await exchange.delete()

    async def test_dlx(
        self,
        event_loop,
        channel: aio_pika.Channel,
        declare_exchange: Callable,
        declare_queue: Callable,
        add_cleanup: Callable,
    ):
        suffix = get_random_name()
        routing_key = "%s_routing_key" % suffix
        dlx_routing_key = "%s_dlx_routing_key" % suffix

        f = event_loop.create_future()

        async def dlx_handle(message):
            await message.ack()
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
        self, channel: aio_pika.Channel, event_loop,
        declare_exchange, declare_queue,
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

        f = event_loop.create_future()

        await dlx_queue.consume(f.set_result, no_ack=True)

        message = await f

        assert message.body == body
        assert message.headers["x-death"][0]["original-expiration"] == "500"

    async def test_add_close_callback(self, create_connection: Callable):
        connection = await create_connection()

        shared_list = []

        def share(*a, **kw):
            shared_list.append((a, kw))

        connection.close_callbacks.add(share)

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
        await incoming_message.ack()

        assert incoming_message.body == body

    async def test_unexpected_channel_close(
        self, channel: aio_pika.Channel, declare_queue,
    ):
        with pytest.raises(aio_pika.exceptions.ChannelClosed):
            await declare_queue("amq.restricted_queue_name", auto_delete=True)

        with pytest.raises(aiormq.exceptions.ChannelInvalidStateError):
            await channel.set_qos(100)

    async def test_declaration_result(
        self, channel: aio_pika.Channel, declare_queue,
    ):
        queue = await declare_queue(auto_delete=True)
        assert queue.declaration_result.message_count == 0
        assert queue.declaration_result.consumer_count == 0

    async def test_declaration_result_with_consumers(
        self, connection, declare_queue,
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
        self, connection, declare_queue, declare_exchange,
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
        self, channel: aio_pika.Channel, add_cleanup: Callable, declare_queue,
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
        self, channel: aio_pika.Channel, declare_queue,
    ):
        queue_name = get_random_name("test_get_on_empty_queue")
        queue = await declare_queue(queue_name, auto_delete=True)

        result = await queue.get(fail=False)
        assert result is None

    async def test_message_nack(
        self, channel: aio_pika.Channel, declare_queue,
    ):
        queue_name = get_random_name("test_nack_queue")
        body = uuid.uuid4().bytes
        queue = await declare_queue(queue_name, auto_delete=True)

        await channel.default_exchange.publish(
            Message(body=body), routing_key=queue_name,
        )

        message = await queue.get()  # type: aio_pika.IncomingMessage

        assert message.body == body
        await message.nack(requeue=True)

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
        self, connection: aio_pika.Connection,
    ):
        async with connection.channel(publisher_confirms=True) as channel:
            with pytest.raises(RuntimeError):
                channel.transaction()

    async def test_transaction_simple_commit(
        self, connection: aio_pika.Connection,
    ):
        async with connection.channel(publisher_confirms=False) as channel:
            tx = channel.transaction()
            await tx.select()
            await tx.commit()

    async def test_transaction_simple_rollback(
        self, connection: aio_pika.Connection,
    ):
        async with connection.channel(publisher_confirms=False) as channel:
            tx = channel.transaction()
            await tx.select()
            await tx.rollback()

    async def test_transaction_simple_async_commit(
        self, connection: aio_pika.Connection,
    ):
        async with connection.channel(publisher_confirms=False) as channel:
            async with channel.transaction():
                pass

    async def test_transaction_simple_async_rollback(
        self, connection: aio_pika.Connection,
    ):
        async with connection.channel(publisher_confirms=False) as channel:
            with pytest.raises(ValueError):
                async with channel.transaction():
                    raise ValueError

    async def test_async_for_queue(
        self, event_loop, connection, declare_queue,
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

        event_loop.create_task(publisher())

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
        self, event_loop, connection, declare_queue,
    ) -> None:
        channel2 = await self.create_channel(connection)

        queue = await declare_queue(
            get_random_name("queue", "is_async", "for"),
            auto_delete=True,
            channel=channel2,
        )

        messages: asyncio.Queue[bytes] = asyncio.Queue(100)
        condition = asyncio.Condition()

        async def publisher() -> None:
            channel1 = await self.create_channel(connection)

            for i in range(messages.maxsize):
                body = str(i).encode()
                await messages.put(body)
                await channel1.default_exchange.publish(
                    Message(body=body), routing_key=queue.name,
                )

        async def consumer() -> None:
            async with queue.iterator() as queue_iterator:
                async for message in queue_iterator:
                    async with message.process():
                        async with condition:
                            data.append(message.body)
                            messages.task_done()
                            condition.notify()

        async def application_stop_request() -> None:
            async with condition:
                await condition.wait_for(messages.full)
            await messages.join()
            await asyncio.sleep(1)
            await connection.close()

        p = event_loop.create_task(publisher())
        c = event_loop.create_task(consumer())
        asr = event_loop.create_task(application_stop_request())

        data: List[bytes] = list()

        await asyncio.gather(p, c, asr)

        assert data == list(
            map(lambda x: str(x).encode(), range(messages.maxsize))
        )

    async def test_async_with_connection(
        self, create_connection: Callable,
        connection, event_loop, declare_queue,
    ):
        async with await create_connection() as connection:

            channel = await self.create_channel(connection)

            queue = await declare_queue(
                get_random_name("queue", "is_async", "for"),
                auto_delete=True,
                channel=channel,
            )

            condition = asyncio.Condition()
            messages: asyncio.Queue[bytes] = asyncio.Queue(
                100
            )

            async def publisher():
                channel1 = await self.create_channel(connection)

                for i in range(messages.maxsize):
                    body = str(i).encode()
                    await messages.put(body)
                    await channel1.default_exchange.publish(
                        Message(body=body), routing_key=queue.name,
                    )

            data = list()

            async def consume_loop():
                async with queue.iterator() as queue_iterator:
                    async for message in queue_iterator:
                        async with message.process():
                            async with condition:
                                data.append(message.body)
                                condition.notify()
                                messages.task_done()

            async def application_close_request():
                async with condition:
                    await condition.wait_for(messages.full)
                await messages.join()
                await asyncio.sleep(1)
                await connection.close()

            p = event_loop.create_task(publisher())
            cl = event_loop.create_task(consume_loop())
            acr = event_loop.create_task(application_close_request())

            await asyncio.gather(p, cl, acr)

            assert data == list(
                map(lambda x: str(x).encode(), range(messages.maxsize)),
            )

        assert channel.is_closed

    async def test_async_with_channel(self, connection: aio_pika.Connection):
        async with self.create_channel(connection) as channel:
            assert isinstance(channel, Channel)

        assert channel.is_closed

    async def test_delivery_fail(
        self, channel: aio_pika.Channel, declare_queue,
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
                    aio_pika.Message(body=b"reject me"),
                    routing_key=queue.name,
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
        self, create_connection: Callable, event_loop, declare_queue,
    ):
        event = asyncio.Event()

        queue_name = get_random_name()
        iterator: QueueIterator = None  # type: ignore[assignment]

        async def task_inner():
            nonlocal iterator
            connection = await create_connection()

            async with connection:
                channel = await self.create_channel(connection)

                queue = await declare_queue(
                    queue_name, channel=channel, cleanup=False,
                )

                async with queue.iterator() as iterator:
                    event.set()

                    async for message in iterator:
                        async with message.process():
                            pytest.fail("who sent this message?")

        task = event_loop.create_task(task_inner())

        await event.wait()
        await iterator.close()
        await task

    async def test_queue_iterator_close_with_noack(
        self,
        create_connection: Callable,
        event_loop,
        add_cleanup: Callable,
        declare_queue,
    ):
        messages: asyncio.Queue = asyncio.Queue()
        queue_name = get_random_name("test_queue")
        body = get_random_name("test_body").encode()

        async def task_inner():
            connection = await create_connection()
            add_cleanup(connection.close)

            async with connection:
                channel = await self.create_channel(connection)

                queue = await declare_queue(
                    queue_name, channel=channel, cleanup=False, passive=True,
                )

                async with queue.iterator(no_ack=True) as q:
                    async for message in q:
                        await messages.put(message)
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

                task = event_loop.create_task(task_inner())

                message = await messages.get()

                assert message
                assert message.body == body

            finally:
                await task
                await queue.delete()

    async def test_queue_iterator_throws_cancelled_error(
        self,
        create_connection: Callable,
        event_loop,
        add_cleanup: Callable,
        declare_queue,
    ):
        event_loop.set_debug(True)
        queue_name = get_random_name("test_queue")

        connection = await create_connection()

        async with connection:
            channel = await self.create_channel(connection)

            queue = await channel.declare_queue(
                queue_name,
            )

            iterator = queue.iterator()
            task = event_loop.create_task(iterator.__anext__())
            done, pending = await asyncio.wait({task}, timeout=1)
            assert not done
            task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await task

    async def test_queue_iterator_throws_timeout_error(
        self,
        create_connection: Callable,
        event_loop,
        add_cleanup: Callable,
        declare_queue,
    ):
        event_loop.set_debug(True)
        queue_name = get_random_name("test_queue")

        connection = await create_connection()

        async with connection:
            channel = await self.create_channel(connection)

            queue = await channel.declare_queue(
                queue_name,
            )

            iterator = queue.iterator(timeout=1)
            task = event_loop.create_task(iterator.__anext__())
            done, pending = await asyncio.wait({task}, timeout=5)
            assert done

            with pytest.raises(asyncio.TimeoutError):
                await task

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
        self, declare_queue: Callable, connection: aio_pika.Connection,
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

    @pytest.mark.skip(reason="temporary skip")
    async def test_channel_blocking_timeout(self, connection):
        channel = await connection.channel()
        close_reasons = []
        close_event = asyncio.Event()

        def on_done(*args):
            close_reasons.append(args)
            close_event.set()
            return

        channel.close_callbacks.add(on_done)

        async def run():
            await channel.set_qos(1)
            time.sleep(1)
            await channel.set_qos(0)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(run(), timeout=0.2)

        await close_event.wait()

        assert channel.is_closed

        # Ensure close callback has been called
        assert close_reasons

        with pytest.raises(RuntimeError):
            await channel.set_qos(10)

    async def test_heartbeat_disabling(
        self, event_loop, amqp_url: URL, connection_fabric,
    ):
        url = amqp_url.update_query(heartbeat=0)
        connection: AbstractConnection = await connection_fabric(url)

        transport = connection.transport
        assert transport
        heartbeat = transport.connection.connection_tune.heartbeat

        async with connection:
            assert heartbeat == 0

    async def test_non_acked_messages_are_redelivered_to_queue(
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

        queue: AbstractQueue = await declare_queue(
            queue_name, auto_delete=False, channel=channel,
        )

        await queue.bind(exchange, routing_key)

        # Publish 5 messages to queue
        all_bodies = []
        for _ in range(0, 5):
            body = bytes(shortuuid.uuid(), "utf-8")
            all_bodies.append(body)

            assert await exchange.publish(Message(body), routing_key)

        # Create a subscription but only process first message
        async with queue.iterator() as queue_iterator:
            first_message = await queue_iterator.__anext__()
            async with first_message.process():
                assert first_message.body == all_bodies[0]

        # Confirm other messages are still in queue
        for i in range(1, 5):
            incoming_message = await queue.get(timeout=5)
            await incoming_message.ack()

            assert incoming_message.body == all_bodies[i]

        # Check if the queue is now empty
        assert await queue.get(fail=False, timeout=.5) is None

        # Cleanup, delete the queue
        await queue.delete()

    async def test_regression_only_messages_cancelled_subscription_are_nacked(
        self,
        channel: aio_pika.Channel,
        declare_queue: Callable,
        declare_exchange: Callable,
    ):
        queue_name1 = get_random_name("test_queue")
        queue_name2 = get_random_name("test_queue")
        routing_key1 = get_random_name()
        routing_key2 = get_random_name()

        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel,
        )

        queue1: AbstractQueue = await declare_queue(
            queue_name1, auto_delete=False, channel=channel,
        )
        queue2: AbstractQueue = await declare_queue(
            queue_name2, auto_delete=False, channel=channel,
        )

        await queue1.bind(exchange, routing_key1)
        await queue2.bind(exchange, routing_key2)

        # Publish 5 messages to queue 1
        all_bodies1 = []
        for _ in range(0, 5):
            body = bytes(shortuuid.uuid(), "utf-8")
            all_bodies1.append(body)

            assert await exchange.publish(Message(body), routing_key1)

        # Publish 5 messages to queue 2
        all_bodies2 = []
        for _ in range(0, 5):
            body = bytes(shortuuid.uuid(), "utf-8")
            all_bodies2.append(body)

            assert await exchange.publish(Message(body), routing_key2)

        # Create a subscription to both queues but only process first message
        queue_iterator1 = await queue1.iterator().__aenter__()
        queue_iterator2 = await queue2.iterator().__aenter__()

        first_message1 = await queue_iterator1.__anext__()
        async with first_message1.process():
            assert first_message1.body == all_bodies1[0]

        first_message2 = await queue_iterator2.__anext__()
        async with first_message2.process():
            assert first_message2.body == all_bodies2[0]
        #  The order of exit here is important.
        #    Subscription to queue 1 is received first then to 2.
        #    Therefore, the delivery tags of subscription to queue 2 will be
        #    higher.
        #    So first we cancel the subscription to 2, to test if we
        #    accidentally also nacked the messages of queue 1. Then we cancel
        #    subscription to queue 1 to test.

        await queue_iterator2.__aexit__(None, None, None)
        # To test if the wrong messages are nacked by stopping subscription to
        # queue 2, we ack a message received from queue 1. If it was nacked,
        # RabbitMQ will throw an exception.
        second_message1 = await queue_iterator1.__anext__()
        async with second_message1.process():
            assert second_message1.body == all_bodies1[1]

        await queue_iterator1.__aexit__(None, None, None)

        # Confirm other messages are still in queue
        for i in range(2, 5):
            incoming_message = await queue1.get(timeout=5)
            await incoming_message.ack()
            assert incoming_message.body == all_bodies1[i]

        for i in range(1, 5):
            incoming_message = await queue2.get(timeout=5)
            await incoming_message.ack()
            assert incoming_message.body == all_bodies2[i]

        # Check if the queue is now empty
        assert await queue1.get(fail=False, timeout=.5) is None
        assert await queue2.get(fail=False, timeout=.5) is None

        # Cleanup, delete the queue
        await queue1.delete()
        await queue2.delete()


class TestCaseAmqpNoConfirms(TestCaseAmqp):
    @staticmethod
    def create_channel(connection: aio_pika.Connection):
        return connection.channel(publisher_confirms=False)


class TestCaseAmqpWithConfirms(TestCaseAmqpBase):
    @staticmethod
    def create_channel(connection: aio_pika.Connection):
        return connection.channel(publisher_confirms=True)

    @pytest.mark.skip(reason="Have to find another way to close connection")
    async def test_connection_close(
        self, connection: aio_pika.Connection, declare_exchange: Callable,
    ):
        routing_key = get_random_name()

        channel = await self.create_channel(connection)
        exchange = await declare_exchange(
            "direct", auto_delete=True, channel=channel,
        )

        try:
            with pytest.raises(aio_pika.exceptions.ChannelPreconditionFailed):
                msg = Message(bytes(shortuuid.uuid(), "utf-8"))
                msg.delivery_mode = 8       # type: ignore

                await exchange.publish(msg, routing_key)

            channel = await self.create_channel(connection)
            exchange = await declare_exchange(
                "direct", auto_delete=True, channel=channel,
            )
        finally:
            await exchange.delete()

    async def test_basic_return(
        self, connection: aio_pika.Connection, event_loop,
    ):
        channel = await self.create_channel(connection)

        f = event_loop.create_future()

        def handler(channel, message: ReturnedMessage):
            f.set_result(message)

        channel.return_callbacks.add(handler)

        body = bytes(shortuuid.uuid(), "utf-8")

        await channel.default_exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            get_random_name("test_basic_return"),
        )

        returned = await f

        assert returned.body == body

        # handler with exception
        f = event_loop.create_future()

        await channel.close()

        channel = await self.create_channel(connection)

        def bad_handler(
            channel: aio_pika.abc.AbstractChannel,
            message: aio_pika.message.IncomingMessage,
        ):
            try:
                raise ValueError
            finally:
                f.set_result(message)

        channel.return_callbacks.add(bad_handler)

        body = bytes(shortuuid.uuid(), "utf-8")

        await channel.default_exchange.publish(
            Message(body, content_type="text/plain", headers={"foo": "bar"}),
            get_random_name("test_basic_return"),
        )

        returned = await f

        assert returned.body == body
