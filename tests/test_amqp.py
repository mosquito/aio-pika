import asyncio
import logging
import os
import time
import unittest
import uuid
from copy import copy
from unittest import mock

import pytest
import shortuuid

import aio_pika
import aio_pika.exceptions
from aio_pika import connect, Message, DeliveryMode, Channel
from aio_pika.exceptions import (
    MessageProcessError, ProbableAuthenticationError
)
from aio_pika.exchange import ExchangeType
from aio_pika.tools import wait
from . import BaseTestCase, AMQP_URL


log = logging.getLogger(__name__)
pytestmark = pytest.mark.asyncio


class TestCase(BaseTestCase):
    async def test_channel_close(self):
        client = await self.create_connection()

        self.get_random_name("test_connection")
        self.get_random_name()

        self.__closed = False

        def on_close(ch):
            log.info("Close called")
            self.__closed = True

        channel = await client.channel()
        channel.add_close_callback(on_close)
        await channel.close()

        await asyncio.sleep(0.5, loop=self.loop)

        self.assertTrue(self.__closed)

        with pytest.raises(RuntimeError):
            await channel.initialize()

        await self.create_channel(connection=client)

    async def test_delete_queue_and_exchange(self):
        queue_name = self.get_random_name("test_connection")
        exchange = self.get_random_name()

        channel = await self.create_channel()
        await channel.declare_exchange(exchange, auto_delete=True)
        await channel.declare_queue(queue_name, auto_delete=True)

        await channel.queue_delete(queue_name)
        await channel.exchange_delete(exchange)

    async def test_temporary_queue(self):
        channel = await self.create_channel()
        queue = await channel.declare_queue(auto_delete=True)

        self.assertNotEqual(queue.name, '')

        body = os.urandom(32)

        await channel.default_exchange.publish(
            Message(body=body),
            routing_key=queue.name
        )

        message = await queue.get()

        self.assertEqual(message.body, body)

        await channel.queue_delete(queue.name)

    async def test_internal_exchange(self):
        client = await self.create_connection()

        routing_key = self.get_random_name()
        exchange_name = self.get_random_name("internal", "exchange")

        channel = await client.channel()
        exchange = await self.declare_exchange(
            exchange_name,
            auto_delete=True,
            internal=True,
            channel=channel
        )
        queue = await self.declare_queue(auto_delete=True, channel=channel)

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        with pytest.raises(ValueError):
            f = exchange.publish(
                Message(
                    body, content_type='text/plain',
                    headers={'foo': 'bar'}
                ),
                routing_key
            )
            await f

        await queue.unbind(exchange, routing_key)

    async def test_declare_exchange_with_passive_flag(self):
        client = await self.create_connection()

        exchange_name = self.get_random_name()
        channel = await client.channel()

        with pytest.raises(aio_pika.exceptions.ChannelClosed):
            await self.declare_exchange(
                exchange_name,
                auto_delete=True,
                passive=True,
                channel=channel
            )

        channel1 = await client.channel()
        channel2 = await client.channel()

        await self.declare_exchange(
            exchange_name,
            auto_delete=True,
            passive=False,
            channel=channel1
        )

        # Check ignoring different exchange options
        await self.declare_exchange(
            exchange_name,
            auto_delete=False,
            passive=True,
            channel=channel2
        )

    async def test_simple_publish_and_receive(self):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await self.create_channel()
        exchange = await self.declare_exchange(
            'direct', auto_delete=True, channel=channel
        )
        queue = await self.declare_queue(
            queue_name, auto_delete=True, channel=channel
        )

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        result = await exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )
        self.assertTrue(result)

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)

        await queue.unbind(exchange, routing_key)

    async def test_simple_publish_without_confirm(self):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await self.create_channel(publisher_confirms=False)
        exchange = await self.declare_exchange(
            'direct', auto_delete=True, channel=channel
        )
        queue = await self.declare_queue(
            queue_name, auto_delete=True, channel=channel
        )

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        result = await exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )
        self.assertIsNone(result)

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)

        await queue.unbind(exchange, routing_key)

    async def test_simple_publish_and_receive_delivery_mode_explicitly(self):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await self.create_channel()
        exchange = await self.declare_exchange(
            'direct', auto_delete=True, channel=channel
        )
        queue = await self.declare_queue(
            queue_name, auto_delete=True, channel=channel
        )

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        await exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'},
                delivery_mode=None
            ),
            routing_key
        )

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)

        await queue.unbind(exchange, routing_key)

    async def test_simple_publish_and_receive_to_bound_exchange(self):
        routing_key = self.get_random_name()
        src_name = self.get_random_name("source", "exchange")
        dest_name = self.get_random_name("destination", "exchange")

        channel = await self.create_channel()
        src_exchange = await self.declare_exchange(
            src_name, auto_delete=True, channel=channel
        )
        dest_exchange = await self.declare_exchange(
            dest_name, auto_delete=True, channel=channel
        )
        queue = await self.declare_queue(auto_delete=True, channel=channel)

        await queue.bind(dest_exchange, routing_key)

        await dest_exchange.bind(src_exchange, routing_key)
        self.addCleanup(dest_exchange.unbind, src_exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        await src_exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)

        await queue.unbind(dest_exchange, routing_key)

    async def test_incoming_message_info(self):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await self.create_channel()
        exchange = await self.declare_exchange(
            'direct', auto_delete=True, channel=channel
        )
        queue = await self.declare_queue(
            queue_name, auto_delete=True, channel=channel
        )

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        self.maxDiff = None

        info = {
            'headers': {"foo": "bar"},
            'content_type': "application/json",
            'content_encoding': "text",
            'delivery_mode': DeliveryMode.PERSISTENT.value,
            'priority': 0,
            'correlation_id': b'1',
            'reply_to': 'test',
            'expiration': 1.5,
            'message_id': shortuuid.uuid(),
            'timestamp': int(time.time()),
            'type': '0',
            'user_id': 'guest',
            'app_id': 'test',
            'body_size': len(body)
        }

        msg = Message(
            body=body,
            headers={'foo': 'bar'},
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

        await exchange.publish(msg, routing_key)

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        info['synchronous'] = incoming_message.synchronous
        info['routing_key'] = incoming_message.routing_key
        info['redelivered'] = incoming_message.redelivered
        info['exchange'] = incoming_message.exchange
        info['delivery_tag'] = incoming_message.delivery_tag
        info['consumer_tag'] = incoming_message.consumer_tag
        info['cluster_id'] = incoming_message.cluster_id

        self.assertEqual(incoming_message.body, body)
        self.assertDictEqual(incoming_message.info(), info)

        await queue.unbind(exchange, routing_key)

    async def test_context_process(self):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await self.create_channel()
        exchange = await self.declare_exchange(
            'direct', auto_delete=True, channel=channel
        )
        queue = await self.declare_queue(
            queue_name, auto_delete=True, channel=channel
        )

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        await exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = await queue.get(timeout=5)

        with pytest.raises(AssertionError):
            with incoming_message.process(requeue=True):
                raise AssertionError

        self.assertEqual(incoming_message.locked, True)

        incoming_message = await queue.get(timeout=5)

        with incoming_message.process():
            pass

        self.assertEqual(incoming_message.body, body)

        await exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = await queue.get(timeout=5)

        with pytest.raises(MessageProcessError):
            with incoming_message.process():
                incoming_message.reject(requeue=True)

        self.assertEqual(incoming_message.locked, True)

        incoming_message = await queue.get(timeout=5)

        with incoming_message.process(ignore_processed=True):
            incoming_message.reject(requeue=False)

        self.assertEqual(incoming_message.body, body)

        await exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = await queue.get(timeout=5)
        with pytest.raises(AssertionError):
            with incoming_message.process(
                requeue=True, reject_on_redelivered=True
            ):
                raise AssertionError

        incoming_message = await queue.get(timeout=5)
        with pytest.raises(AssertionError):
            with incoming_message.process(
                requeue=True, reject_on_redelivered=True
            ):
                raise AssertionError

        self.assertEqual(incoming_message.locked, True)

        await queue.unbind(exchange, routing_key)

    async def test_context_process_redelivery(self):
        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await self.create_channel()
        exchange = await self.declare_exchange(
            'direct', auto_delete=True, channel=channel
        )
        queue = await self.declare_queue(
            queue_name, auto_delete=True, channel=channel
        )

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        await exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = await queue.get(timeout=5)

        with pytest.raises(AssertionError):
            with incoming_message.process(
                requeue=True, reject_on_redelivered=True
            ):
                raise AssertionError

        incoming_message = await queue.get(timeout=5)

        with mock.patch('aio_pika.message.log') as message_logger:
            with pytest.raises(Exception):
                with incoming_message.process(
                    requeue=True, reject_on_redelivered=True
                ):
                    raise Exception

            self.assertTrue(message_logger.info.called)
            self.assertEqual(
                message_logger.info.mock_calls[0][1][1].body,
                incoming_message.body
            )

        self.assertEqual(incoming_message.body, body)

        await queue.unbind(exchange, routing_key)

    async def test_no_ack_redelivery(self):
        client = await self.create_connection()

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await client.channel()
        exchange = await channel.declare_exchange('direct', auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=False)

        await queue.bind(exchange, routing_key)

        # publish 2 messages
        for _ in range(2):
            body = bytes(shortuuid.uuid(), 'utf-8')
            msg = Message(body)
            await exchange.publish(msg, routing_key)

        # ack 1 message out of 2
        first_message = await queue.get(timeout=5)

        last_message = await queue.get(timeout=5)
        last_message.ack()

        # close channel, not acked message should be redelivered
        await channel.close()

        channel = await client.channel()
        exchange = await channel.declare_exchange('direct', auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=False)

        # receive not acked message
        message = await queue.get(timeout=5)
        self.assertEqual(message.body, first_message.body)
        message.ack()

        await queue.unbind(exchange, routing_key)

    async def test_ack_multiple(self):
        client = await self.create_connection()

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await client.channel()
        exchange = await channel.declare_exchange('direct', auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=False)

        await queue.bind(exchange, routing_key)

        # publish 2 messages
        for _ in range(2):
            body = bytes(shortuuid.uuid(), 'utf-8')
            msg = Message(body)
            await exchange.publish(msg, routing_key)

        # ack only last mesage with multiple flag, first
        # message should be acked too
        await queue.get(timeout=5)
        last_message = await queue.get(timeout=5)
        last_message.ack(multiple=True)

        # close channel, no messages should be redelivered
        await channel.close()

        channel = await client.channel()
        exchange = await channel.declare_exchange('direct', auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=False)

        with pytest.raises(aio_pika.exceptions.QueueEmpty):
            await queue.get()

        await queue.unbind(exchange, routing_key)
        await queue.delete()
        await wait((client.close(), client.closing), loop=self.loop)

    async def test_ack_twice(self):
        client = await self.create_connection()

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await client.channel()
        exchange = await channel.declare_exchange('direct', auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        await exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        with pytest.raises(MessageProcessError):
            incoming_message.ack()

        self.assertEqual(incoming_message.body, body)
        await queue.unbind(exchange, routing_key)
        await queue.delete()
        await wait((client.close(), client.closing), loop=self.loop)

    async def test_reject_twice(self):
        client = await self.create_connection()

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = await client.channel()
        exchange = await channel.declare_exchange('direct', auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        await exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = await queue.get(timeout=5)
        incoming_message.reject(requeue=False)

        with pytest.raises(MessageProcessError):
            incoming_message.reject(requeue=False)

        self.assertEqual(incoming_message.body, body)
        await queue.unbind(exchange, routing_key)
        await queue.delete()
        await wait((client.close(), client.closing), loop=self.loop)

    async def test_consuming(self):
        client = await self.create_connection()

        queue_name = self.get_random_name("tc2")
        routing_key = self.get_random_name()

        channel = await client.channel()
        exchange = await channel.declare_exchange('direct', auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        f = asyncio.Future(loop=self.loop)

        async def handle(message):
            message.ack()
            self.assertEqual(message.body, body)
            self.assertEqual(message.routing_key, routing_key)
            f.set_result(True)

        await queue.consume(handle)

        await exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        if not f.done():
            await f

        await queue.unbind(exchange, routing_key)
        await exchange.delete()
        await wait((client.close(), client.closing), loop=self.loop)

    async def test_consuming_not_coroutine(self):
        client = await self.create_connection()

        queue_name = self.get_random_name("tc2")
        routing_key = self.get_random_name()

        channel = await client.channel()
        exchange = await channel.declare_exchange('direct', auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        f = asyncio.Future(loop=self.loop)

        def handle(message):
            message.ack()
            self.assertEqual(message.body, body)
            self.assertEqual(message.routing_key, routing_key)
            f.set_result(True)

        await queue.consume(handle)

        await exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        if not f.done():
            await f

        await queue.unbind(exchange, routing_key)
        await exchange.delete()
        await wait((client.close(), client.closing), loop=self.loop)

    async def test_ack_reject(self):
        client = await self.create_connection()

        queue_name = self.get_random_name("test_connection3")
        routing_key = self.get_random_name()

        channel = await client.channel()
        exchange = await channel.declare_exchange('direct', auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        await exchange.publish(
            Message(
                body,
                content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = await queue.get(timeout=5, no_ack=True)

        with pytest.raises(TypeError):
            incoming_message.ack()

        await exchange.publish(
            Message(
                body,
                content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = await queue.get(timeout=5)

        incoming_message.reject()

        await exchange.publish(
            Message(
                body,
                content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = await queue.get(timeout=5, no_ack=True)

        with pytest.raises(TypeError):
            await incoming_message.reject()

        self.assertEqual(incoming_message.body, body)

        await queue.unbind(exchange, routing_key)
        await queue.delete()
        await wait((client.close(), client.closing), loop=self.loop)

    async def test_purge_queue(self):
        queue_name = self.get_random_name("test_connection4")
        routing_key = self.get_random_name()

        channel = await self.create_channel()
        exchange = await channel.declare_exchange('direct', auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)

        try:
            body = bytes(shortuuid.uuid(), 'utf-8')

            await exchange.publish(
                Message(
                    body, content_type='text/plain',
                    headers={'foo': 'bar'}
                ),
                routing_key
            )

            await queue.purge()

            with pytest.raises(asyncio.TimeoutError):
                await queue.get(timeout=1)
        except aio_pika.exceptions.QueueEmpty:
            await queue.unbind(exchange, routing_key)
            await queue.delete()

    async def test_connection_refused(self):
        with pytest.raises(ConnectionRefusedError):
            await connect('amqp://guest:guest@localhost:9999', loop=self.loop)

    async def test_wrong_credentials(self):
        amqp_url = AMQP_URL.with_user(
            uuid.uuid4().hex
        ).with_password(
            uuid.uuid4().hex
        )

        with pytest.raises(ProbableAuthenticationError):
            await connect(str(amqp_url), loop=self.loop)

    async def test_set_qos(self):
        channel = await self.create_channel()
        await channel.set_qos(prefetch_count=1, all_channels=True)

    async def test_exchange_delete(self):
        channel = await self.create_channel()
        exchange = await channel.declare_exchange("test", auto_delete=True)
        await exchange.delete()

    async def test_dlx(self):
        suffix = self.get_random_name()
        routing_key = "%s_routing_key" % suffix
        dlx_routing_key = "%s_dlx_routing_key" % suffix

        channel = await self.create_channel()

        f = asyncio.Future(loop=self.loop)

        async def dlx_handle(message):
            message.ack()
            self.assertEqual(message.body, body)
            self.assertEqual(message.routing_key, dlx_routing_key)
            f.set_result(True)

        direct_exchange = await self.declare_exchange(
            'direct', channel=channel, auto_delete=True
        )  # type: aio_pika.Exchange

        dlx_exchange = await channel.declare_exchange(
            'dlx', ExchangeType.DIRECT, auto_delete=True
        )

        direct_queue = await channel.declare_queue(
            "%s_direct_queue" % suffix,
            auto_delete=True,
            arguments={
                'x-message-ttl': 300,
                'x-dead-letter-exchange': 'dlx',
                'x-dead-letter-routing-key': dlx_routing_key
            }
        )

        dlx_queue = await channel.declare_queue(
            "%s_dlx_queue" % suffix,
            auto_delete=True
        )

        await dlx_queue.consume(dlx_handle)
        await dlx_queue.bind(dlx_exchange, dlx_routing_key)
        await direct_queue.bind(direct_exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        try:
            await direct_exchange.publish(
                Message(
                    body,
                    content_type='text/plain',
                    headers={
                        'x-message-ttl': 100,
                        'x-dead-letter-exchange': 'dlx',
                    }
                ),
                routing_key
            )

            if not f.done():
                await f
        finally:
            await dlx_queue.unbind(dlx_exchange, routing_key)
            await direct_queue.unbind(direct_exchange, routing_key)
            await direct_queue.delete()
            await direct_exchange.delete()
            await dlx_exchange.delete()

    async def test_connection_close(self):
        client = await self.create_connection()

        routing_key = self.get_random_name()

        channel = await client.channel()    # type: aio_pika.Channel
        exchange = await channel.declare_exchange('direct', auto_delete=True)

        try:
            with pytest.raises(aio_pika.exceptions.ChannelClosed):
                msg = Message(bytes(shortuuid.uuid(), 'utf-8'))
                msg.delivery_mode = 8

                await exchange.publish(msg, routing_key)

            channel = await client.channel()
            exchange = await channel.declare_exchange(
                'direct', auto_delete=True
            )
        finally:
            await exchange.delete()
            await wait((client.close(), client.closing), loop=self.loop)

    async def test_basic_return(self):
        client = await self.create_connection()

        channel = await client.channel()   # type: aio_pika.Channel

        f = asyncio.Future(loop=self.loop)

        channel.add_on_return_callback(f.set_result)

        body = bytes(shortuuid.uuid(), 'utf-8')

        await channel.default_exchange.publish(
            Message(
                body,
                content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            self.get_random_name("test_basic_return")
        )

        returned = await f

        self.assertEqual(returned.body, body)

        # handler with exception
        f = asyncio.Future(loop=self.loop)

        await channel.close()

        channel = await client.channel()  # type: aio_pika.Channel

        def bad_handler(message):
            try:
                raise ValueError
            finally:
                f.set_result(message)

        channel.add_on_return_callback(bad_handler)

        body = bytes(shortuuid.uuid(), 'utf-8')

        await channel.default_exchange.publish(
            Message(
                body,
                content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            self.get_random_name("test_basic_return")
        )

        returned = await f

        self.assertEqual(returned.body, body)

        await wait((client.close(), client.closing), loop=self.loop)

    async def test_expiration(self):
        client = await self.create_connection()

        channel = await client.channel()  # type: aio_pika.Channel

        dlx_queue = await channel.declare_queue(
            self.get_random_name("test_dlx")
        )   # type: aio_pika.Queue

        dlx_exchange = await channel.declare_exchange(
            self.get_random_name("dlx"),
        )   # type: aio_pika.Exchange

        await dlx_queue.bind(dlx_exchange, routing_key=dlx_queue.name)

        queue = await channel.declare_queue(
            self.get_random_name("test_expiration"),
            arguments={
                "x-message-ttl": 10000,
                "x-dead-letter-exchange": dlx_exchange.name,
                "x-dead-letter-routing-key": dlx_queue.name,
            }
        )  # type: aio_pika.Queue

        body = bytes(shortuuid.uuid(), 'utf-8')

        await channel.default_exchange.publish(
            Message(
                body,
                content_type='text/plain',
                headers={'foo': 'bar'},
                expiration=0.5
            ),
            queue.name
        )

        f = asyncio.Future(loop=self.loop)

        await dlx_queue.consume(f.set_result, no_ack=True)

        message = await f

        self.assertEqual(message.body, body)
        self.assertEqual(
            message.headers['x-death'][0]['original-expiration'], '500'
        )

        await wait((client.close(), client.closing), loop=self.loop)

    async def test_add_close_callback(self):
        client = await self.create_connection()

        shared_list = []

        def share(f):
            shared_list.append(f)

        client.add_close_callback(share)
        await client.close()

        self.assertEqual(len(shared_list), 1)

    async def test_big_message(self):
        client = await self.create_connection()

        queue_name = self.get_random_name("test_big")
        routing_key = self.get_random_name()

        channel = await client.channel()
        exchange = await channel.declare_exchange('direct', auto_delete=True)
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        await queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8') * 9999999

        await exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = await queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)
        await queue.unbind(exchange, routing_key)
        await queue.delete()
        await wait((client.close(), client.closing), loop=self.loop)

    async def test_unexpected_channel_close(self):
        client = await self.create_connection()

        channel = await client.channel()

        with pytest.raises(aio_pika.exceptions.ChannelClosed):
            await channel.declare_queue("amq.restricted_queue_name",
                                        auto_delete=True)

        await wait((client.close(), client.closing), loop=self.loop)

    async def test_declaration_result(self):
        client = await self.create_connection()

        channel = await client.channel()

        queue = await channel.declare_queue(auto_delete=True)

        self.assertEqual(queue.declaration_result.message_count, 0)
        self.assertEqual(queue.declaration_result.consumer_count, 0)

        await wait((client.close(), client.closing), loop=self.loop)

    async def test_declaration_result_with_consumers(self):
        client = await self.create_connection()

        channel1 = await client.channel()

        queue_name = self.get_random_name("queue", "declaration-result")
        queue1 = await channel1.declare_queue(queue_name, auto_delete=True)
        await queue1.consume(print)

        channel2 = await client.channel()

        queue2 = await channel2.declare_queue(queue_name, passive=True)

        self.assertEqual(queue2.declaration_result.consumer_count, 1)

        await wait((client.close(), client.closing), loop=self.loop)

    async def test_declaration_result_with_messages(self):
        client = await self.create_connection()

        channel1 = await client.channel()
        channel2 = await client.channel()

        queue_name = self.get_random_name("queue", "declaration-result")
        queue1 = await channel1.declare_queue(queue_name, auto_delete=True)

        await channel1.default_exchange.publish(
            Message(body=b'test'),
            routing_key=queue1.name
        )

        queue2 = await channel2.declare_queue(queue_name, passive=True)
        await queue2.get()
        await queue2.delete()

        self.assertEqual(queue2.declaration_result.consumer_count, 0)
        self.assertEqual(queue2.declaration_result.message_count, 1)

        await wait((client.close(), client.closing), loop=self.loop)

    async def test_queue_empty_exception(self):

        client = await self.create_connection()
        queue_name = self.get_random_name("test_get_on_empty_queue")
        channel = await client.channel()
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        with pytest.raises(aio_pika.exceptions.QueueEmpty):
            await queue.get(timeout=5)

        await channel.default_exchange.publish(
            Message(b'test'),
            queue_name,
        )

        message = await queue.get(timeout=5)
        self.assertEqual(message.body, b'test')

        # test again for #110
        with pytest.raises(aio_pika.exceptions.QueueEmpty):
            await queue.get(timeout=5)

        await queue.delete()
        await wait((client.close(), client.closing), loop=self.loop)

    async def test_queue_empty_fail_false(self):

        client = await self.create_connection()
        queue_name = self.get_random_name("test_get_on_empty_queue")
        channel = await client.channel()
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        result = await queue.get(fail=False)
        self.assertIsNone(result)

        await queue.delete()
        await wait((client.close(), client.closing), loop=self.loop)

    async def test_message_nack(self):

        client = await self.create_connection()
        queue_name = self.get_random_name("test_nack_queue")
        body = uuid.uuid4().bytes
        channel = await client.channel()
        queue = await channel.declare_queue(queue_name, auto_delete=True)

        await channel.default_exchange.publish(
            Message(body=body), routing_key=queue_name
        )

        message = await queue.get()    # type: aio_pika.IncomingMessage

        self.assertEqual(message.body, body)
        message.nack(requeue=True)

        message = await queue.get()

        self.assertTrue(message.redelivered)
        self.assertEqual(message.body, body)
        message.ack()

        await queue.delete()
        await wait((client.close(), client.closing), loop=self.loop)

    async def test_on_return_raises(self):
        client = await self.create_connection()
        queue_name = self.get_random_name("test_on_return_raises")
        body = uuid.uuid4().bytes

        with pytest.raises(RuntimeError):
            await client.channel(
                publisher_confirms=False, on_return_raises=True
            )

        channel = await client.channel(
            publisher_confirms=True, on_return_raises=True
        )

        for _ in range(100):
            with pytest.raises(aio_pika.exceptions.UnroutableError):
                await channel.default_exchange.publish(
                    Message(body=body), routing_key=queue_name,
                )

        await client.close()

    async def test_transaction_when_publisher_confirms_error(self):
        channel = await self.create_channel(publisher_confirms=True)
        with pytest.raises(RuntimeError):
            channel.transaction()

    async def test_transaction_simple_commit(self):
        channel = await self.create_channel(publisher_confirms=False)
        tx = channel.transaction()
        await tx.select()
        await tx.commit()

    async def test_transaction_simple_rollback(self):
        channel = await self.create_channel(publisher_confirms=False)
        tx = channel.transaction()
        await tx.select()
        await tx.rollback()

    async def test_transaction_simple_async_commit(self):
        channel = await self.create_channel(publisher_confirms=False)

        async with channel.transaction():
            pass

    async def test_transaction_simple_async_rollback(self):
        channel = await self.create_channel(publisher_confirms=False)

        with pytest.raises(ValueError):
            async with channel.transaction():
                raise ValueError

    async def test_async_for_queue(self):
        conn = await self.create_connection()

        channel2 = await self.create_channel(connection=conn)

        queue = await channel2.declare_queue(
            self.get_random_name("queue", "is_async", "for"), auto_delete=True)

        messages = 100

        async def publisher():
            channel1 = await self.create_channel(connection=conn)

            for i in range(messages):
                await channel1.default_exchange.publish(
                    Message(body=str(i).encode()), routing_key=queue.name)

        self.loop.create_task(publisher())

        count = 0
        data = list()

        async for message in queue:
            with message.process():
                count += 1
                data.append(message.body)

            if count >= messages:
                break

        self.assertSequenceEqual(data, list(
            map(lambda x: str(x).encode(), range(messages))))

    async def test_async_for_queue_context(self):
        conn = await self.create_connection()

        channel2 = await self.create_channel(connection=conn)

        queue = await channel2.declare_queue(
            self.get_random_name("queue", "is_async", "for"), auto_delete=True)

        messages = 100

        async def publisher():
            channel1 = await self.create_channel(connection=conn)

            for i in range(messages):
                await channel1.default_exchange.publish(
                    Message(body=str(i).encode()), routing_key=queue.name)

        self.loop.create_task(publisher())

        count = 0
        data = list()

        async with queue.iterator() as queue_iterator:
            async for message in queue_iterator:
                with message.process():
                    count += 1
                    data.append(message.body)

                if count >= messages:
                    break

        self.assertSequenceEqual(data, list(
            map(lambda x: str(x).encode(), range(messages))))

    async def test_async_with_connection(self):
        conn = await self.create_connection(cleanup=False)

        async with conn:

            channel2 = await self.create_channel(
                connection=conn, cleanup=False
            )

            queue = await channel2.declare_queue(
                self.get_random_name("queue", "is_async", "for"),
                auto_delete=True)

            messages = 100

            async def publisher():
                channel1 = await self.create_channel(connection=conn,
                                                     cleanup=False)

                for i in range(messages):
                    await channel1.default_exchange.publish(
                        Message(body=str(i).encode()),
                        routing_key=queue.name
                    )

            self.loop.create_task(publisher())

            count = 0
            data = list()

            async with queue.iterator() as queue_iterator:
                async for message in queue_iterator:
                    with message.process():
                        count += 1
                        data.append(message.body)

                    if count >= messages:
                        break

            self.assertSequenceEqual(data, list(
                map(lambda x: str(x).encode(), range(messages))))
        self.assertTrue(channel2.is_closed)

    async def test_async_with_channel(self):
        conn = await self.create_connection()

        async with conn.channel() as channel:
            self.assertTrue(isinstance(channel, Channel))

        self.assertTrue(channel.is_closed)


class MessageTestCase(unittest.TestCase):
    def test_message_copy(self):
        msg1 = Message(bytes(shortuuid.uuid(), 'utf-8'))
        msg2 = copy(msg1)

        msg1.lock()

        self.assertFalse(msg2.locked)

    def test_message_info(self):
        body = bytes(shortuuid.uuid(), 'utf-8')

        info = {
            'headers': {"foo": "bar"},
            'content_type': "application/json",
            'content_encoding': "text",
            'delivery_mode': DeliveryMode.PERSISTENT.value,
            'priority': 0,
            'correlation_id': b'1',
            'reply_to': 'test',
            'expiration': 1.5,
            'message_id': shortuuid.uuid(),
            'timestamp': int(time.time()),
            'type': '0',
            'user_id': 'guest',
            'app_id': 'test',
            'body_size': len(body)
        }

        msg = Message(
            body=body,
            headers={'foo': 'bar'},
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

        self.assertDictEqual(info, msg.info())

@pytest.mark.parametrize('url,kwargs,exp', [
    ('amqps://', {}, {'ssl': True}),
    ('localhost', {'ssl': True}, {'ssl': True}),
    ('localhost', {'ssl_options': {'ssl_version': '2'}}, {'ssl': True}),
    ('localhost?ssl_version=2', {}, {'ssl': True}),
])
async def test_connection_url_params(url, kwargs, exp):
    connection_mock = mock.MagicMock()
    await connect(url, connection_class=connection_mock, **kwargs)
    _, mock_kwargs = connection_mock.call_args
    for item in exp.items():
        assert item in mock_kwargs.items()
