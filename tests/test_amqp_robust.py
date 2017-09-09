import asyncio
import os
import uuid
import logging
import pytest
import shortuuid
import time
import unittest

from aio_pika.exceptions import ChannelClosed

import aio_pika
import aio_pika.exceptions
from copy import copy
from aio_pika import connect_robust, Message, DeliveryMode
from aio_pika.exceptions import ProbableAuthenticationError, MessageProcessError
from aio_pika.exchange import ExchangeType
from aio_pika.tools import wait, create_future
from unittest import mock
from . import AsyncTestCase, AMQP_URL, timeout

log = logging.getLogger(__name__)


class TestCase(AsyncTestCase):
    def get_random_name(self, *args):
        prefix = ['test']
        for item in args:
            prefix.append(item)
        prefix.append(shortuuid.uuid())

        return ".".join(prefix)

    @pytest.mark.asyncio
    @timeout()
    def test_channel_close(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        self.get_random_name("test_connection")
        self.get_random_name()

        self.__closed = False

        def on_close(ch):
            log.info("Close called")
            self.__closed = True

        channel = yield from client.channel()
        channel.add_close_callback(on_close)
        yield from channel.close()

        yield from asyncio.sleep(1, loop=self.loop)

        self.assertTrue(self.__closed)

        with self.assertRaises(RuntimeError):
            yield from channel.initialize()

        channel = yield from client.channel()
        yield from asyncio.wait_for(wait((channel.close(), channel.closing), loop=self.loop), timeout=3, loop=self.loop)

        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_delete_queue_and_exchange(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection")
        exchange = self.get_random_name()

        channel = yield from client.channel()
        yield from channel.declare_exchange(exchange, auto_delete=True)
        yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from channel.queue_delete(queue_name)
        yield from channel.exchange_delete(exchange)

        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_temporary_queue(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        channel = yield from client.channel()
        queue = yield from channel.declare_queue(auto_delete=True)

        self.assertNotEqual(queue.name, '')

        body = os.urandom(32)

        yield from channel.default_exchange.publish(Message(body=body), routing_key=queue.name)

        message = yield from queue.get()

        self.assertEqual(message.body, body)

        yield from channel.queue_delete(queue.name)

        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_internal_exchange(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        routing_key = self.get_random_name()
        exchange_name = self.get_random_name("internal", "exchange")

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange(exchange_name, auto_delete=True, internal=True)
        queue = yield from channel.declare_queue(auto_delete=True)

        yield from queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        with self.assertRaises(ValueError):
            f = exchange.publish(
                Message(
                    body, content_type='text/plain',
                    headers={'foo': 'bar'}
                ),
                routing_key
            )
            yield from f

        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_simple_publish_and_receive(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        result = yield from exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )
        self.assertTrue(result)

        incoming_message = yield from queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)
        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_simple_publish_without_confirm(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield from client.channel(publisher_confirms=False)
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        result = yield from exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )
        self.assertIsNone(result)

        incoming_message = yield from queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)
        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_simple_publish_and_receive_delivery_mode_explicitly_none(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield from exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'},
                delivery_mode=None
            ),
            routing_key
        )

        incoming_message = yield from queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)
        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_simple_publish_and_receive_to_bound_exchange(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        routing_key = self.get_random_name()
        src_name = self.get_random_name("source", "exchange")
        dest_name = self.get_random_name("destination", "exchange")

        channel = yield from client.channel()
        src_exchange = yield from channel.declare_exchange(src_name, auto_delete=True)
        dest_exchange = yield from channel.declare_exchange(dest_name, auto_delete=True)
        queue = yield from channel.declare_queue(auto_delete=True)

        yield from queue.bind(dest_exchange, routing_key)
        yield from dest_exchange.bind(src_exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield from src_exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = yield from queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)

        yield from dest_exchange.unbind(src_exchange, routing_key)
        yield from queue.unbind(dest_exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_incoming_message_info(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from queue.bind(exchange, routing_key)

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

        yield from exchange.publish(msg, routing_key)

        incoming_message = yield from queue.get(timeout=5)
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

        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_context_process(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield from exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = yield from queue.get(timeout=5)

        with self.assertRaises(AssertionError):
            with incoming_message.process(requeue=True):
                raise AssertionError

        self.assertEqual(incoming_message.locked, True)

        incoming_message = yield from queue.get(timeout=5)

        with incoming_message.process():
            pass

        self.assertEqual(incoming_message.body, body)

        yield from exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = yield from queue.get(timeout=5)

        with self.assertRaises(MessageProcessError):
            with incoming_message.process():
                incoming_message.reject(requeue=True)

        self.assertEqual(incoming_message.locked, True)

        incoming_message = yield from queue.get(timeout=5)

        with incoming_message.process(ignore_processed=True):
            incoming_message.reject(requeue=False)

        self.assertEqual(incoming_message.body, body)

        yield from exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = yield from queue.get(timeout=5)
        with self.assertRaises(AssertionError):
            with incoming_message.process(requeue=True, reject_on_redelivered=True):
                raise AssertionError

        incoming_message = yield from queue.get(timeout=5)
        with self.assertRaises(AssertionError):
            with incoming_message.process(requeue=True, reject_on_redelivered=True):
                raise AssertionError

        self.assertEqual(incoming_message.locked, True)

        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_context_process_redelivery(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield from exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = yield from queue.get(timeout=5)

        with self.assertRaises(AssertionError):
            with incoming_message.process(requeue=True, reject_on_redelivered=True):
                raise AssertionError

        incoming_message = yield from queue.get(timeout=5)

        with mock.patch('aio_pika.message.log') as message_logger:
            with self.assertRaises(Exception):
                with incoming_message.process(requeue=True, reject_on_redelivered=True):
                    raise Exception

            self.assertTrue(message_logger.info.called)
            self.assertEqual(message_logger.info.mock_calls[0][1][1].body, incoming_message.body)

        self.assertEqual(incoming_message.body, body)
        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_no_ack_redelivery(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=False)

        yield from queue.bind(exchange, routing_key)

        # publish 2 messages
        for _ in range(2):
            body = bytes(shortuuid.uuid(), 'utf-8')
            msg = Message(body)
            yield from exchange.publish(msg, routing_key)

        # ack 1 message out of 2
        first_message = yield from queue.get(timeout=5)

        last_message = yield from queue.get(timeout=5)
        last_message.ack()

        # close channel, not acked message should be redelivered
        yield from channel.close()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=False)

        # receive not acked message
        message = yield from queue.get(timeout=5)
        self.assertEqual(message.body, first_message.body)
        message.ack()

        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_ack_multiple(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=False)

        yield from queue.bind(exchange, routing_key)

        # publish 2 messages
        for _ in range(2):
            body = bytes(shortuuid.uuid(), 'utf-8')
            msg = Message(body)
            yield from exchange.publish(msg, routing_key)

        # ack only last mesage with multiple flag, first message should be acked too
        yield from queue.get(timeout=5)
        last_message = yield from queue.get(timeout=5)
        last_message.ack(multiple=True)

        # close channel, no messages should be redelivered
        yield from channel.close()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=False)

        with self.assertRaises(aio_pika.exceptions.QueueEmpty):
            yield from queue.get()

        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_ack_twice(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield from exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = yield from queue.get(timeout=5)
        incoming_message.ack()

        with self.assertRaises(MessageProcessError):
            incoming_message.ack()

        self.assertEqual(incoming_message.body, body)
        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_reject_twice(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield from exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = yield from queue.get(timeout=5)
        incoming_message.reject(requeue=False)

        with self.assertRaises(MessageProcessError):
            incoming_message.reject(requeue=False)

        self.assertEqual(incoming_message.body, body)
        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_consuming(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("tc2")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        f = asyncio.Future(loop=self.loop)

        @asyncio.coroutine
        def handle(message):
            message.ack()
            self.assertEqual(message.body, body)
            self.assertEqual(message.routing_key, routing_key)
            f.set_result(True)

        yield from queue.consume(handle)

        yield from exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        if not f.done():
            yield from f

        yield from queue.unbind(exchange, routing_key)
        yield from exchange.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_consuming_not_coroutine(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("tc2")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        f = asyncio.Future(loop=self.loop)

        def handle(message):
            message.ack()
            self.assertEqual(message.body, body)
            self.assertEqual(message.routing_key, routing_key)
            f.set_result(True)

        yield from queue.consume(handle)

        yield from exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        if not f.done():
            yield from f

        yield from queue.unbind(exchange, routing_key)
        yield from exchange.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_ack_reject(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection3")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield from exchange.publish(
            Message(
                body,
                content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = yield from queue.get(timeout=5, no_ack=True)

        with self.assertRaises(TypeError):
            incoming_message.ack()

        yield from exchange.publish(
            Message(
                body,
                content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = yield from queue.get(timeout=5)

        incoming_message.reject()

        yield from exchange.publish(
            Message(
                body,
                content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = yield from queue.get(timeout=5, no_ack=True)

        with self.assertRaises(TypeError):
            yield from incoming_message.reject()

        self.assertEqual(incoming_message.body, body)

        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_purge_queue(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection4")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from queue.bind(exchange, routing_key)

        try:
            body = bytes(shortuuid.uuid(), 'utf-8')

            yield from exchange.publish(
                Message(
                    body, content_type='text/plain',
                    headers={'foo': 'bar'}
                ),
                routing_key
            )

            yield from queue.purge()

            with self.assertRaises(TimeoutError):
                yield from queue.get(timeout=1)
        except:
            yield from queue.unbind(exchange, routing_key)
            yield from queue.delete()
            yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_wrong_credentials(self):
        amqp_url = AMQP_URL.with_user(uuid.uuid4().hex).with_password(uuid.uuid4().hex)

        with self.assertRaises(ProbableAuthenticationError):
            yield from connect_robust(
                amqp_url,
                loop=self.loop
            )

    @pytest.mark.asyncio
    @timeout()
    def test_set_qos(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        channel = yield from client.channel()
        yield from channel.set_qos(prefetch_count=1)
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_exchange_delete(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)
        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange("test", auto_delete=True)
        yield from exchange.delete()
        yield from client.close()

    @pytest.mark.asyncio
    def test_dlx(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)
        suffix = self.get_random_name()
        routing_key = "%s_routing_key" % suffix
        dlx_routing_key = "%s_dlx_routing_key" % suffix

        channel = yield from client.channel()

        f = asyncio.Future(loop=self.loop)

        @asyncio.coroutine
        def dlx_handle(message):
            message.ack()
            self.assertEqual(message.body, body)
            self.assertEqual(message.routing_key, dlx_routing_key)
            f.set_result(True)

        direct_exchange = yield from channel.declare_exchange('direct', auto_delete=True)   # type: aio_pika.Exchange
        dlx_exchange = yield from channel.declare_exchange('dlx', ExchangeType.DIRECT, auto_delete=True)

        direct_queue = yield from channel.declare_queue(
            "%s_direct_queue" % suffix,
            auto_delete=True,
            arguments={
                'x-message-ttl': 300,
                'x-dead-letter-exchange': 'dlx',
                'x-dead-letter-routing-key': dlx_routing_key
            }
        )

        dlx_queue = yield from channel.declare_queue(
            "%s_dlx_queue" % suffix,
            auto_delete=True
        )

        yield from dlx_queue.consume(dlx_handle)
        yield from dlx_queue.bind(dlx_exchange, dlx_routing_key)
        yield from direct_queue.bind(direct_exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        try:
            yield from direct_exchange.publish(
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
                yield from f
        finally:
            yield from dlx_queue.unbind(dlx_exchange, routing_key)
            yield from direct_queue.unbind(direct_exchange, routing_key)
            yield from direct_queue.delete()
            yield from direct_exchange.delete()
            yield from dlx_exchange.delete()
            yield from client.close()

    @pytest.mark.asyncio
    @timeout()
    def test_channel_close_invalid_message(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)  # type: aio_pika.connection.Connection

        routing_key = self.get_random_name()

        channel = yield from client.channel()    # type: aio_pika.Channel
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)

        try:
            with self.assertRaises(aio_pika.exceptions.ChannelClosed):
                msg = Message(bytes(shortuuid.uuid(), 'utf-8'))
                msg.delivery_mode = 8

                yield from exchange.publish(msg, routing_key)

            channel = yield from client.channel()
            exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        finally:
            yield from exchange.delete()
            yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_basic_return(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        channel = yield from client.channel()   # type: aio_pika.Channel

        f = asyncio.Future(loop=self.loop)

        channel.add_on_return_callback(f.set_result)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield from channel.default_exchange.publish(
            Message(
                body,
                content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            self.get_random_name("test_basic_return")
        )

        returned = yield from f

        self.assertEqual(returned.body, body)

        # handler with exception
        f = asyncio.Future(loop=self.loop)

        yield from channel.close()

        channel = yield from client.channel()  # type: aio_pika.Channel

        def bad_handler(message):
            try:
                raise ValueError
            finally:
                f.set_result(message)

        channel.add_on_return_callback(bad_handler)

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield from channel.default_exchange.publish(
            Message(
                body,
                content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            self.get_random_name("test_basic_return")
        )

        returned = yield from f

        self.assertEqual(returned.body, body)

        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_expiration(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        channel = yield from client.channel()  # type: aio_pika.Channel

        dlx_queue = yield from channel.declare_queue(
            self.get_random_name("test_dlx")
        )   # type: aio_pika.Queue

        dlx_exchange = yield from channel.declare_exchange(
            self.get_random_name("dlx"),
        )   # type: aio_pika.Exchange

        yield from dlx_queue.bind(dlx_exchange, routing_key=dlx_queue.name)

        queue = yield from channel.declare_queue(
            self.get_random_name("test_expiration"),
            arguments={
                "x-message-ttl": 10000,
                "x-dead-letter-exchange": dlx_exchange.name,
                "x-dead-letter-routing-key": dlx_queue.name,
            }
        )  # type: aio_pika.Queue

        body = bytes(shortuuid.uuid(), 'utf-8')

        yield from channel.default_exchange.publish(
            Message(
                body,
                content_type='text/plain',
                headers={'foo': 'bar'},
                expiration=0.5
            ),
            queue.name
        )

        f = asyncio.Future(loop=self.loop)

        yield from dlx_queue.consume(f.set_result, no_ack=True)

        message = yield from f

        self.assertEqual(message.body, body)
        self.assertEqual(message.headers['x-death'][0]['original-expiration'], '500')

        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    @timeout()
    def test_add_close_callback(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        f = create_future(loop=self.loop)

        client.add_close_callback(f.set_result)
        yield from client.close()

        self.assertTrue(f.done())

    @pytest.mark.asyncio
    @timeout()
    def test_big_message(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_big")
        routing_key = self.get_random_name()

        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from queue.bind(exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8') * 9999999

        yield from exchange.publish(
            Message(
                body, content_type='text/plain',
                headers={'foo': 'bar'}
            ),
            routing_key
        )

        incoming_message = yield from queue.get(timeout=5)
        incoming_message.ack()

        self.assertEqual(incoming_message.body, body)
        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    def test_unexpected_channel_close(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        channel = yield from client.channel()

        with self.assertRaises(ChannelClosed):
            yield from channel.declare_queue("amq.restricted_queue_name", auto_delete=True)

        yield from wait((client.close(), client.closing), loop=self.loop)

    def test_declaration_result(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        channel = yield from client.channel()

        queue = yield from channel.declare_queue(auto_delete=True)

        self.assertEqual(queue.declaration_result.message_count, 0)
        self.assertEqual(queue.declaration_result.consumer_count, 0)

        yield from wait((client.close(), client.closing), loop=self.loop)

    def test_declaration_result_with_consumers(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        channel1 = yield from client.channel()

        queue_name = self.get_random_name("queue", "declaration-result")
        queue1 = yield from channel1.declare_queue(queue_name, auto_delete=True)
        yield from queue1.consume(print)

        channel2 = yield from client.channel()

        queue2 = yield from channel2.declare_queue(queue_name, passive=True)

        self.assertEqual(queue2.declaration_result.consumer_count, 1)

        yield from wait((client.close(), client.closing), loop=self.loop)

    def test_declaration_result_with_messages(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)

        channel1 = yield from client.channel()
        channel2 = yield from client.channel()

        queue_name = self.get_random_name("queue", "declaration-result")
        queue1 = yield from channel1.declare_queue(queue_name, auto_delete=True)

        yield from channel1.default_exchange.publish(
            Message(body=b'test'),
            routing_key=queue1.name
        )

        queue2 = yield from channel2.declare_queue(queue_name, passive=True)
        yield from queue2.get()
        yield from queue2.delete()

        self.assertEqual(queue2.declaration_result.consumer_count, 0)
        self.assertEqual(queue2.declaration_result.message_count, 1)

        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    def test_queue_empty_exception(self):

        client = yield from connect_robust(AMQP_URL, loop=self.loop)
        queue_name = self.get_random_name("test_get_on_empty_queue")
        channel = yield from client.channel()
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        with self.assertRaises(aio_pika.exceptions.QueueEmpty):
            yield from queue.get(timeout=5)

        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    def test_queue_empty_fail_false(self):

        client = yield from connect_robust(AMQP_URL, loop=self.loop)
        queue_name = self.get_random_name("test_get_on_empty_queue")
        channel = yield from client.channel()
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        result = yield from queue.get(fail=False)
        self.assertIsNone(result)

        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    def test_message_nack(self):

        client = yield from connect(AMQP_URL, loop=self.loop)
        queue_name = self.get_random_name("test_nack_queue")
        body = uuid.uuid4().bytes
        channel = yield from client.channel()
        queue = yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from channel.default_exchange.publish(Message(body=body), routing_key=queue_name)

        message = yield from queue.get()    # type: aio_pika.IncomingMessage

        self.assertEqual(message.body, body)
        message.nack(requeue=True)

        message = yield from queue.get()

        self.assertTrue(message.redelivered)
        self.assertEqual(message.body, body)
        message.ack()

        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)


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
