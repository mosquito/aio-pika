import asyncio
import uuid
import logging
import pytest
import shortuuid
import aio_pika.exceptions
from copy import copy
from aio_pika import connect, connect_url, Message, Connection, Channel, Exchange
from aio_pika.exceptions import ProbableAuthenticationError, MessageProcessError
from aio_pika.exchange import ExchangeType
from aio_pika.tools import wait
from . import AsyncTestCase, AMQP_URL


log = logging.getLogger(__name__)


class TestCase(AsyncTestCase):
    def get_random_name(self, *args):
        prefix = ['test']
        for item in args:
            prefix.append(item)
        prefix.append(shortuuid.uuid())

        return ".".join(prefix)

    @pytest.mark.asyncio
    def test_connection_url_deprecated(self):
        with self.assertWarns(DeprecationWarning):
            yield from connect_url(AMQP_URL, loop=self.loop)

    @pytest.mark.asyncio
    def test_channel_close(self):
        client = yield from connect(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection")
        routing_key = self.get_random_name()


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

        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    def test_delete_queue_and_exchange(self):
        client = yield from connect(AMQP_URL, loop=self.loop)

        queue_name = self.get_random_name("test_connection")
        exchange = self.get_random_name()

        channel = yield from client.channel()
        yield from channel.declare_exchange(exchange, auto_delete=True)
        yield from channel.declare_queue(queue_name, auto_delete=True)

        yield from channel.queue_delete(queue_name)
        yield from channel.exchange_delete(exchange)

        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    def test_simple_publish_and_receive(self):
        client = yield from connect(AMQP_URL, loop=self.loop)

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

        self.assertEqual(incoming_message.body, body)
        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    def test_context_process(self):
        client = yield from connect(AMQP_URL, loop=self.loop)

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
            with incoming_message.proccess(requeue=True) as msg:
                raise AssertionError

        incoming_message = yield from queue.get(timeout=5)

        with incoming_message.proccess() as msg:
            pass

        self.assertEqual(incoming_message.body, body)
        yield from queue.unbind(exchange, routing_key)
        yield from queue.delete()
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    def test_ack_twice(self):
        client = yield from connect(AMQP_URL, loop=self.loop)

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
    def test_reject_twice(self):
        client = yield from connect(AMQP_URL, loop=self.loop)

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
    def test_consuming(self):
        client = yield from connect(AMQP_URL, loop=self.loop)

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

        queue.consume(handle)

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
    def test_ack_reject(self):
        client = yield from connect(AMQP_URL, loop=self.loop)

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

        self.assertFalse(incoming_message.ack())

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
    def test_purge_queue(self):
        client = yield from connect(AMQP_URL, loop=self.loop)

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
    def test_connection_refused(self):
        with self.assertRaises(ConnectionRefusedError):
            yield from connect('amqp://guest:guest@localhost:9999', loop=self.loop)

    @pytest.mark.asyncio
    def test_wrong_credentials(self):
        amqp_url = copy(AMQP_URL)
        amqp_url.user = uuid.uuid4().hex
        amqp_url.password = uuid.uuid4().hex

        with self.assertRaises(ProbableAuthenticationError):
            yield from connect(
                amqp_url,
                loop=self.loop
            )

    @pytest.mark.asyncio
    def test_set_qos(self):
        client = yield from connect(AMQP_URL, loop=self.loop)

        channel = yield from client.channel()
        yield from channel.set_qos(prefetch_count=1, all_channels=True)
        yield from wait((client.close(), client.closing), loop=self.loop)

    @pytest.mark.asyncio
    def test_exchange_delete(self):
        client = yield from connect(AMQP_URL, loop=self.loop)
        channel = yield from client.channel()
        exchange = yield from channel.declare_exchange("test", auto_delete=True)
        yield from exchange.delete()
        yield from client.close()

    @pytest.mark.asyncio
    def test_dlx(self):
        client = yield from connect(AMQP_URL, loop=self.loop)    # type: Connection
        direct_queue_name = self.get_random_name("test_dlx", "direct")
        dlx_queue_name = self.get_random_name("test_dlx", "dlx")

        routing_key = self.get_random_name()

        channel = yield from client.channel()

        direct_exchange = yield from channel.declare_exchange('direct', auto_delete=True)
        direct_queue = yield from channel.declare_queue(
            direct_queue_name,
            auto_delete=True,
            arguments={
                'x-message-ttl': 300,
                'x-dead-letter-exchange': 'dlx',
                'x-dead-letter-routing-key': routing_key
            }
        )
        direct_queue.bind(direct_exchange, routing_key)

        @asyncio.coroutine
        def dlx_handle(message):
            message.ack()
            self.assertEqual(message.body, body)
            self.assertEqual(message.routing_key, routing_key)
            f.set_result(True)

        dlx_exchange = yield from channel.declare_exchange('dlx', ExchangeType.DIRECT, auto_delete=True)
        dlx_queue = yield from channel.declare_queue(dlx_queue_name, auto_delete=True)
        dlx_queue.consume(dlx_handle)
        yield from dlx_queue.bind(dlx_exchange, routing_key)

        body = bytes(shortuuid.uuid(), 'utf-8')

        try:
            f = asyncio.Future(loop=self.loop)

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
    def test_connection_close(self):
        client = yield from connect(AMQP_URL, loop=self.loop)  # type: Connection

        routing_key = self.get_random_name()

        channel = yield from client.channel()    # type: Channel
        exchange = yield from channel.declare_exchange('direct', auto_delete=True)    # type: Exchange

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
