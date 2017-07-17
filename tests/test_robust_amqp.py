import asyncio
import logging

import aio_pika
import aio_pika.exceptions
from aio_pika import Message
from aio_pika.robust_connection import connect
from . import AsyncTestCase, AMQP_URL


log = logging.getLogger(__name__)


class TestCase(AsyncTestCase):
    @asyncio.coroutine
    def create_client(self):
        client = yield from connect(AMQP_URL, loop=self.loop)

        @asyncio.coroutine
        def cleanup():
            nonlocal client
            yield from client.close()
            yield from client.closing

        self.addCleanup(cleanup)
        return client

    def test_simple_reconnect(self):
        client = yield from self.create_client()

        channel = yield from client.channel()
        queue = yield from channel.declare_queue()
        exchange = channel.default_exchange

        body = self.get_random_name('queue').encode()

        self.messages = list()

        def on_message(message: aio_pika.IncomingMessage):
            self.messages.append(message)

        queue.consume(on_message, no_ack=True)

        yield from exchange.publish(
            Message(body=body),
            routing_key=queue.name
        )

        client._connection.close(400, 'Test connection closing')

        with self.assertRaises(aio_pika.exceptions.ChannelClosed):
            yield from exchange.publish(
                Message(body=body),
                routing_key=queue.name
            )

        yield from exchange.publish(
            Message(body=body),
            routing_key=queue.name
        )

        self.assertEqual(len(self.messages), 2)

