import asyncio

import pytest

from aio_pika.connector import AMQPConnector, DeliveredMessage
from tests import AsyncTestCase, AMQP_URL


pytestmark = pytest.mark.asyncio


class TestCase(AsyncTestCase):
    async def test_simple(self):
        connector = AMQPConnector(AMQP_URL, loop=self.loop)
        await connector.connect()

        self.assertIsNotNone(connector.reader)
        self.assertIsNotNone(connector.writer)

        channel_number, frame = await connector.channel_open()
        await connector.basic_qos(channel=channel_number, prefetch_count=1)

        self.assertIsNotNone(channel_number)

        await connector.channel_open(11)
        self.assertIsNotNone(channel_number)

        await connector.channel_close(channel_number)
        await connector.channel_close(11)

        channel, frame = await connector.channel_open()

        queue = asyncio.Queue(loop=self.loop)

        deaclare_ok = await connector.queue_declare(channel, auto_delete=True)
        await connector.basic_consume(channel, deaclare_ok.queue, queue.put)

        await connector.basic_publish(
            channel, b'foo',
            routing_key=deaclare_ok.queue,
        )

        message = await queue.get()     # type: DeliveredMessage
        self.assertEqual(message.body, b'foo')

        await connector.add_return_callback(queue.put)

        await connector.basic_publish(
            channel, b'bar',
            routing_key=deaclare_ok.queue + 'foo',
            mandatory=True,
        )

        message = await queue.get()     # type: DeliveredMessage

        self.assertEqual(
            message.delivery.routing_key,
            deaclare_ok.queue + 'foo',
        )

        self.assertEqual(message.body, b'bar')

        await connector.queue_delete(channel, deaclare_ok.queue)

        await connector.close()
        self.assertIsNone(connector.reader)
        self.assertIsNone(connector.writer)
