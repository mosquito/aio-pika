import asyncio
import uuid

import pytest

from aio_pika import exceptions
from aio_pika.connector import AMQPConnector, DeliveredMessage
from pamqp import specification as spec
from tests import AsyncTestCase, AMQP_URL


pytestmark = pytest.mark.asyncio


class TestCase(AsyncTestCase):
    async def test_simple(self):
        connector = AMQPConnector(AMQP_URL, loop=self.loop)
        await connector.connect()

        self.assertIsNotNone(connector.reader)
        self.assertIsNotNone(connector.writer)

        channel1 = await connector.channel()
        await channel1.basic_qos(prefetch_count=1)

        self.assertIsNotNone(channel1.number)

        channel2 = await connector.channel(11)
        self.assertIsNotNone(channel2.number)

        await channel1.close()
        await channel2.close()

        channel = await connector.channel()

        queue = asyncio.Queue(loop=self.loop)

        deaclare_ok = await channel.queue_declare(auto_delete=True)
        await channel.basic_consume(deaclare_ok.queue, queue.put)
        await channel.basic_publish(b'foo', routing_key=deaclare_ok.queue)

        message = await queue.get()     # type: DeliveredMessage
        self.assertEqual(message.body, b'foo')

        await channel.add_return_callback(queue.put)

        await channel.basic_publish(
            b'bar', routing_key=deaclare_ok.queue + 'foo',
            mandatory=True,
        )

        message = await queue.get()     # type: DeliveredMessage

        self.assertEqual(
            message.delivery.routing_key,
            deaclare_ok.queue + 'foo',
        )

        self.assertEqual(message.body, b'bar')

        await channel.queue_delete(deaclare_ok.queue)

        await connector.close()
        self.assertIsNone(connector.reader)
        self.assertIsNone(connector.writer)

    async def test_bad_channel(self):
        connector = AMQPConnector(AMQP_URL, loop=self.loop)
        await connector.connect()

        channel = await connector.channel()

        with self.assertRaises(exceptions.ChannelClosed):
            await channel.queue_bind(
                uuid.uuid4().hex,
                uuid.uuid4().hex
            )

        channel = await connector.channel()
        self.assertIsNotNone(channel)
