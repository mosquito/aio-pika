import pytest

from aio_pika.connector import AMQPConnector
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
        await connector.basic_publish(channel, b'foo')

        await connector.close()
        self.assertIsNone(connector.reader)
        self.assertIsNone(connector.writer)
