import pytest

from aio_pika import connect_robust
from tests import AMQP_URL
from tests.test_amqp_without_publisher_confirms import TestCase as AMQPTestCase


class TestCase(AMQPTestCase):
    async def create_connection(self, cleanup=True):
        client = await connect_robust(str(AMQP_URL), loop=self.loop)

        if cleanup:
            self.addCleanup(client.close)

        return client

    async def create_channel(self, connection=None, cleanup=False,
                             publisher_confirms=False, **kwargs):
        if connection is None:
            connection = await self.create_connection()

        channel = await connection.channel(
            publisher_confirms=publisher_confirms, **kwargs
        )

        if cleanup:
            self.addCleanup(channel.close)

        return channel

    @pytest.mark.asyncio
    async def test_set_qos(self):
        channel = await self.create_channel()
        await channel.set_qos(prefetch_count=1)
