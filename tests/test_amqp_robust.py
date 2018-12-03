import pytest

from aio_pika import connect_robust
from tests import AMQP_URL
from tests.test_amqp import TestCase as AMQPTestCase


pytestmark = pytest.mark.asyncio


class TestCase(AMQPTestCase):
    async def create_connection(self, cleanup=True):
        client = await connect_robust(str(AMQP_URL), loop=self.loop)

        if cleanup:
            self.addCleanup(client.close)

        return client

    async def test_set_qos(self):
        channel = await self.create_channel()
        await channel.set_qos(prefetch_count=1)
