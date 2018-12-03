import asyncio

import pytest

from aio_pika import connect_robust
from tests import AMQP_URL
from tests.test_amqp_without_publisher_confirms import TestCase as AMQPTestCase


pytestmark = pytest.mark.asyncio


class TestCase(AMQPTestCase):
    @asyncio.coroutine
    def create_connection(self, cleanup=True):
        client = yield from connect_robust(str(AMQP_URL), loop=self.loop)

        if cleanup:
            self.addCleanup(client.close)

        return client

    @asyncio.coroutine
    def create_channel(self, connection=None, cleanup=False,
                       publisher_confirms=False, **kwargs):
        if connection is None:
            connection = yield from self.create_connection()

        channel = yield from connection.channel(
            publisher_confirms=publisher_confirms, **kwargs
        )

        if cleanup:
            self.addCleanup(channel.close)

        return channel

    @pytest.mark.asyncio
    @asyncio.coroutine
    def test_set_qos(self):
        channel = yield from self.create_channel()
        yield from channel.set_qos(prefetch_count=1)
