import asyncio

import pytest

from aio_pika import connect_robust
from tests import AMQP_URL
from tests.test_amqp_without_publisher_confirms import TestCase as AMQPTestCase


class TestCase(AMQPTestCase):
    @asyncio.coroutine
    def create_connection(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)
        self.addCleanup(client.close)
        return client

    @asyncio.coroutine
    def create_channel(self, connection=None, publisher_confirms=False, **kwargs):
        if connection is None:
            connection = yield from self.create_connection()

        channel = yield from connection.channel(publisher_confirms=publisher_confirms, **kwargs)
        self.addCleanup(channel.close)
        return channel

    @pytest.mark.asyncio
    def test_set_qos(self):
        channel = yield from self.create_channel()
        yield from channel.set_qos(prefetch_count=1)
