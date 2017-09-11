import asyncio

import pytest

from aio_pika import connect_robust
from tests import AMQP_URL
from tests.test_amqp import TestCase as AMQPTestCase


class TestCase(AMQPTestCase):
    @asyncio.coroutine
    def create_connection(self):
        client = yield from connect_robust(AMQP_URL, loop=self.loop)
        self.addCleanup(client.close)
        return client

    @pytest.mark.asyncio
    def test_set_qos(self):
        channel = yield from self.create_channel()
        yield from channel.set_qos(prefetch_count=1)
