import asyncio
from unittest import skip

from tests.test_amqp import TestCase as AMQPTestCase


class TestCase(AMQPTestCase):
    @asyncio.coroutine
    def create_channel(self, connection=None, publisher_confirms=False, **kwargs):
        if connection is None:
            connection = yield from self.create_connection()

        channel = yield from connection.channel(publisher_confirms=publisher_confirms, **kwargs)
        self.addCleanup(channel.close)
        return channel

    test_simple_publish_and_receive = skip("skipped")(AMQPTestCase.test_simple_publish_and_receive)
    test_simple_publish_without_confirm = skip("skipped")(AMQPTestCase.test_simple_publish_without_confirm)
