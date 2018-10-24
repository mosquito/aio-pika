from unittest import skip

import pytest

from tests.test_amqp import TestCase as AMQPTestCase


pytestmark = pytest.mark.asyncio


class TestCase(AMQPTestCase):
    async def create_channel(self, connection=None, cleanup=True,
                             publisher_confirms=False, **kwargs):
        if connection is None:
            connection = await self.create_connection()

        channel = await connection.channel(
            publisher_confirms=publisher_confirms, **kwargs
        )

        if cleanup:
            self.addCleanup(channel.close)

        return channel

    test_simple_publish_and_receive = skip("skipped")(
        AMQPTestCase.test_simple_publish_and_receive
    )
    test_simple_publish_without_confirm = skip("skipped")(
        AMQPTestCase.test_simple_publish_without_confirm
    )

    test_test_delivery_fail = skip("skipped")(
        AMQPTestCase.test_delivery_fail
    )
