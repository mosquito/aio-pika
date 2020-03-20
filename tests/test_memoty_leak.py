import weakref

from tests import BaseTestCase


class TestCase(BaseTestCase):

    async def test_on_successful_cleanup_message(self):
        rabbitmq_connection = await self.create_connection()

        weakset = weakref.WeakSet()

        async def f(rabbitmq_connection, weakset):
            async with rabbitmq_connection.channel() as channel:
                weakset.add(channel)

        async with rabbitmq_connection:
            for i in range(5):
                await f(rabbitmq_connection, weakset)

        self.assertEqual(len(weakset), 0)
