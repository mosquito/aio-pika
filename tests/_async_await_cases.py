from .test_amqp import TestCase


async def test_transaction_simple_async_commit(self: TestCase):
    channel = await self.create_channel(publisher_confirms=False)

    async with channel.transaction():
        pass


async def test_transaction_simple_async_rollback(self: TestCase):
    channel = await self.create_channel(publisher_confirms=False)

    with self.assertRaises(ValueError):
        async with channel.transaction():
            raise ValueError
