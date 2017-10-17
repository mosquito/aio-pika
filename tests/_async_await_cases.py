from aio_pika import Message
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


async def test_async_for_queue(self: TestCase):
    conn = await self.create_connection()

    channel2 = await self.create_channel(connection=conn)

    queue = await channel2.declare_queue(self.get_random_name("queue", "async", "for"), auto_delete=True)

    messages = 100

    async def publisher():
        channel1 = await self.create_channel(connection=conn)

        for i in range(messages):
            await channel1.default_exchange.publish(Message(body=str(i).encode()), routing_key=queue.name)

    self.loop.create_task(publisher())

    count = 0
    data = list()

    async for message in queue:
        with message.process():
            count += 1
            data.append(message.body)

        if count >= messages:
            break

    self.assertSequenceEqual(data, list(map(lambda x: str(x).encode(), range(messages))))


async def test_async_for_queue_async_context(self: TestCase):
    conn = await self.create_connection()

    channel2 = await self.create_channel(connection=conn)

    queue = await channel2.declare_queue(self.get_random_name("queue", "async", "for"), auto_delete=True)

    messages = 100

    async def publisher():
        channel1 = await self.create_channel(connection=conn)

        for i in range(messages):
            await channel1.default_exchange.publish(Message(body=str(i).encode()), routing_key=queue.name)

    self.loop.create_task(publisher())

    count = 0
    data = list()

    async with queue.iterator() as queue_iterator:
        async for message in queue_iterator:
            with message.process():
                count += 1
                data.append(message.body)

            if count >= messages:
                break

    self.assertSequenceEqual(data, list(map(lambda x: str(x).encode(), range(messages))))


async def test_async_connection_context(self: TestCase):
    conn = await self.create_connection()

    async with conn:

        channel2 = await self.create_channel(connection=conn)

        queue = await channel2.declare_queue(self.get_random_name("queue", "async", "for"), auto_delete=True)

        messages = 100

        async def publisher():
            channel1 = await self.create_channel(connection=conn)

            for i in range(messages):
                await channel1.default_exchange.publish(Message(body=str(i).encode()), routing_key=queue.name)

        self.loop.create_task(publisher())

        count = 0
        data = list()

        async with queue.iterator() as queue_iterator:
            async for message in queue_iterator:
                with message.process():
                    count += 1
                    data.append(message.body)

                if count >= messages:
                    break

        self.assertSequenceEqual(data, list(map(lambda x: str(x).encode(), range(messages))))
    self.assertTrue(channel2.is_closed)
