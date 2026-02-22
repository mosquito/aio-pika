import asyncio

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from aio_pika.pool import Pool


async def main() -> None:
    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust("amqp://guest:guest@localhost/")

    connection_pool: Pool = Pool(get_connection, max_size=2)

    async def get_channel() -> aio_pika.Channel:
        async with connection_pool.acquire() as connection:
            return await connection.channel()

    channel_pool: Pool = Pool(get_channel, max_size=10)
    queue_name = "pool_queue"

    async def consume() -> None:
        async with channel_pool.acquire() as channel:  # type: aio_pika.Channel
            await channel.set_qos(10)

            queue = await channel.declare_queue(
                queue_name,
                durable=False,
                auto_delete=False,
            )

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    print(message)
                    await message.ack()

    async def publish() -> None:
        async with channel_pool.acquire() as channel:  # type: aio_pika.Channel
            await channel.default_exchange.publish(
                aio_pika.Message(("Channel: %r" % channel).encode()),
                queue_name,
            )

    async with connection_pool, channel_pool:
        task = asyncio.create_task(consume())
        await asyncio.wait([asyncio.create_task(publish()) for _ in range(50)])
        await task


if __name__ == "__main__":
    asyncio.run(main())
