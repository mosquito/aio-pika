import asyncio
import aio_pika
from aio_pika.pool import Pool


async def main():
    loop = asyncio.get_event_loop()

    async def get_connection():
        return await aio_pika.connect_robust("amqp://guest:guest@localhost/")

    connection_pool = Pool(get_connection, max_size=2, loop=loop)

    async def get_channel() -> aio_pika.Channel:
        async with connection_pool.acquire() as connection:
            return await connection.channel()

    channel_pool = Pool(get_channel, max_size=10, loop=loop)
    queue_name = "pool_queue"

    async def consume():
        async with channel_pool.acquire() as channel:  # type: aio_pika.Channel
            await channel.set_qos(10)

            queue = await channel.declare_queue(
                queue_name, durable=False, auto_delete=False
            )

            async for message in queue:
                print(message)
                message.ack()

    async def publish():
        async with channel_pool.acquire() as channel:  # type: aio_pika.Channel
            await channel.default_exchange.publish(
                aio_pika.Message(
                    ("Channel: %r" % channel).encode()
                ),
                queue_name,
            )

    task = loop.create_task(consume())
    await asyncio.wait([publish() for _ in range(10000)])
    await task


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
