import asyncio


import aio_pika


async def main() -> None:
    connection = await aio_pika.connect_robust(
        "amqps://guest:secret@amqp.example.com/?name=SSL%20client"
    )

    async with connection:
        routing_key = "test_queue"

        channel = await connection.channel()

        await channel.default_exchange.publish(
            aio_pika.Message(body="Hello {}".format(routing_key).encode()),
            routing_key=routing_key,
        )


if __name__ == "__main__":
    asyncio.run(main())
