import asyncio

from aio_pika import Message, connect


async def main() -> None:
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/")

    async with connection:
        # Creating a channel
        channel = await connection.channel()

        # Declaring queue
        queue = await channel.declare_queue("hello")

        # Sending the message
        await channel.default_exchange.publish(
            Message(b"Hello World!"),
            routing_key=queue.name,
        )

        print(" [x] Sent 'Hello World!'")


if __name__ == "__main__":
    asyncio.run(main())
