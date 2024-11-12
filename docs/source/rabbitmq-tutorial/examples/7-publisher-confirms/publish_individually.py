import asyncio
from typing import Generator

from aio_pika import Message, connect


def get_messages_to_publish() -> Generator[bytes, None, None]:
    for i in range(10000):
        yield f"Hello World {i}!".encode()


async def main() -> None:
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/")

    async with connection:
        # Creating a channel
        channel = await connection.channel()

        # Declaring queue
        queue = await channel.declare_queue("hello")

        # Sending the messages
        for msg in get_messages_to_publish():
            # Waiting for publisher confirmation with timeout for every message
            await channel.default_exchange.publish(
                Message(msg),
                routing_key=queue.name,
                timeout=5.0,
            )
        # Done sending messages
        print(" [x] Sent and confirmed multiple messages individually. ")


if __name__ == "__main__":
    asyncio.run(main())
