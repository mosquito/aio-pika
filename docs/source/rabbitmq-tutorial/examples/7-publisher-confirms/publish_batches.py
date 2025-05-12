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

        batchsize = 100
        outstanding_messages = []

        # Sending the messages
        for msg in get_messages_to_publish():
            outstanding_messages.append(
                asyncio.create_task(
                    channel.default_exchange.publish(
                        Message(msg),
                        routing_key=queue.name,
                        timeout=5.0,
                    )
                )
            )
            # Yield control flow to event loop, so message sending is initiated:
            await asyncio.sleep(0)

            if len(outstanding_messages) == batchsize:
                await asyncio.gather(*outstanding_messages)
                outstanding_messages.clear()

        if len(outstanding_messages) > 0:
            await asyncio.gather(*outstanding_messages)
            outstanding_messages.clear()
        # Done sending messages

        print(" [x] Sent and confirmed multiple messages in batches. ")


if __name__ == "__main__":
    asyncio.run(main())
