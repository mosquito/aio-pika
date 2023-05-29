import asyncio

from aio_pika import Message, connect


def get_messages_to_publish():
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
                channel.default_exchange.publish(
                    Message(msg),
                    routing_key=queue.name,
                ),
            )
            if len(outstanding_messages) == batchsize:
                await asyncio.wait_for(
                    asyncio.gather(*outstanding_messages),
                    timeout=5.0,
                )
                outstanding_messages.clear()

        if len(outstanding_messages) > 0:
            await asyncio.wait_for(
                asyncio.gather(*outstanding_messages),
                timeout=5.0,
            )
            outstanding_messages.clear()
        # Done sending messages

        print(" [x] Sent and confirmed multiple messages in batches. ")


if __name__ == "__main__":
    asyncio.run(main())
