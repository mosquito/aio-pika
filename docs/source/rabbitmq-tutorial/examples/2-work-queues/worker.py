import asyncio

from aio_pika import connect
from aio_pika.abc import AbstractIncomingMessage


async def on_message(message: AbstractIncomingMessage) -> None:
    async with message.process():
        print(f" [x] Received message {message!r}")
        print(f"     Message body is: {message.body!r}")


async def main() -> None:
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/")

    async with connection:
        # Creating a channel
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        # Declaring queue
        queue = await channel.declare_queue(
            "task_queue",
            durable=True,
        )

        # Start listening the queue with name 'task_queue'
        await queue.consume(on_message)

        print(" [*] Waiting for messages. To exit press CTRL+C")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
