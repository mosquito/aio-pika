import asyncio
import sys

from aio_pika import ExchangeType, connect
from aio_pika.abc import AbstractIncomingMessage


async def main() -> None:
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/")

    # Creating a channel
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    # Declare an exchange
    topic_logs_exchange = await channel.declare_exchange(
        "topic_logs", ExchangeType.TOPIC,
    )

    # Declaring queue
    queue = await channel.declare_queue(
        "task_queue", durable=True,
    )

    binding_keys = sys.argv[1:]

    if not binding_keys:
        sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
        sys.exit(1)

    for binding_key in binding_keys:
        await queue.bind(topic_logs_exchange, routing_key=binding_key)

    print(" [*] Waiting for messages. To exit press CTRL+C")

    # Start listening the queue with name 'task_queue'
    async with queue.iterator() as iterator:
        message: AbstractIncomingMessage
        async for message in iterator:
            async with message.process():
                print(f" [x] {message.routing_key!r}:{message.body!r}")


if __name__ == "__main__":
    asyncio.run(main())
