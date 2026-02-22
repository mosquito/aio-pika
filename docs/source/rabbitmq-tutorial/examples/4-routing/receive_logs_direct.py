import asyncio
import sys

from aio_pika import ExchangeType, connect
from aio_pika.abc import AbstractIncomingMessage


async def on_message(message: AbstractIncomingMessage) -> None:
    async with message.process():
        print(" [x] %r:%r" % (message.routing_key, message.body))


async def main() -> None:
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/")

    async with connection:
        # Creating a channel
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        severities = sys.argv[1:]

        if not severities:
            sys.stderr.write(
                "Usage: %s [info] [warning] [error]\n" % sys.argv[0],
            )
            sys.exit(1)

        # Declare an exchange
        direct_logs_exchange = await channel.declare_exchange(
            "logs",
            ExchangeType.DIRECT,
        )

        # Declaring random queue
        queue = await channel.declare_queue(durable=True)

        for severity in severities:
            await queue.bind(direct_logs_exchange, routing_key=severity)

        # Start listening the random queue
        await queue.consume(on_message)

        print(" [*] Waiting for messages. To exit press CTRL+C")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
