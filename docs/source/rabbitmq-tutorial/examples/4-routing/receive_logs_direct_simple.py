import asyncio
import sys

from aio_pika import ExchangeType, connect
from aio_pika.abc import AbstractIncomingMessage


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
                f"Usage: {sys.argv[0]} [info] [warning] [error]\n",
            )
            sys.exit(1)

        # Declare an exchange
        direct_logs_exchange = await channel.declare_exchange(
            "logs", ExchangeType.DIRECT,
        )

        # Declaring random queue
        queue = await channel.declare_queue(durable=True)

        for severity in severities:
            await queue.bind(direct_logs_exchange, routing_key=severity)

        async with queue.iterator() as iterator:
            message: AbstractIncomingMessage
            async for message in iterator:
                async with message.process():
                    print(f" [x] {message.routing_key!r}:{message.body!r}")

        print(" [*] Waiting for messages. To exit press CTRL+C")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
