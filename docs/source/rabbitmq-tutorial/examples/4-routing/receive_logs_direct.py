import sys
import asyncio
from aio_pika import connect, IncomingMessage, ExchangeType


def on_message(message: IncomingMessage):
    with message.process():
        print(" [x] %r:%r" % (message.routing_key, message.body))


async def main(loop):
    # Perform connection
    connection = await connect(
        "amqp://guest:guest@localhost/", loop=loop
    )

    # Creating a channel
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    severities = sys.argv[1:]

    if not severities:
        sys.stderr.write(
            "Usage: %s [info] [warning] [error]\n" % sys.argv[0]
        )
        sys.exit(1)

    # Declare an exchange
    direct_logs_exchange = await channel.declare_exchange(
        "logs", ExchangeType.DIRECT
    )

    # Declaring random queue
    queue = await channel.declare_queue(durable=True)

    for severity in severities:
        await queue.bind(direct_logs_exchange, routing_key=severity)

    # Start listening the random queue
    await queue.consume(on_message)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))

    # we enter a never-ending loop that waits for data
    # and runs callbacks whenever necessary.
    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()
