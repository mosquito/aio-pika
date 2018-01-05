import asyncio
import sys
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

    # Declare an exchange
    topic_logs_exchange = await channel.declare_exchange(
        'topic_logs', ExchangeType.TOPIC
    )

    # Declaring queue
    queue = await channel.declare_queue(
        'task_queue', durable=True
    )

    binding_keys = sys.argv[1:]

    if not binding_keys:
        sys.stderr.write(
            "Usage: %s [binding_key]...\n" % sys.argv[0]
        )
        sys.exit(1)

    for binding_key in binding_keys:
        await queue.bind(topic_logs_exchange, routing_key=binding_key)

    # Start listening the queue with name 'task_queue'
    await queue.consume(on_message)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))

    # we enter a never-ending loop that waits for
    # data and runs callbacks whenever necessary.
    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()
