import asyncio
from aio_pika import connect, IncomingMessage, ExchangeType

loop = asyncio.get_event_loop()


def on_message(message: IncomingMessage):
    print("[x] %r" % message.body)


async def main():
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/", loop=loop)

    # Creating a channel
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    logs_exchange = await channel.declare_exchange(
        'logs',
        ExchangeType.FANOUT
    )

    # Declaring queue
    queue = await channel.declare_queue(exclusive=True)

    # Binding the queue to the exchange
    await queue.bind(logs_exchange)

    # Start listening the queue with name 'task_queue'
    queue.consume(on_message)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main())

    # we enter a never-ending loop that waits for data and runs callbacks whenever necessary.
    print(' [*] Waiting for logs. To exit press CTRL+C')
    loop.run_forever()
