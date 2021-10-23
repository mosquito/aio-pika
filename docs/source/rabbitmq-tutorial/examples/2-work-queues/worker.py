import asyncio
from aio_pika import connect, IncomingMessage

loop = asyncio.get_event_loop()


async def on_message(message: IncomingMessage):
    async with message.process():
        print(" [x] Received message %r" % message)
        print("     Message body is: %r" % message.body)


async def main():
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/", loop=loop)

    # Creating a channel
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)

    # Declaring queue
    queue = await channel.declare_queue(
        "task_queue",
        durable=True
    )

    # Start listening the queue with name 'task_queue'
    await queue.consume(on_message)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main())

    # we enter a never-ending loop that waits for data and runs
    # callbacks whenever necessary.
    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()
