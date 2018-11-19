import asyncio
from aio_pika import connect_robust
from aio_pika.patterns import Master, RejectMessage, NackMessage


async def worker(*, task_id):
    # If you want to reject message or send
    # nack you might raise special exception

    if task_id % 2 == 0:
        raise RejectMessage(requeue=False)

    if task_id % 2 == 1:
        raise NackMessage(requeue=False)

    print(task_id)


async def main():
    connection = await connect_robust("amqp://guest:guest@127.0.0.1/")

    # Creating channel
    channel = await connection.channel()

    master = Master(channel)
    await master.create_worker('my_task_name', worker, auto_delete=True)

    return connection


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    connection = loop.run_until_complete(main())
    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(connection.close())
