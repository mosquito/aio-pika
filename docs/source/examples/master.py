import asyncio
from aio_pika import connect_robust
from aio_pika.patterns import Master


async def main():
    connection = await connect_robust("amqp://guest:guest@127.0.0.1/")

    # Creating channel
    channel = await connection.channel()

    master = Master(channel)

    for task_id in range(1000):
        await master.my_task_name(task_id=task_id)

    await connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
