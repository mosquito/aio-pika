import asyncio
from aio_pika import connect_robust
from aio_pika.patterns import Master


async def worker(*, task_id):
    print(task_id)


async def main():
    connection = await connect_robust("amqp://guest:guest@127.0.0.1/")

    # Creating channel
    channel = await connection.channel()

    master = Master(channel)
    await master.create_worker('my_task_name', worker, auto_delete=True)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()
