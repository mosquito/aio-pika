import asyncio
from aio_pika import connect_robust
from aio_pika.patterns import Master


async def main():
    connection = await connect_robust(
        "amqp://guest:guest@127.0.0.1/"
    )

    async with connection:
        # Creating channel
        channel = await connection.channel()

        master = Master(channel)

        # Creates tasks by proxy object
        for task_id in range(1000):
            await master.proxy.my_task_name(task_id=task_id)

        # Or using create_task method
        for task_id in range(1000):
            await master.create_task(
                'my_task_name', kwargs=dict(task_id=task_id)
            )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
