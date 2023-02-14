import asyncio

from aio_pika import connect_robust
from aio_pika.patterns import Master, NackMessage, RejectMessage


async def worker(*, task_id: int) -> None:
    # If you want to reject message or send
    # nack you might raise special exception

    if task_id % 2 == 0:
        raise RejectMessage(requeue=False)

    if task_id % 2 == 1:
        raise NackMessage(requeue=False)

    print(task_id)


async def main() -> None:
    connection = await connect_robust(
        "amqp://guest:guest@127.0.0.1/?name=aio-pika%20worker",
    )

    # Creating channel
    channel = await connection.channel()

    # Initializing Master with channel
    master = Master(channel)
    await master.create_worker("my_task_name", worker, auto_delete=True)

    try:
        await asyncio.Future()
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
