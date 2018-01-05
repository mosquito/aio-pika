import asyncio
from aio_pika import connect_robust
from aio_pika.patterns import RPC


async def multiply(*, x, y):
    return x * y


async def main():
    connection = await connect_robust(
        "amqp://guest:guest@127.0.0.1/"
    )

    # Creating channel
    channel = await connection.channel()

    rpc = await RPC.create(channel)
    await rpc.register('multiply', multiply, auto_delete=True)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()
