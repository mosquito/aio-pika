import asyncio
from aio_pika import connect_robust
from aio_pika.patterns import RPC


async def main():
    connection = await connect_robust(
        "amqp://guest:guest@127.0.0.1/"
    )

    async with connection:
        # Creating channel
        channel = await connection.channel()

        rpc = await RPC.create(channel)

        # Creates tasks by proxy object
        for i in range(1000):
            print(await rpc.proxy.multiply(x=100, y=i))

        # Or using create_task method
        for i in range(1000):
            print(
                await rpc.call(
                    'multiply', kwargs=dict(x=100, y=i)
                )
            )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
