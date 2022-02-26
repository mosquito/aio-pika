import asyncio

from aio_pika import connect_robust
from aio_pika.patterns import RPC


async def main() -> None:
    connection = await connect_robust(
        "amqp://guest:guest@127.0.0.1/",
        client_properties={"connection_name": "caller"},
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
            print(await rpc.call("multiply", kwargs=dict(x=100, y=i)))


if __name__ == "__main__":
    asyncio.run(main())
