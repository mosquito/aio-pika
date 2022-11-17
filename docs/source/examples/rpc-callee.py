import asyncio

from aio_pika import connect_robust
from aio_pika.patterns import RPC


async def multiply(*, x: int, y: int) -> int:
    return x * y


async def main() -> None:
    connection = await connect_robust(
        "amqp://guest:guest@127.0.0.1/",
        client_properties={"connection_name": "callee"},
    )

    # Creating channel
    channel = await connection.channel()

    rpc = await RPC.create(channel)
    await rpc.register("multiply", multiply, auto_delete=True)

    try:
        await asyncio.Future()
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())

