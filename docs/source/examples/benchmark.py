import asyncio
import os
import time
from contextlib import contextmanager
from typing import Generator, Any

import aio_pika
from aio_pika import connect_robust


@contextmanager
def timeit(message: str, iterations: int) -> Generator[Any, Any, Any]:
    delay = -time.perf_counter()
    print(f"{message} started")
    try:
        yield
    finally:
        delay += time.perf_counter()
        print(
            f"{message} completed in {delay:.6f} seconds, "
            f"{iterations} iterations {delay / iterations:.6f} seconds "
            f"per iteration"
        )


async def main() -> None:
    connect = await connect_robust(
        os.getenv("AMQP_URL", "amqp://guest:guest@localhost")
    )

    iterations = 100_000

    async with connect:
        message = aio_pika.Message(b"test")
        incoming_message: aio_pika.abc.AbstractIncomingMessage

        async with connect.channel() as channel:
            queue = await channel.declare_queue(auto_delete=True)

            with timeit("Sequential publisher confirms", iterations=iterations):
                for _ in range(iterations):
                    await channel.default_exchange.publish(
                        message, routing_key=queue.name
                    )

            with timeit("Iterator consume no_ack=False", iterations=iterations):
                counter = 0
                async for incoming_message in queue.iterator(no_ack=False):
                    await incoming_message.ack()
                    counter += 1
                    if counter >= iterations:
                        break

        async with connect.channel(publisher_confirms=False) as channel:
            queue = await channel.declare_queue(auto_delete=True)

            with timeit(
                "Sequential no publisher confirms", iterations=iterations
            ):
                for _ in range(iterations):
                    await channel.default_exchange.publish(
                        message, routing_key=queue.name
                    )

            with timeit("Iterator consume no_ack=True", iterations=iterations):
                counter = 0
                async for _ in queue.iterator(no_ack=True):
                    counter += 1
                    if counter >= iterations:
                        break


if __name__ == "__main__":
    asyncio.run(main())
