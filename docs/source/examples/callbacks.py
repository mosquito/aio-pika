import asyncio
import logging

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractIncomingMessage,
    AbstractRobustConnection,
)


# Every callback receives the object that owns the collection as the
# first argument. The other arguments depend on the collection.
def on_close(
    connection: AbstractConnection | None,
    exc: BaseException | None,
) -> None:
    # exc is set even for a close requested by your code.
    logging.info("Connection %r closed: %r", connection, exc)


async def on_reconnect(connection: AbstractRobustConnection | None) -> None:
    # A coroutine function is also accepted. It is scheduled as a task.
    logging.info("Connection %r reconnected", connection)


def on_return(
    channel: AbstractChannel | None,
    message: AbstractIncomingMessage,
) -> None:
    logging.warning("Message %r was returned by the broker", message)


class Service:
    def on_channel_close(
        self,
        channel: AbstractChannel | None,
        exc: BaseException | None,
    ) -> None:
        # A bound method works too: self is bound, channel is the sender.
        logging.info("Channel %r closed: %r", channel, exc)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@127.0.0.1/",
    )
    connection.close_callbacks.add(on_close)
    connection.reconnect_callbacks.add(on_reconnect)

    service = Service()

    async with connection:
        channel = await connection.channel()
        channel.close_callbacks.add(service.on_channel_close)
        channel.return_callbacks.add(on_return)

        await channel.default_exchange.publish(
            aio_pika.Message(b"hello"),
            routing_key="no-such-queue",
        )


if __name__ == "__main__":
    asyncio.run(main())
