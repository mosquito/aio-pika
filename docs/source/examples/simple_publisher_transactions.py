import asyncio

import aio_pika


async def main() -> None:
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@127.0.0.1/",
    )

    async with connection:
        routing_key = "test_queue"

        # Transactions conflicts with `publisher_confirms`
        channel = await connection.channel(publisher_confirms=False)

        # Use transactions with async context manager
        async with channel.transaction():
            # Publishing messages but delivery will not be done
            # before committing this transaction
            for i in range(10):
                message = aio_pika.Message(body="Hello #{}".format(i).encode())

                await channel.default_exchange.publish(
                    message, routing_key=routing_key,
                )

        # Using transactions manually
        tx = channel.transaction()

        # start transaction manually
        await tx.select()

        await channel.default_exchange.publish(
            aio_pika.Message(body="Hello {}".format(routing_key).encode()),
            routing_key=routing_key,
        )

        await tx.commit()

        # Using transactions manually
        tx = channel.transaction()

        # start transaction manually
        await tx.select()

        await channel.default_exchange.publish(
            aio_pika.Message(body="Should be rejected".encode()),
            routing_key=routing_key,
        )

        await tx.rollback()


if __name__ == "__main__":
    asyncio.run(main())
