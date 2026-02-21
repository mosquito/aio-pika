import asyncio
import sys

from aio_pika import DeliveryMode, ExchangeType, Message, connect


async def main() -> None:
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/")

    async with connection:
        # Creating a channel
        channel = await connection.channel()

        # docs: begin-declare-exchange
        logs_exchange = await channel.declare_exchange(
            "logs", ExchangeType.FANOUT,
        )
        # docs: end-declare-exchange

        # docs: begin-publish-message
        message_body = b" ".join(
            arg.encode() for arg in sys.argv[1:]
        ) or b"Hello World!"

        message = Message(
            message_body,
            delivery_mode=DeliveryMode.PERSISTENT,
        )

        # Sending the message
        await logs_exchange.publish(message, routing_key="info")
        # docs: end-publish-message

        print(f" [x] Sent {message!r}")


if __name__ == "__main__":
    asyncio.run(main())
