import asyncio
import sys

from aio_pika import DeliveryMode, ExchangeType, Message, connect


async def main() -> None:
    # Perform connection
    connection = await connect(
        "amqp://guest:guest@localhost/",
    )

    async with connection:
        # Creating a channel
        channel = await connection.channel()

        topic_logs_exchange = await channel.declare_exchange(
            "topic_logs", ExchangeType.TOPIC,
        )

        routing_key = sys.argv[1] if len(sys.argv) > 2 else "anonymous.info"

        message_body = b" ".join(
            arg.encode() for arg in sys.argv[2:]
        ) or b"Hello World!"

        message = Message(
            message_body,
            delivery_mode=DeliveryMode.PERSISTENT,
        )

        # Sending the message
        await topic_logs_exchange.publish(message, routing_key=routing_key)

        print(f" [x] Sent {message!r}")


if __name__ == "__main__":
    asyncio.run(main())
