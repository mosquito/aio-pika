import asyncio
from typing import Optional

from aio_pika import Message, connect_robust
from aio_pika.abc import AbstractIncomingMessage


async def main() -> None:
    connection = await connect_robust(
        "amqp://guest:guest@127.0.0.1/?name=aio-pika%20example",
    )

    queue_name = "test_queue"
    routing_key = "test_queue"

    # Creating channel
    channel = await connection.channel()

    # Declaring exchange
    exchange = await channel.declare_exchange("direct", auto_delete=True)

    # Declaring queue
    queue = await channel.declare_queue(queue_name, auto_delete=True)

    # Binding queue
    await queue.bind(exchange, routing_key)

    await exchange.publish(
        Message(
            bytes("Hello", "utf-8"),
            content_type="text/plain",
            headers={"foo": "bar"},
        ),
        routing_key,
    )

    # Receiving one message
    incoming_message: Optional[AbstractIncomingMessage] = await queue.get(
        timeout=5, fail=False
    )
    if incoming_message:
        # Confirm message
        await incoming_message.ack()
    else:
        print("Queue empty")

    await queue.unbind(exchange, routing_key)
    await queue.delete()
    await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
