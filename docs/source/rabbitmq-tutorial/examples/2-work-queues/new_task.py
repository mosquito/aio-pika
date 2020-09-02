import sys
import asyncio
from aio_pika import connect, Message, DeliveryMode


async def main(loop):
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/", loop=loop)

    # Creating a channel
    channel = await connection.channel()

    message_body = b" ".join(
        arg.encode() for arg in sys.argv[1:]) or b"Hello World!"

    message = Message(
        message_body,
        delivery_mode=DeliveryMode.PERSISTENT
    )

    # Sending the message
    await channel.default_exchange.publish(
        message, routing_key="task_queue"
    )

    print(" [x] Sent %r" % message)

    await connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
