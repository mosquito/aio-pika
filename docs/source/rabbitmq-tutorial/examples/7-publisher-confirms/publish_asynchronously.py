import asyncio

from aio_pika import Message, connect
from aiormq.exceptions import DeliveryError
from pamqp.commands import Basic


def get_messages_to_publish():
    for i in range(10000):
        yield f"Hello World {i}!".encode()


async def publish_and_handle_confirm(exchange, queue_name, message_body):
    try:
        confirmation = await exchange.publish(
            Message(message_body),
            routing_key=queue_name,
            timeout=5.0,
        )
    except DeliveryError as e:
        print(f"Delivery of {message_body!r} failed with exception: {e}")
    except TimeoutError:
        print(f"Timeout occured for {message_body!r}")
    else:
        if not isinstance(confirmation, Basic.Ack):
            print(f"Message {message_body!r} was not acknowledged by broker!")


async def main() -> None:
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/")

    async with connection:
        # Creating a channel
        channel = await connection.channel()

        # Declaring queue
        queue = await channel.declare_queue("hello")

        # List for storing tasks
        tasks = []
        # Sending the messages
        for msg in get_messages_to_publish():
            task = asyncio.create_task(
                publish_and_handle_confirm(
                    channel.default_exchange,
                    queue.name,
                    msg,
                )
            )
            tasks.append(task)
            # Yield control flow to event loop, so message sending is initiated:
            await asyncio.sleep(0)

        # Await all tasks
        await asyncio.gather(*tasks)

        print(" [x] Sent and confirmed multiple messages asynchronously. ")


if __name__ == "__main__":
    asyncio.run(main())
