import asyncio

from aio_pika import Message, connect

from aiormq.exceptions import DeliveryError


def get_messages_to_publish():
    for i in range(10000):
        yield f"Hello World {i}!".encode()


def handle_confirm(confirmation):
    try:
        _ = confirmation.result()
        # code when message is ack-ed
    except DeliveryError:
        # code when message is nack-ed
        pass
    except TimeoutError:
        # code for message timeout
        pass
    else:
        # code when message is confirmed
        pass


async def main() -> None:
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/")

    async with connection:
        # Creating a channel
        channel = await connection.channel()

        # Declaring queue
        queue = await channel.declare_queue("hello")

        async with asyncio.TaskGroup() as tg:
            # Sending the messages
            for msg in get_messages_to_publish():
                tg.create_task(
                    channel.default_exchange.publish(
                        Message(msg),
                        routing_key=queue.name,
                    )
                ).add_done_callback(handle_confirm)

        print(" [x] Sent and confirmed multiple messages asynchronously. ")


if __name__ == "__main__":
    asyncio.run(main())
