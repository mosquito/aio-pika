import asyncio
from aio_pika import connect_robust, Message


async def main(loop):
    connection = await connect_robust(
        "amqp://guest:guest@127.0.0.1/", loop=loop
    )

    queue_name = "test_queue"
    routing_key = "test_queue"

    # Creating channel
    channel = await connection.channel()

    # Declaring exchange
    exchange = await channel.declare_exchange(
        'direct', auto_delete=True
    )

    # Declaring queue
    queue = await channel.declare_queue(
        queue_name, auto_delete=True
    )

    # Binding queue
    await queue.bind(exchange, routing_key)

    await exchange.publish(
        Message(
            bytes('Hello', 'utf-8'),
            content_type='text/plain',
            headers={'foo': 'bar'}
        ),
        routing_key
    )

    # Receiving message
    incoming_message = await queue.get(timeout=5)

    # Confirm message
    await incoming_message.ack()

    await queue.unbind(exchange, routing_key)
    await queue.delete()
    await connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
