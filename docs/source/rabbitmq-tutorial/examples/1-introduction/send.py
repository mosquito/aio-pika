import asyncio

from aio_pika import Message, connect


# docs: begin-connection-setup
async def main() -> None:
    # Perform connection
    connection = await connect("amqp://guest:guest@localhost/")

    async with connection:
        # Creating a channel
        channel = await connection.channel()
        # docs: end-connection-setup

        # docs: begin-declaring-queue
        # Declaring queue
        queue = await channel.declare_queue("hello")
        # docs: end-declaring-queue

        # docs: begin-sending-message
        # Sending the message
        await channel.default_exchange.publish(
            Message(b"Hello World!"),
            routing_key=queue.name,
        )
        # docs: end-sending-message

        print(" [x] Sent 'Hello World!'")


if __name__ == "__main__":
    asyncio.run(main())
