import asyncio

import tornado.ioloop
import tornado.web

from aio_pika import Message, connect_robust


class Base:
    QUEUE: asyncio.Queue


class SubscriberHandler(tornado.web.RequestHandler, Base):
    async def get(self) -> None:
        message = await self.QUEUE.get()
        await self.finish(message.body)


class PublisherHandler(tornado.web.RequestHandler):
    async def post(self) -> None:
        connection = self.application.settings["amqp_connection"]
        channel = await connection.channel()

        try:
            await channel.default_exchange.publish(
                Message(body=self.request.body), routing_key="test",
            )
        finally:
            await channel.close()

        await self.finish("OK")


async def make_app() -> tornado.web.Application:
    amqp_connection = await connect_robust()

    channel = await amqp_connection.channel()
    queue = await channel.declare_queue("test", auto_delete=True)
    Base.QUEUE = asyncio.Queue()

    await queue.consume(Base.QUEUE.put, no_ack=True)

    return tornado.web.Application(
        [(r"/publish", PublisherHandler), (r"/subscribe", SubscriberHandler)],
        amqp_connection=amqp_connection,
    )


async def main() -> None:
    app = await make_app()
    app.listen(8888)
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())

