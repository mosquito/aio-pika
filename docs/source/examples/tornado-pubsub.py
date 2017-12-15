import asyncio
import tornado.ioloop
import tornado.web

from aio_pika import connect_robust, Message

tornado.ioloop.IOLoop.configure('tornado.platform.asyncio.AsyncIOLoop')
io_loop = tornado.ioloop.IOLoop.current()
asyncio.set_event_loop(io_loop.asyncio_loop)


QUEUE = asyncio.Queue()


class SubscriberHandler(tornado.web.RequestHandler):
    async def get(self):
        message = await QUEUE.get()
        self.finish(message.body)


class PublisherHandler(tornado.web.RequestHandler):
    async def post(self):
        channel = await self.application.amqp_connection.channel()
        await channel.default_exchange.publish(
            Message(body=self.request.body),
            routing_key="test",
        )

        await channel.close()
        self.finish("OK")


def make_app():
    return tornado.web.Application([
        (r"/publish", PublisherHandler),
        (r"/subscribe", SubscriberHandler),
    ])


async def initialize(app):
    app.amqp_connection = await connect_robust()

    channel = await app.amqp_connection.channel()
    queue = await channel.declare_queue('test', auto_delete=True)
    await queue.consume(QUEUE.put, no_ack=True)


if __name__ == "__main__":
    app = make_app()
    app.listen(8888)

    io_loop.asyncio_loop.run_until_complete(initialize(app))
    tornado.ioloop.IOLoop.current().start()
