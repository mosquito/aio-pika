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
        connection = self.application.settings['amqp_connection']
        channel = await connection.channel()

        try:
            await channel.default_exchange.publish(
                Message(body=self.request.body),
                routing_key="test",
            )
        finally:
            await channel.close()

        self.finish("OK")


async def make_app():
    amqp_connection = await connect_robust()

    channel = await amqp_connection.channel()
    queue = await channel.declare_queue('test', auto_delete=True)
    await queue.consume(QUEUE.put, no_ack=True)

    return tornado.web.Application(
        [
            (r"/publish", PublisherHandler),
            (r"/subscribe", SubscriberHandler),
        ],
        amqp_connection=amqp_connection
    )


if __name__ == "__main__":
    app = io_loop.asyncio_loop.run_until_complete(make_app())
    app.listen(8888)

    tornado.ioloop.IOLoop.current().start()
