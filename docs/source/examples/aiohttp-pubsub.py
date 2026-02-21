import aio_pika
from aiohttp import web


async def publish_message(request: web.Request) -> web.Response:
    channel = request.app["channel"]
    body = await request.read()
    await channel.default_exchange.publish(
        aio_pika.Message(body=body),
        routing_key="test_queue",
    )
    return web.json_response({"status": "ok"})


async def consume_message(request: web.Request) -> web.Response:
    channel = request.app["channel"]
    queue = await channel.declare_queue("test_queue", auto_delete=True)
    message = await queue.get(timeout=5, fail=False)

    if message:
        await message.ack()
        return web.json_response({"body": message.body.decode()})

    return web.json_response({"body": None})


async def on_startup(app: web.Application) -> None:
    app["connection"] = await aio_pika.connect_robust(
        "amqp://guest:guest@localhost/",
    )
    app["channel"] = await app["connection"].channel()


async def on_shutdown(app: web.Application) -> None:
    await app["connection"].close()


app = web.Application()
app.router.add_post("/publish", publish_message)
app.router.add_get("/consume", consume_message)
app.on_startup.append(on_startup)
app.on_shutdown.append(on_shutdown)

if __name__ == "__main__":
    web.run_app(app)
