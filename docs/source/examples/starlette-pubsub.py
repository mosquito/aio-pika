import aio_pika
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route


async def publish_message(request: Request) -> JSONResponse:
    channel = request.app.state.channel
    body = await request.body()
    await channel.default_exchange.publish(
        aio_pika.Message(body=body),
        routing_key="test_queue",
    )
    return JSONResponse({"status": "ok"})


async def consume_message(request: Request) -> JSONResponse:
    channel = request.app.state.channel
    queue = await channel.declare_queue("test_queue", auto_delete=True)
    message = await queue.get(timeout=5, fail=False)

    if message:
        await message.ack()
        return JSONResponse({"body": message.body.decode()})

    return JSONResponse({"body": None})


async def on_startup() -> None:
    app.state.connection = await aio_pika.connect_robust(
        "amqp://guest:guest@localhost/",
    )
    app.state.channel = await app.state.connection.channel()


async def on_shutdown() -> None:
    await app.state.connection.close()


app = Starlette(
    routes=[
        Route("/publish", publish_message, methods=["POST"]),
        Route("/consume", consume_message),
    ],
    on_startup=[on_startup],
    on_shutdown=[on_shutdown],
)
