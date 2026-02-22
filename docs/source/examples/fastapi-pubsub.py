from contextlib import asynccontextmanager
from typing import AsyncGenerator

import aio_pika
from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    app.state.connection = await aio_pika.connect_robust(
        "amqp://guest:guest@localhost/",
    )
    app.state.channel = await app.state.connection.channel()
    try:
        yield
    finally:
        await app.state.connection.close()


app = FastAPI(lifespan=lifespan)


@app.post("/publish")
async def publish_message(message: str) -> dict:
    await app.state.channel.default_exchange.publish(
        aio_pika.Message(body=message.encode()),
        routing_key="test_queue",
    )
    return {"status": "ok"}


@app.get("/consume")
async def consume_message() -> dict:
    queue = await app.state.channel.declare_queue(
        "test_queue",
        auto_delete=True,
    )
    message = await queue.get(timeout=5, fail=False)

    if message:
        await message.ack()
        return {"body": message.body.decode()}

    return {"body": None}
