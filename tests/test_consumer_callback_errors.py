import asyncio
import logging
from typing import Any, List

import aio_pika
from aio_pika.abc import AbstractIncomingMessage


class RecordCollector(logging.Handler):
    def __init__(self, records: List[logging.LogRecord]):
        super().__init__(level=logging.ERROR)
        self.records = records

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


async def test_callback_error_keeps_consumer(connection: aio_pika.Connection):
    """A failing consumer callback must not stop the consumer.

    The queue logger reports the exception once. The event loop must get
    no unhandled exception, and the exception must not come back when the
    channel closes. See issue #688.
    """
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop_errors: List[Any] = []
    loop.set_exception_handler(
        lambda _loop, context: loop_errors.append(context),
    )

    records: List[logging.LogRecord] = []
    collector = RecordCollector(records)
    logger = logging.getLogger("aio_pika.queue")
    logger.addHandler(collector)

    channel = await connection.channel()
    queue = await channel.declare_queue(exclusive=True, auto_delete=True)
    processed: asyncio.Queue = asyncio.Queue()

    async def callback(message: AbstractIncomingMessage) -> None:
        if message.body == b"fail":
            raise RuntimeError("callback failure")
        await message.ack()
        await processed.put(message.body)

    try:
        consumer_tag = await queue.consume(callback)
        await channel.default_exchange.publish(
            aio_pika.Message(b"fail"),
            routing_key=queue.name,
        )
        await channel.default_exchange.publish(
            aio_pika.Message(b"next"),
            routing_key=queue.name,
        )

        body = await asyncio.wait_for(processed.get(), timeout=10)
        await queue.cancel(consumer_tag)

        # The exception of the callback must not come back here.
        await channel.close()
    finally:
        logger.removeHandler(collector)
        loop.set_exception_handler(previous_handler)

    assert body == b"next"
    assert not loop_errors, loop_errors
    assert len(records) == 1, [record.getMessage() for record in records]
    assert "callback failure" in records[0].getMessage()
    assert channel.is_closed
    assert not connection.is_closed
