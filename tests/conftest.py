import gc
import os
import tracemalloc
from functools import partial

from aiomisc import awaitable

import aio_pika
import aiormq
import pamqp
import pytest
from async_generator import async_generator, yield_
from yarl import URL


@pytest.fixture
@async_generator
async def add_cleanup():
    entities = []

    def payload(func, *args, **kwargs):
        nonlocal entities
        entities.append(partial(awaitable(func), *args, **kwargs))

    try:
        await yield_(payload)
    finally:
        for func in entities:
            await func()

        entities.clear()


@pytest.fixture
def amqp_url() -> URL:
    return URL(os.getenv("AMQP_URL", "amqp://guest:guest@localhost"))


@pytest.fixture
def create_connection(loop, amqp_url):
    return partial(aio_pika.connect, amqp_url, loop=loop)


# noinspection PyTypeChecker
@pytest.fixture
@async_generator
async def connection(create_connection) -> aio_pika.Connection:
    async with await create_connection() as conn:
        await yield_(conn)


# noinspection PyTypeChecker
@pytest.fixture
@async_generator
async def channel(connection: aio_pika.Connection) -> aio_pika.Channel:
    async with connection.channel() as ch:
        await yield_(ch)


@pytest.fixture
def declare_queue(connection, channel, add_cleanup):
    async def fabric(*args, **kwargs) -> aio_pika.Queue:
        nonlocal channel, add_cleanup

        if 'channel' not in kwargs:
            channel = channel
        else:
            channel = kwargs.pop('channel')

        queue = await channel.declare_queue(*args, **kwargs)

        if not kwargs.get('auto_delete'):
            add_cleanup(queue.delete)

        return queue

    return fabric


@pytest.fixture
def declare_exchange(connection, channel, add_cleanup):
    async def fabric(*args, **kwargs) -> aio_pika.Exchange:
        nonlocal channel, add_cleanup

        if 'channel' not in kwargs:
            channel = channel
        else:
            channel = kwargs.pop('channel')

        exchange = await channel.declare_exchange(*args, **kwargs)

        if not kwargs.get('auto_delete'):
            add_cleanup(exchange.delete)

        return exchange
    return fabric


@pytest.fixture(autouse=True)
def memory_tracer():
    tracemalloc.start()
    tracemalloc.clear_traces()

    filters = (
        tracemalloc.Filter(True, aiormq.__file__),
        tracemalloc.Filter(True, pamqp.__file__),
        tracemalloc.Filter(True, aio_pika.__file__),
    )

    snapshot_before = tracemalloc.take_snapshot().filter_traces(filters)

    try:
        yield

        gc.collect()

        snapshot_after = tracemalloc.take_snapshot().filter_traces(filters)

        top_stats = snapshot_after.compare_to(
            snapshot_before, 'lineno', cumulative=True
        )

        assert not top_stats
    finally:
        tracemalloc.stop()
