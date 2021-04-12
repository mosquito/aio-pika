import asyncio
import gc
import os
import tracemalloc
from contextlib import suppress
from functools import partial

import aiormq
import pamqp
import pytest
from aiomisc import awaitable
from async_generator import async_generator, yield_
from yarl import URL

import aio_pika
from aiormq.connection import DEFAULT_PORTS


@pytest.fixture
@async_generator
async def add_cleanup(loop):
    entities = []

    def payload(func, *args, **kwargs):
        nonlocal entities
        func = partial(awaitable(func), *args, **kwargs)
        entities.append(func)

    try:
        await yield_(payload)
    finally:
        for func in entities[::-1]:
            await func()

        entities.clear()


@pytest.fixture
@async_generator
async def create_task(loop):
    tasks = []

    def payload(coroutine):
        nonlocal tasks
        task = loop.create_task(coroutine)
        tasks.append(task)
        return task

    try:
        await yield_(payload)
    finally:
        cancelled = []
        for task in tasks:
            if task.done():
                continue
            task.cancel()
            cancelled.append(task)

        results = await asyncio.gather(*cancelled, return_exceptions=True)

        for result in results:
            if not isinstance(result, asyncio.CancelledError):
                raise result


@pytest.fixture
def amqp_direct_url(request) -> URL:
    url = URL(
        os.getenv("AMQP_URL", "amqp://guest:guest@localhost"),
    ).update_query(name=request.node.nodeid)

    default_port = DEFAULT_PORTS[url.scheme]

    if not url.port:
        url = url.with_port(default_port)

    return url


@pytest.fixture
def amqp_url(request, amqp_direct_url) -> URL:
    query = dict(amqp_direct_url.query)
    query['name'] = request.node.nodeid
    return amqp_direct_url.with_query(**query)


@pytest.fixture(
    scope="module",
    params=[aio_pika.connect, aio_pika.connect_robust],
    ids=["connect", "connect_robust"],
)
def connection_fabric(request):
    return request.param


@pytest.fixture
def create_connection(connection_fabric, loop, amqp_url):
    return partial(connection_fabric, amqp_url, loop=loop)


@pytest.fixture
def create_channel(connection: aio_pika.Connection, add_cleanup):
    conn = connection

    async def fabric(cleanup=True, connection=None, *args, **kwargs):
        nonlocal add_cleanup, conn

        if connection is None:
            connection = conn

        channel = await connection.channel(*args, **kwargs)
        if cleanup:
            add_cleanup(channel.close)

        return channel

    return fabric


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
    ch = channel

    async def fabric(
        *args, cleanup=True, channel=None, **kwargs
    ) -> aio_pika.Queue:
        nonlocal ch, add_cleanup

        if channel is None:
            channel = ch

        queue = await channel.declare_queue(*args, **kwargs)

        if cleanup and not kwargs.get("auto_delete"):
            add_cleanup(queue.delete)

        return queue

    return fabric


@pytest.fixture
def declare_exchange(connection, channel, add_cleanup):
    ch = channel

    async def fabric(
        *args, channel=None, cleanup=True, **kwargs
    ) -> aio_pika.Exchange:
        nonlocal ch, add_cleanup

        if channel is None:
            channel = ch

        exchange = await channel.declare_exchange(*args, **kwargs)

        if cleanup and not kwargs.get("auto_delete"):
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

        with suppress(Exception):
            gc.collect()

        snapshot_after = tracemalloc.take_snapshot().filter_traces(filters)

        top_stats = snapshot_after.compare_to(
            snapshot_before, "lineno", cumulative=True,
        )

        assert not top_stats
    finally:
        tracemalloc.stop()
