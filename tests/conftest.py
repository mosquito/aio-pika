import asyncio
import gc
import socket
import tracemalloc
from contextlib import suppress
from functools import partial
from time import sleep
from typing import Any, Generator

import aiormq
import pamqp
import pytest
from aiomisc import awaitable
from testcontainers.core.container import DockerContainer
from yarl import URL

import aio_pika


@pytest.fixture
async def add_cleanup(event_loop):
    entities = []

    def payload(func, *args, **kwargs):
        func = partial(awaitable(func), *args, **kwargs)
        entities.append(func)

    try:
        yield payload
    finally:
        for func in entities[::-1]:
            await func()

        entities.clear()


@pytest.fixture
async def create_task(event_loop):
    tasks = []

    def payload(coroutine):
        task = event_loop.create_task(coroutine)
        tasks.append(task)
        return task

    try:
        yield payload
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


class RabbitmqContainer(DockerContainer):       # type: ignore
    _amqp_port: int
    _amqps_port: int

    def get_amqp_url(self) -> URL:
        return URL.build(
            scheme="amqp", user="guest", password="guest", path="//",
            host=self.get_container_host_ip(),
            port=self._amqp_port,
        )

    def get_amqps_url(self) -> URL:
        return URL.build(
            scheme="amqps", user="guest", password="guest", path="//",
            host=self.get_container_host_ip(),
            port=self._amqps_port,
        )

    def readiness_probe(self) -> None:
        host = self.get_container_host_ip()
        port = int(self.get_exposed_port(5672))
        while True:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                try:
                    sock.connect((host, port))
                    sock.send(b"AMQP\0x0\0x0\0x9\0x1")
                    data = sock.recv(4)
                    if len(data) != 4:
                        sleep(0.3)
                        continue
                except ConnectionError:
                    sleep(0.3)
                    continue
                return

    def start(self) -> "RabbitmqContainer":
        self.with_exposed_ports(5672, 5671)
        super().start()
        self.readiness_probe()
        self._amqp_port = int(self.get_exposed_port(5672))
        self._amqps_port = int(self.get_exposed_port(5671))
        return self


@pytest.fixture(scope="module")
def rabbitmq_container() -> Generator[RabbitmqContainer, Any, Any]:
    with RabbitmqContainer("mosquito/aiormq-rabbitmq") as container:
        yield container


@pytest.fixture(scope="module")
def amqp_direct_url(request, rabbitmq_container: RabbitmqContainer) -> URL:
    return rabbitmq_container.get_amqp_url().update_query(
        name=request.node.nodeid
    )


@pytest.fixture
def amqp_url(request, amqp_direct_url) -> URL:
    query = dict(amqp_direct_url.query)
    query["name"] = request.node.nodeid
    return amqp_direct_url.with_query(**query)


@pytest.fixture(
    scope="module",
    params=[aio_pika.connect, aio_pika.connect_robust],
    ids=["connect", "connect_robust"],
)
def connection_fabric(request):
    return request.param


@pytest.fixture
def create_connection(connection_fabric, event_loop, amqp_url):
    return partial(connection_fabric, amqp_url, loop=event_loop)


@pytest.fixture
def create_channel(connection: aio_pika.Connection, add_cleanup):
    conn = connection

    async def fabric(cleanup=True, connection=None, *args, **kwargs):
        if connection is None:
            connection = conn

        channel = await connection.channel(*args, **kwargs)
        if cleanup:
            add_cleanup(channel.close)

        return channel

    return fabric


# noinspection PyTypeChecker
@pytest.fixture
async def connection(create_connection) -> aio_pika.Connection:  # type: ignore
    async with await create_connection() as conn:
        yield conn


# noinspection PyTypeChecker
@pytest.fixture
async def channel(      # type: ignore
    connection: aio_pika.Connection,
) -> aio_pika.Channel:
    async with connection.channel() as ch:
        yield ch


@pytest.fixture
def declare_queue(connection, channel, add_cleanup):
    ch = channel

    async def fabric(
        *args, cleanup=True, channel=None, **kwargs,
    ) -> aio_pika.Queue:
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
        *args, channel=None, cleanup=True, **kwargs,
    ) -> aio_pika.Exchange:
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
