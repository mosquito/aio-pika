import asyncio
import logging
from contextlib import suppress
from functools import partial
from typing import Callable

import shortuuid

import aio_pika
import aiormq.exceptions
import pytest
from aio_pika.message import Message
from aio_pika.robust_channel import RobustChannel
from aio_pika.robust_connection import RobustConnection
from aio_pika.robust_queue import RobustQueue
from async_generator import async_generator, yield_
from tests import get_random_name


class Proxy:
    CHUNK_SIZE = 1500

    def __init__(self, *, loop, shost="127.0.0.1", sport, dhost="127.0.0.1", dport):

        self.loop = loop

        self.src_host = shost
        self.src_port = sport
        self.dst_host = dhost
        self.dst_port = dport
        self.connections = set()

    async def _pipe(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        try:
            while not reader.at_eof():
                writer.write(await reader.read(self.CHUNK_SIZE))
        finally:
            writer.close()

    async def handle_client(
        self, creader: asyncio.StreamReader, cwriter: asyncio.StreamWriter
    ):
        sreader, swriter = await asyncio.open_connection(
            host=self.dst_host, port=self.dst_port,
        )

        self.connections.add(swriter)
        self.connections.add(cwriter)

        await asyncio.gather(
            self._pipe(sreader, cwriter), self._pipe(creader, swriter),
        )

    async def start(self):
        return await asyncio.start_server(
            self.handle_client, host=self.src_host, port=self.src_port,
        )

    async def disconnect(self, wait=True):
        tasks = list()

        async def close(writer):
            writer.close()
            await asyncio.gather(writer.drain(), return_exceptions=True)

        while self.connections:
            writer = self.connections.pop()  # type: asyncio.StreamWriter
            tasks.append(self.loop.create_task(close(writer)))

        if wait and tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


@pytest.fixture
def amqp_url(amqp_direct_url, proxy: Proxy):
    return (
        amqp_direct_url.with_host(proxy.src_host)
        .with_port(proxy.src_port)
        .update_query(reconnect_interval=1)
    )


@pytest.fixture
def proxy_port(aiomisc_unused_port_factory) -> int:
    return aiomisc_unused_port_factory()


@pytest.fixture
@async_generator
async def proxy(loop, amqp_direct_url, proxy_port, add_cleanup):
    p = Proxy(
        dhost=amqp_direct_url.host,
        dport=amqp_direct_url.port,
        sport=proxy_port,
        loop=loop,
    )

    await p.start()

    try:
        await yield_(p)
    finally:
        await p.disconnect()


@pytest.fixture(scope="module")
def connection_fabric():
    return aio_pika.connect_robust


@pytest.fixture
def create_direct_connection(loop, amqp_direct_url):
    return partial(aio_pika.connect, amqp_direct_url, loop=loop)


@pytest.fixture
def create_connection(connection_fabric, loop, amqp_url):
    return partial(connection_fabric, amqp_url, loop=loop)


async def test_channel_fixture(channel: aio_pika.RobustChannel):
    assert isinstance(channel, aio_pika.RobustChannel)


async def test_connection_fixture(connection: aio_pika.RobustConnection):
    assert isinstance(connection, aio_pika.RobustConnection)


def test_amqp_url_is_not_direct(amqp_url, amqp_direct_url):
    assert amqp_url != amqp_direct_url


async def test_set_qos(channel: aio_pika.Channel):
    await channel.set_qos(prefetch_count=1)


async def test_revive_passive_queue_on_reconnect(create_connection):
    client1 = await create_connection()
    assert isinstance(client1, RobustConnection)

    client2 = await create_connection()
    assert isinstance(client2, RobustConnection)

    reconnect_event = asyncio.Event()
    reconnect_count = 0

    def reconnect_callback(sender, conn):
        nonlocal reconnect_count
        reconnect_count += 1
        reconnect_event.set()
        reconnect_event.clear()

    client2.add_reconnect_callback(reconnect_callback)

    queue_name = get_random_name()
    channel1 = await client1.channel()
    assert isinstance(channel1, RobustChannel)

    channel2 = await client2.channel()
    assert isinstance(channel2, RobustChannel)

    queue1 = await channel1.declare_queue(
        queue_name, auto_delete=False, passive=False
    )
    assert isinstance(queue1, RobustQueue)

    queue2 = await channel2.declare_queue(queue_name, passive=True)
    assert isinstance(queue2, RobustQueue)

    await client2.connection.close(aiormq.AMQPError(320, "Closed"))

    await reconnect_event.wait()

    assert reconnect_count == 1

    with suppress(asyncio.TimeoutError):
        await asyncio.wait_for(
            reconnect_event.wait(), client2.reconnect_interval * 2
        )

    assert reconnect_count == 1


@pytest.mark.skip
async def test_robust_reconnect(
    create_connection, proxy: Proxy, loop, add_cleanup: Callable
):
    conn1 = await create_connection()
    conn2 = await create_connection()

    assert isinstance(conn1, aio_pika.RobustConnection)
    assert isinstance(conn2, aio_pika.RobustConnection)

    async with conn1, conn2:

        channel1 = await conn1.channel()
        channel2 = await conn2.channel()

        assert isinstance(channel1, aio_pika.RobustChannel)
        assert isinstance(channel2, aio_pika.RobustChannel)

        async with channel1, channel2:
            shared = []

            # Declaring temporary queue
            queue = await channel1.declare_queue()

            async def reader():
                nonlocal shared
                async with queue.iterator() as q:
                    async for message in q:
                        shared.append(message)
                        await message.ack()

            reader_task = loop.create_task(reader())

            for i in range(5):
                await channel2.default_exchange.publish(
                    Message(str(i).encode()), queue.name,
                )

            logging.info("Disconnect all clients")
            await proxy.disconnect()

            logging.info("Waiting for reconnect")
            await asyncio.sleep(5)

            logging.info("Waiting connections")
            await asyncio.wait_for(
                asyncio.gather(conn1.ready(), conn2.ready(),), timeout=20,
            )

            for i in range(5, 10):
                await channel2.default_exchange.publish(
                    Message(str(i).encode()), queue.name,
                )

            while len(shared) < 10:
                await asyncio.sleep(0.1)

            assert len(shared) == 10

            reader_task.cancel()
            await asyncio.gather(reader_task, return_exceptions=True)


async def test_channel_locked_resource2(connection: aio_pika.RobustConnection):
    ch1 = await connection.channel()
    ch2 = await connection.channel()

    qname = get_random_name("channel", "locked", "resource")

    q1 = await ch1.declare_queue(qname, exclusive=True, robust=False)
    await q1.consume(print, exclusive=True)

    with pytest.raises(aiormq.exceptions.ChannelAccessRefused):
        q2 = await ch2.declare_queue(qname, exclusive=True, robust=False)
        await q2.consume(print, exclusive=True)


async def test_channel_close_when_exclusive_queue(
    create_connection, create_direct_connection, proxy: Proxy, loop
):
    direct_conn, proxy_conn = await asyncio.gather(
        create_direct_connection(), create_connection()
    )

    direct_channel, proxy_channel = await asyncio.gather(
        direct_conn.channel(), proxy_conn.channel()
    )

    qname = get_random_name("robust", "exclusive", "queue")

    proxy_queue = await proxy_channel.declare_queue(
        qname, exclusive=True, durable=True
    )

    logging.info("Disconnecting all proxy connections")
    await proxy.disconnect(wait=True)
    await asyncio.sleep(0.5)

    logging.info("Declaring exclusive queue through direct channel")
    await direct_channel.declare_queue(qname, exclusive=True, durable=True)

    async def close_after(delay, closer):
        await asyncio.sleep(delay)
        await closer()
        logging.info("Closed")

    await loop.create_task(close_after(5, direct_conn.close))
    await proxy_conn.connected.wait()
    await proxy_queue.delete()


async def test_context_process_abrupt_channel_close(
    connection: aio_pika.RobustConnection,
    declare_exchange: Callable,
    declare_queue: Callable,
):
    # https://github.com/mosquito/aio-pika/issues/302
    queue_name = get_random_name("test_connection")
    routing_key = get_random_name("rounting_key")

    channel = await connection.channel()
    exchange = await declare_exchange(
        "direct", auto_delete=True, channel=channel
    )
    queue = await declare_queue(queue_name, auto_delete=True, channel=channel)

    await queue.bind(exchange, routing_key)
    body = bytes(shortuuid.uuid(), "utf-8")

    await exchange.publish(
        Message(body, content_type="text/plain", headers={"foo": "bar"}),
        routing_key,
    )

    incoming_message = await queue.get(timeout=5)
    # close aiormq channel to emulate abrupt connection/channel close
    await channel.channel.close()
    with pytest.raises(aiormq.exceptions.ChannelInvalidStateError):
        async with incoming_message.process():
            # emulate some activity on closed channel
            await channel.channel.basic_publish(
                "dummy", exchange="", routing_key="non_existent"
            )

    # emulate connection/channel restoration of connect_robust
    await channel.reopen()

    # cleanup queue
    incoming_message = await queue.get(timeout=5)
    async with incoming_message.process():
        pass
    await queue.unbind(exchange, routing_key)
