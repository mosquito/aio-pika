import asyncio
import logging
from contextlib import suppress
from functools import partial
from typing import Callable, Type

import aiomisc
import aiormq.exceptions
import pytest
import shortuuid
from aiomisc_pytest.pytest_plugin import TCPProxy
from aiormq import Connection
from pamqp.exceptions import AMQPFrameError
from yarl import URL

import aio_pika
from aio_pika.exceptions import QueueEmpty
from aio_pika.message import Message
from aio_pika.robust_channel import RobustChannel
from aio_pika.robust_connection import RobustConnection
from aio_pika.robust_queue import RobustQueue
from tests import get_random_name


@pytest.fixture
async def proxy(tcp_proxy: Type[TCPProxy], amqp_direct_url: URL):
    p = tcp_proxy(amqp_direct_url.host, amqp_direct_url.port)

    await p.start()
    try:
        yield p
    finally:
        await p.close()


@pytest.fixture
def amqp_url(amqp_direct_url, proxy: TCPProxy):
    return amqp_direct_url.with_host(
        proxy.proxy_host,
    ).with_port(
        proxy.proxy_port,
    ).update_query(
        reconnect_interval=1,
        heartbeat=1,
    )


@pytest.fixture
def proxy_port(aiomisc_unused_port_factory) -> int:
    return aiomisc_unused_port_factory()


@pytest.fixture(scope="module")
def connection_fabric():
    return aio_pika.connect_robust


@pytest.fixture
def create_direct_connection(loop, amqp_direct_url):
    return partial(
        aio_pika.connect,
        amqp_direct_url.update_query(
           name=amqp_direct_url.query["name"] + "::direct",
        ),
        loop=loop,
    )


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
        queue_name, auto_delete=False, passive=False,
    )
    assert isinstance(queue1, RobustQueue)

    queue2 = await channel2.declare_queue(queue_name, passive=True)
    assert isinstance(queue2, RobustQueue)

    await client2.connection.close(aiormq.AMQPError(320, "Closed"))

    await reconnect_event.wait()

    assert reconnect_count == 1

    with suppress(asyncio.TimeoutError):
        await asyncio.wait_for(
            reconnect_event.wait(), client2.reconnect_interval * 2,
        )

    assert reconnect_count == 1


@aiomisc.timeout(30)
async def test_robust_reconnect(
    create_connection, create_direct_connection,
    proxy: TCPProxy, loop, add_cleanup: Callable
):
    read_conn = await create_connection()   # type: aio_pika.RobustConnection
    write_conn = await create_direct_connection()

    reconnect_event = asyncio.Event()
    read_conn.reconnect_callbacks.add(
        lambda *_: reconnect_event.set(), weak=False,
    )

    assert isinstance(read_conn, aio_pika.RobustConnection)

    async with read_conn, write_conn:
        read_channel = await read_conn.channel()
        write_channel = await write_conn.channel()

        assert isinstance(read_channel, aio_pika.RobustChannel)

        qname = get_random_name("robust", "proxy", "shared")

        async with read_channel, write_channel:
            shared = []

            # Declaring temporary queue
            queue = await write_channel.declare_queue(
                qname,
                auto_delete=False,
                durable=True,
            )

            consumer_event = asyncio.Event()

            async def reader(queue_name):
                nonlocal shared

                try:
                    queue = await read_channel.declare_queue(
                        name=queue_name, passive=True,
                    )

                    async with queue.iterator() as q:
                        loop.call_soon(consumer_event.set)

                        async for message in q:
                            shared.append(message)
                            await message.ack()
                finally:
                    logging.info("Exit reader task")

            try:
                reader_task = loop.create_task(reader(queue.name))

                await consumer_event.wait()
                logging.info("Disconnect all clients")
                with proxy.slowdown(1, 1):
                    for i in range(5):
                        await write_channel.default_exchange.publish(
                            Message(str(i).encode()), queue.name,
                        )

                    await proxy.disconnect_all()

                    # noinspection PyTypeChecker
                    with pytest.raises(AMQPFrameError):
                        await read_conn.channel()

                logging.info("Waiting reconnect")
                await reconnect_event.wait()

                logging.info("Waiting connections")
                await asyncio.wait_for(read_conn.ready(), timeout=20)

                for i in range(5, 10):
                    await write_channel.default_exchange.publish(
                        Message(str(i).encode()), queue.name,
                    )

                while len(shared) < 10:
                    await asyncio.sleep(0.1)

                assert len(shared) == 10

                reader_task.cancel()
                await asyncio.gather(reader_task, return_exceptions=True)

                with pytest.raises(QueueEmpty):
                    await queue.get(timeout=0.5)
            finally:
                await queue.purge()
                await queue.delete()


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
    create_connection, create_direct_connection, proxy: TCPProxy, loop
):
    logging.info("Creating connections")
    direct_conn, proxy_conn = await asyncio.gather(
        create_direct_connection(), create_connection(),
    )

    logging.info("Creating channels")
    direct_channel, proxy_channel = await asyncio.gather(
        direct_conn.channel(), proxy_conn.channel(),
    )

    reconnect_event = asyncio.Event()
    proxy_conn.reconnect_callbacks.add(
        lambda *_: reconnect_event.set(), weak=False,
    )

    qname = get_random_name("robust", "exclusive", "queue")

    logging.info("Declaring exclusing queue: %s", qname)
    proxy_queue = await proxy_channel.declare_queue(
        qname, exclusive=True, durable=True,
    )

    logging.info("Disconnecting all proxy connections")
    await proxy.disconnect_all()
    await asyncio.sleep(0.5)

    logging.info("Declaring exclusive queue through direct channel")
    await direct_channel.declare_queue(
        qname, exclusive=True, durable=True,
    )

    async def close_after(delay, closer):
        await asyncio.sleep(delay)
        logging.info("Disconnecting direct connection")
        await closer()
        logging.info("Closed")

    await loop.create_task(close_after(5, direct_conn.close))

    # reconnect fired
    await reconnect_event.wait()

    # Wait method ready
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
        "direct", auto_delete=True, channel=channel,
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
                "dummy", exchange="", routing_key="non_existent",
            )

    # emulate connection/channel restoration of connect_robust
    await channel.reopen()

    # cleanup queue
    incoming_message = await queue.get(timeout=5)
    async with incoming_message.process():
        pass
    await queue.unbind(exchange, routing_key)


@aiomisc.timeout(10)
async def test_robust_duplicate_queue(
    connection: aio_pika.RobustConnection,
    declare_exchange: Callable,
    declare_queue: Callable,
    loop: asyncio.AbstractEventLoop,
    proxy: TCPProxy,
    create_task: Callable,
):
    queue_name = "test"

    channel1 = await connection.channel()
    channel2 = await connection.channel()

    reconnect_event = asyncio.Event()
    connection.reconnect_callbacks.add(
        lambda *_: reconnect_event.set(), weak=False,
    )

    shared = []
    queue1 = await declare_queue(queue_name, channel=channel1, cleanup=False)
    queue2 = await declare_queue(queue_name, channel=channel2, cleanup=False)

    async def reader(queue):
        nonlocal shared
        async with queue.iterator() as q:
            async for message in q:
                shared.append(message)
                await message.ack()

    create_task(reader(queue1))
    create_task(reader(queue2))

    for _ in range(5):
        await channel2.default_exchange.publish(
            aio_pika.Message(b""), queue_name,
        )

    logging.info("Disconnect all clients")
    await proxy.disconnect_all()

    await reconnect_event.wait()

    logging.info("Waiting connections")
    await asyncio.wait([
        channel1._connection.ready(),
        channel2._connection.ready(),
    ])

    for _ in range(5):
        await channel2.default_exchange.publish(
            Message(b""), queue_name,
        )

    while len(shared) < 10:
        await asyncio.sleep(0.1)

    assert len(shared) == 10


async def test_channel_reconnect(
    connection_fabric, loop, amqp_url, proxy: TCPProxy, add_cleanup: Callable,
):
    drop_packets = False

    async def processor(body: bytes) -> bytes:
        return body if not drop_packets else b""

    proxy.set_content_processors(read=processor, write=processor)

    heartbeat = 2
    amqp_url = amqp_url.update_query(heartbeat=heartbeat)

    conn = await connection_fabric(amqp_url, loop=loop)
    assert isinstance(conn, aio_pika.RobustConnection)

    async with conn:
        channel = await conn.channel()
        assert isinstance(channel, aio_pika.RobustChannel)

        async with channel:
            await channel.set_qos(0)
            await channel.set_qos(1)

            drop_packets = True
            with pytest.raises(asyncio.TimeoutError):
                await channel.set_qos(0, timeout=0.5)

            drop_packets = False
            # Wait less then heartbeat * HEARTBEAT_GRACEFUL_PERIOD
            await asyncio.sleep(heartbeat)
            await channel.set_qos(0)
            await channel.set_qos(1)
