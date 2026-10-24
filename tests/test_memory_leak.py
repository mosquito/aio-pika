import asyncio
import gc
import weakref
from functools import partial

import pytest

import aio_pika
from aio_pika import RobustChannel
from tests import get_random_name


async def test_leak_unclosed_channel(create_connection):
    rabbitmq_connection = await create_connection()

    weakset: weakref.WeakSet[aio_pika.abc.AbstractChannel] = weakref.WeakSet()

    async def f(rabbitmq_connection: aio_pika.Connection, weakset):
        weakset.add(await rabbitmq_connection.channel())

    async with rabbitmq_connection:
        for i in range(5):
            await f(rabbitmq_connection, weakset)

    gc.collect()

    assert len(tuple(weakset)) == 0


async def test_leak_closed_channel(create_connection):
    rabbitmq_connection = await create_connection()

    weakset: weakref.WeakSet[aio_pika.abc.AbstractConnection] = (
        weakref.WeakSet()
    )

    async def f(rabbitmq_connection: aio_pika.Connection, weakset):
        async with rabbitmq_connection.channel() as channel:
            weakset.add(channel)

    async with rabbitmq_connection:
        for i in range(5):
            await f(rabbitmq_connection, weakset)

    gc.collect()

    assert len(tuple(weakset)) == 0


@pytest.fixture
def robust_connection_fabric():
    return aio_pika.connect_robust


@pytest.fixture
def create_robust_connection(robust_connection_fabric, event_loop, amqp_url):
    return partial(robust_connection_fabric, amqp_url, loop=event_loop)


async def test_get_exchange_no_memory_leak(create_robust_connection):
    """Test that repeated get_exchange calls don't create multiple instances.

    This verifies the fix for the memory leak where each call to get_exchange()
    was adding a new exchange instance to the internal storage instead of
    returning the cached one.
    """
    connection = await create_robust_connection()

    weakset: weakref.WeakSet[aio_pika.abc.AbstractExchange] = weakref.WeakSet()

    async with connection:
        channel: RobustChannel = await connection.channel()  # type: ignore
        name = get_random_name("memory", "leak", "exchange")

        # Declare the exchange
        exchange = await channel.declare_exchange(
            name,
            auto_delete=True,
        )
        weakset.add(exchange)

        # Get the exchange multiple times - should return same instance
        for _ in range(10):
            ex = await channel.get_exchange(name)
            weakset.add(ex)

        # Should only have 1 unique instance in the WeakSet
        assert len(tuple(weakset)) == 1

        # Verify internal storage doesn't grow
        assert name in channel._exchanges
        # Should be a single exchange, not a set/list of exchanges
        assert isinstance(
            channel._exchanges[name], aio_pika.abc.AbstractExchange
        )


async def test_get_queue_no_memory_leak(create_robust_connection):
    """Test that repeated get_queue calls don't create multiple instances.

    This verifies the fix for the memory leak where each call to get_queue()
    with passive=True was creating new queue instances instead of returning
    the cached one.
    """
    connection = await create_robust_connection()

    weakset: weakref.WeakSet[aio_pika.abc.AbstractQueue] = weakref.WeakSet()

    async with connection:
        channel: RobustChannel = await connection.channel()  # type: ignore
        name = get_random_name("memory", "leak", "queue")

        # Declare the queue
        queue = await channel.declare_queue(
            name,
            auto_delete=True,
        )
        weakset.add(queue)

        # Get the queue multiple times - should return same instance
        for _ in range(10):
            q = await channel.get_queue(name)
            weakset.add(q)

        # Should only have 1 unique instance in the WeakSet
        assert len(tuple(weakset)) == 1

        # Verify internal storage has only one queue for this name
        assert name in channel._queues
        assert len(channel._queues[name]) == 1


async def test_queue_iterator_no_memory_leak(create_connection):
    """Closed iterators must not stay in memory while the channel is open.

    The channel keeps its closed future until the channel closes, and a
    strong done callback on that future kept every iterator alive.
    """
    connection = await create_connection()

    weakset: weakref.WeakSet[aio_pika.abc.AbstractQueueIterator] = (
        weakref.WeakSet()
    )

    async with connection:
        channel = await connection.channel()
        # An exclusive queue lives while the connection lives, therefore
        # the iterators consume the same queue one after another.
        queue = await channel.declare_queue(
            get_random_name("memory", "leak", "iterator"),
            exclusive=True,
        )

        async def consume() -> None:
            async with queue.iterator() as iterator:
                weakset.add(iterator)

        for _ in range(5):
            await consume()

        # The close future schedules its done callbacks, which hold the
        # last references until the loop runs them.
        await asyncio.sleep(0)
        gc.collect()

        assert len(tuple(weakset)) == 0
        assert not channel.is_closed


async def test_unused_queue_iterator_no_memory_leak(create_connection):
    """An iterator which never consumes must not stay in memory.

    The "async for message in queue" form abandons the iterator instead
    of closing it.
    """
    connection = await create_connection()

    weakset: weakref.WeakSet[aio_pika.abc.AbstractQueueIterator] = (
        weakref.WeakSet()
    )

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(
            get_random_name("memory", "leak", "unused", "iterator"),
            exclusive=True,
        )

        for _ in range(5):
            weakset.add(queue.iterator())

        gc.collect()

        assert len(tuple(weakset)) == 0
        assert not channel.is_closed
