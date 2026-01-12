import gc
import weakref
from functools import partial
from typing import AbstractSet

import pytest

import aio_pika
from aio_pika import RobustChannel
from tests import get_random_name


async def test_leak_unclosed_channel(create_connection):
    rabbitmq_connection = await create_connection()

    weakset: AbstractSet[aio_pika.abc.AbstractChannel] = weakref.WeakSet()

    async def f(rabbitmq_connection: aio_pika.Connection, weakset):
        weakset.add(await rabbitmq_connection.channel())

    async with rabbitmq_connection:
        for i in range(5):
            await f(rabbitmq_connection, weakset)

    gc.collect()

    assert len(tuple(weakset)) == 0


async def test_leak_closed_channel(create_connection):
    rabbitmq_connection = await create_connection()

    weakset: AbstractSet[aio_pika.abc.AbstractConnection] = weakref.WeakSet()

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

    weakset: AbstractSet[aio_pika.abc.AbstractExchange] = weakref.WeakSet()

    async with connection:
        channel: RobustChannel = await connection.channel()  # type: ignore
        name = get_random_name("memory", "leak", "exchange")

        # Declare the exchange
        exchange = await channel.declare_exchange(
            name, auto_delete=True,
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
        assert isinstance(channel._exchanges[name], aio_pika.abc.AbstractExchange)


async def test_get_queue_no_memory_leak(create_robust_connection):
    """Test that repeated get_queue calls don't create multiple instances.

    This verifies the fix for the memory leak where each call to get_queue()
    with passive=True was creating new queue instances instead of returning
    the cached one.
    """
    connection = await create_robust_connection()

    weakset: AbstractSet[aio_pika.abc.AbstractQueue] = weakref.WeakSet()

    async with connection:
        channel: RobustChannel = await connection.channel()  # type: ignore
        name = get_random_name("memory", "leak", "queue")

        # Declare the queue
        queue = await channel.declare_queue(
            name, auto_delete=True,
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
