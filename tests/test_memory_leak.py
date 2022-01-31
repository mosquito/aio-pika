import gc
import weakref

import pytest

import aio_pika


@pytest.mark.skip(reason="temporary skipped")
async def test_leak_unclosed_channel(create_connection):
    rabbitmq_connection = await create_connection()

    weakset = weakref.WeakSet()

    async def f(rabbitmq_connection: aio_pika.Connection, weakset):
        weakset.add(await rabbitmq_connection.channel())

    async with rabbitmq_connection:
        for i in range(5):
            await f(rabbitmq_connection, weakset)

    gc.collect()

    assert len(tuple(weakset)) == 0


@pytest.mark.skip(reason="temporary skipped")
async def test_leak_closed_channel(create_connection):
    rabbitmq_connection = await create_connection()

    weakset = weakref.WeakSet()

    async def f(rabbitmq_connection: aio_pika.Connection, weakset):
        async with rabbitmq_connection.channel() as channel:
            weakset.add(channel)

    async with rabbitmq_connection:
        for i in range(5):
            await f(rabbitmq_connection, weakset)

    gc.collect()

    assert len(tuple(weakset)) == 0
