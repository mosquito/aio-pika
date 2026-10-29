import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from yarl import URL

from aio_pika.channel import Channel
from aio_pika.connection import Connection
from aio_pika.exceptions import ChannelInvalidStateError
from aio_pika.robust_channel import RobustChannel
from aio_pika.robust_connection import RobustConnection


@pytest.mark.parametrize(
    "resource_class",
    [Connection, RobustConnection, Channel, RobustChannel],
)
@pytest.mark.parametrize("cancellation", ["cancel", "timeout"])
async def test_close_waiter_cancellation_is_isolated(
    resource_class,
    cancellation,
):
    connection_class = (
        resource_class if issubclass(resource_class, Connection) else Connection
    )
    connection = connection_class(URL("amqp://localhost/"))
    connection.transport = SimpleNamespace(close=AsyncMock())
    connection.connected.set()
    resource = connection
    if issubclass(resource_class, Channel):
        resource = resource_class(connection)
        resource._channel = SimpleNamespace(
            channel=SimpleNamespace(is_closed=False, number=1),
            close=AsyncMock(),
        )

    try:
        first = asyncio.ensure_future(resource.closed())
        second = asyncio.ensure_future(resource.closed())
        notified = asyncio.Event()
        notification = asyncio.ensure_future(resource.closed())
        notification.add_done_callback(lambda future: notified.set())
        assert not resource.is_closed

        if cancellation == "cancel":
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
        else:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(first, timeout=0)

        assert not resource.is_closed
        assert not second.done()
        assert not notified.is_set()

        await resource.close()
        assert await second is True
        await asyncio.wait_for(notified.wait(), timeout=1)
        assert await resource.closed() is True
        assert resource.is_closed
    finally:
        await resource.close()
        if resource is not connection:
            await connection.close()


@pytest.mark.parametrize("channel_class", [Channel, RobustChannel])
async def test_channel_close_before_initialize(channel_class):
    channel = channel_class(Connection(URL("amqp://localhost/")))
    assert not channel.closed().done()

    await channel.close()

    assert channel.is_closed
    assert channel.closed().done()
    assert await channel.closed() is True

    await channel.close()
    assert await channel.closed() is True

    with pytest.raises(RuntimeError, match="Can't initialize closed channel"):
        await channel.initialize()


@pytest.mark.parametrize("channel_class", [Channel, RobustChannel])
async def test_channel_close_after_initialize_failure(channel_class):
    channel = channel_class(Connection(URL("amqp://localhost/")))

    try:
        with pytest.raises(ChannelInvalidStateError, match="No active"):
            await channel.initialize()
    finally:
        await channel.close()

    assert channel.closed().done()
    assert await channel.closed() is True
