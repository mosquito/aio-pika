import asyncio
import logging
from unittest.mock import AsyncMock, Mock

import pytest
from aiormq.exceptions import ChannelInvalidStateError, ConnectionClosed

from aio_pika import (
    RobustChannel,
    RobustConnection,
    RobustQueue,
    connect_robust,
)


@pytest.mark.parametrize("closed", [False, True])
async def test_restore_waiting_for_lock_stops_on_connection_close(
    closed,
    monkeypatch,
):
    connection = Mock(is_closed=False, close_called=False)
    channel = RobustChannel(connection)
    reopen = AsyncMock()
    monkeypatch.setattr(channel, "reopen", reopen)
    lock = getattr(channel, "_RobustChannel__restore_lock")
    await lock.acquire()
    task = asyncio.create_task(channel.restore())
    await asyncio.sleep(0)
    connection.is_closed = closed
    connection.close_called = not closed
    lock.release()
    await asyncio.wait_for(task, 1)
    reopen.assert_not_awaited()


@pytest.mark.parametrize("closed", [False, True])
async def test_channel_operations_fail_after_connection_close(closed):
    # A closed RobustConnection never becomes ready again (#253)
    connection = Mock(is_closed=closed, close_called=not closed)
    connection.ready = AsyncMock(side_effect=asyncio.Event().wait)
    channel = RobustChannel(connection)
    with pytest.raises(ChannelInvalidStateError):
        await asyncio.wait_for(channel.get_underlay_channel(), 1)
    with pytest.raises(ChannelInvalidStateError):
        await asyncio.wait_for(channel.ready(), 1)


@pytest.mark.parametrize("closed", [False, True])
async def test_queue_iterator_fails_after_connection_close(closed):
    # The iterator must not retry forever after a deliberate close (#253)
    connection = Mock(is_closed=closed, close_called=not closed)
    connection.ready = AsyncMock(side_effect=asyncio.Event().wait)
    queue = RobustQueue(RobustChannel(connection), "test")

    async def enter():
        async with queue.iterator():
            pass

    with pytest.raises(ChannelInvalidStateError):
        await asyncio.wait_for(enter(), 1)
    with pytest.raises(ChannelInvalidStateError):
        await asyncio.wait_for(anext(queue.iterator()), 1)


@pytest.mark.parametrize("explicit_reason", [False, True])
async def test_connection_close_does_not_restore_channels(
    amqp_url,
    explicit_reason,
    monkeypatch,
    caplog,
):
    caplog.set_level(logging.ERROR)
    for _ in range(3):
        connection = await connect_robust(amqp_url)
        channels = [await connection.channel() for _ in range(3)]
        restores = []
        for channel in channels:
            restore = AsyncMock(wraps=channel.restore)
            monkeypatch.setattr(channel, "restore", restore)
            restores.append(restore)
        if explicit_reason:
            await asyncio.wait_for(connection.close(ConnectionClosed), 10)
        else:
            await asyncio.wait_for(connection.close(), 10)
        await connection.close()
        for restore in restores:
            restore.assert_not_awaited()
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.mark.parametrize("shutdown", [False, True])
async def test_close_during_restore_preserves_unrelated_errors(
    shutdown,
    monkeypatch,
):
    connection = Mock(is_closed=False, close_called=False)
    channel = RobustChannel(connection)
    getattr(channel, "_RobustChannel__restored").set()
    error = RuntimeError("transport closed during reopen")

    async def reopen():
        connection.close_called = shutdown
        raise error

    monkeypatch.setattr(channel, "reopen", reopen)
    closing = asyncio.get_running_loop().create_future()
    reason = ConnectionClosed(320, "broker closed connection")
    closing.set_exception(reason)
    if shutdown:
        assert await channel._on_close(closing) is reason
    else:
        with pytest.raises(RuntimeError) as raised:
            await channel._on_close(closing)
        assert raised.value is error


async def test_connection_close_during_channel_reopen(
    amqp_url,
    monkeypatch,
    caplog,
):
    caplog.set_level(logging.ERROR)
    connection = await connect_robust(amqp_url)
    channel = await connection.channel()
    underlay = await channel.get_underlay_channel()
    assert isinstance(channel, RobustChannel)
    assert channel._channel is not None
    close_callback = channel._channel.close_callback
    entered = asyncio.Event()
    release = asyncio.Event()
    original_open = channel._open

    async def delayed_open():
        entered.set()
        await release.wait()
        await original_open()

    monkeypatch.setattr(channel, "_open", delayed_open)
    closing_channel = asyncio.create_task(underlay.close())
    try:
        await asyncio.wait_for(entered.wait(), 10)
        closing_connection = asyncio.create_task(connection.close())
        await asyncio.wait_for(closing_connection, 10)
        assert connection.close_called
        release.set()
        await asyncio.wait_for(
            asyncio.gather(closing_channel, close_callback.wait()),
            10,
        )
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    finally:
        release.set()
        await connection.close()
        await asyncio.gather(closing_channel, return_exceptions=True)


async def test_close_wakes_pending_connect(monkeypatch):
    # close() must not leave a pending connect() hanging forever (#746)
    from yarl import URL

    from aio_pika.connection import Connection

    started = asyncio.Event()

    async def stalled_connect(self, timeout=None):
        started.set()
        await asyncio.Event().wait()

    connection = RobustConnection(
        URL("amqp://guest:guest@127.0.0.1/"),
        fail_fast=False,
    )
    monkeypatch.setattr(Connection, "connect", stalled_connect)

    task = asyncio.create_task(connection.connect())
    await asyncio.wait_for(started.wait(), 1)
    await asyncio.sleep(0)
    await asyncio.wait_for(connection.close(), 1)

    with pytest.raises(ConnectionClosed):
        await asyncio.wait_for(task, 1)


async def test_close_wakes_pending_connect_fail_fast(monkeypatch):
    # ... and the same with fail_fast=True, where connect() first waits
    # for the verdict of the very first connection attempt (#746)
    from yarl import URL

    from aio_pika.connection import Connection

    started = asyncio.Event()

    async def stalled_connect(self, timeout=None):
        started.set()
        await asyncio.Event().wait()

    connection = RobustConnection(
        URL("amqp://guest:guest@127.0.0.1/"),
        fail_fast=True,
    )
    monkeypatch.setattr(Connection, "connect", stalled_connect)

    task = asyncio.create_task(connection.connect())
    await asyncio.wait_for(started.wait(), 1)
    await asyncio.sleep(0)
    await asyncio.wait_for(connection.close(), 1)

    with pytest.raises(ConnectionClosed):
        await asyncio.wait_for(task, 1)


async def test_close_wakes_pending_ready_waiters():
    # Callers blocked in ready() (e.g. declare_queue during recovery)
    # must fail instead of staying pending after close() (#746)
    from yarl import URL

    connection = RobustConnection(URL("amqp://guest:guest@127.0.0.1/"))

    waiter = asyncio.create_task(connection.ready())
    await asyncio.sleep(0)
    await asyncio.wait_for(connection.close(), 1)

    with pytest.raises(ConnectionClosed):
        await asyncio.wait_for(waiter, 1)
