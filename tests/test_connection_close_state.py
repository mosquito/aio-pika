import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from yarl import URL

from aio_pika.connection import Connection
from aio_pika.robust_connection import RobustConnection


@pytest.mark.parametrize("connection_class", [Connection, RobustConnection])
async def test_close_before_connect(connection_class):
    connection = connection_class(URL("amqp://localhost/"))

    await connection.close()

    assert connection.is_closed
    assert connection.close_called
    assert connection.closed().done()
    assert await connection.closed() is True

    await connection.close()
    assert await connection.closed() is True


@pytest.mark.parametrize("connection_class", [Connection, RobustConnection])
async def test_close_after_connect_failure(connection_class):
    connection = connection_class(URL("amqp://localhost/"))

    with patch(
        "aio_pika.connection.UnderlayConnection.connect",
        side_effect=OSError("connection unavailable"),
    ):
        try:
            with pytest.raises(OSError, match="connection unavailable"):
                await connection.connect(timeout=0.1)
        finally:
            await connection.close()

    assert connection.is_closed
    assert connection.closed().done()
    assert await connection.closed() is True


@pytest.mark.parametrize("connection_class", [Connection, RobustConnection])
async def test_concurrent_close_does_not_finish_before_transport(
    connection_class,
):
    connection = connection_class(URL("amqp://localhost/"))
    started = asyncio.Event()
    release = asyncio.Event()
    error = RuntimeError("close reason")

    async def close_transport(exc):
        started.set()
        await release.wait()

    transport = SimpleNamespace(close=AsyncMock(side_effect=close_transport))
    connection.transport = transport
    closing = asyncio.create_task(connection.close(error))
    try:
        await started.wait()
        await connection.close()
        assert not connection.closed().done()
        assert not connection.is_closed
    finally:
        release.set()
        await closing

    transport.close.assert_awaited_once_with(error)
    assert await connection.closed() is True


@pytest.mark.parametrize("connection_class", [Connection, RobustConnection])
async def test_reentrant_close_does_not_finish_notification(connection_class):
    connection = connection_class(URL("amqp://localhost/"))

    async def close_transport(exc):
        await connection.close()
        assert not connection.closed().done()

    connection.transport = SimpleNamespace(
        close=AsyncMock(side_effect=close_transport),
    )

    await connection.close()

    assert await connection.closed() is True


@pytest.mark.parametrize("connection_class", [Connection, RobustConnection])
@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError])
async def test_close_retry_after_transport_error(connection_class, error_type):
    connection = connection_class(URL("amqp://localhost/"))
    error = error_type("transport close failed")
    connection.transport = SimpleNamespace(
        close=AsyncMock(side_effect=error),
    )

    with pytest.raises(error_type) as caught:
        await connection.close()

    assert caught.value is error
    assert not connection.closed().done()

    await connection.close()

    assert connection.closed().done()
    assert await connection.closed() is True
