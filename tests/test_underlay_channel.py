import asyncio
from typing import cast
from unittest.mock import AsyncMock

import pytest
from aiormq.abc import AbstractConnection
from aiormq.base import Base

from aio_pika.abc import UnderlayChannel


class ClosingResource(Base):
    connection: "ClosingResource"

    def __init__(self):
        super().__init__(loop=asyncio.get_running_loop())
        self.observers = []
        self.ready = AsyncMock()
        self.channel = AsyncMock()

    @property
    def closing(self):
        observer = super().closing
        self.observers.append(observer)
        return observer

    async def _on_close(self, exc=None):
        pass

    def __str__(self):
        return "test resource"


@pytest.mark.parametrize("close_from_broker", [False, True])
async def test_channel_close_releases_connection_observer(close_from_broker):
    connection = ClosingResource()
    channel = ClosingResource()
    channel.connection = connection
    connection.channel.return_value = channel
    callback = AsyncMock()
    underlay = await UnderlayChannel.create(
        cast(AbstractConnection, connection),
        callback,
    )
    observer = connection.observers[0]
    channel_observer = channel.observers[0]

    if close_from_broker:
        await channel.close()
        await underlay.close_callback.wait()
    else:
        await underlay.close()

    # aiormq 7 creates a fresh observer on every access. Cancelling the
    # original observer detaches it without closing the connection.
    assert observer.cancelled()
    assert not connection.is_closed
    assert len(connection.observers) == 1
    assert len(channel.observers) == 1
    callback.assert_awaited_once_with(channel_observer)

    await underlay.close()
    await connection.close()
    callback.assert_awaited_once()


@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError])
async def test_failed_channel_open_releases_connection_observer(error_type):
    connection = ClosingResource()
    connection.channel.side_effect = error_type("channel open failed")
    callback = AsyncMock()

    with pytest.raises(error_type, match="channel open failed"):
        await UnderlayChannel.create(
            cast(AbstractConnection, connection),
            callback,
        )

    observer = connection.observers[0]
    assert observer.cancelled()
    assert not connection.is_closed
    await connection.close()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    callback.assert_not_awaited()
