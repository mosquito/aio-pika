"""Regression tests for issue #688: prefetched message consumer callback
exceptions (e.g. ``ChannelInvalidStateError`` raised by ``message.ack()``)
were silently dropped by aiormq's consumer task wrapper.  The worker
stopped processing further prefetched messages without any log output.

These tests verify that exceptions raised inside the consumer callback
delivered to ``Queue.consume`` are logged instead of being silently
swallowed by the wrapper task.
"""

from __future__ import annotations

import asyncio
import logging
from typing import List
from unittest import mock

import pytest

from aio_pika.abc import AbstractIncomingMessage
from aio_pika.queue import consumer


def _make_delivered_message(
    delivery_tag: int = 1,
    body: bytes = b"body",
) -> mock.MagicMock:
    """Build a mock aiormq DeliveredMessage compatible with IncomingMessage."""

    properties = mock.MagicMock()
    properties.content_type = None
    properties.content_encoding = None
    properties.headers = None
    properties.delivery_mode = None
    properties.priority = None
    properties.correlation_id = None
    properties.reply_to = None
    properties.expiration = None
    properties.message_id = None
    properties.timestamp = None
    properties.message_type = None
    properties.user_id = None
    properties.app_id = None
    properties.cluster_id = None

    header = mock.MagicMock()
    header.properties = properties

    mock_channel = mock.MagicMock()
    mock_channel.is_closed = False
    mock_channel.connection.basic_nack = True

    delivered = mock.MagicMock()
    delivered.channel = mock_channel
    delivered.body = body
    delivered.consumer_tag = "ctag-1"
    delivered.delivery_tag = delivery_tag
    delivered.redelivered = False
    delivered.routing_key = ""
    delivered.exchange = ""
    delivered.message_count = 0
    delivered.header = header
    return delivered


async def test_consumer_logs_callback_exceptions(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``consumer`` must log exceptions raised by the user callback
    instead of letting them be silently dropped by aiormq's task wrapper
    (see issue #688)."""
    delivered = _make_delivered_message()
    seen: List[AbstractIncomingMessage] = []

    async def exploding(message: AbstractIncomingMessage) -> None:
        seen.append(message)
        raise RuntimeError("simulated network failure during ack")

    with caplog.at_level(logging.ERROR, logger="aio_pika.queue"):
        # Must NOT re-raise: aiormq wraps the consumer in an asyncio.Task
        # and any uncaught exception there would be reported as
        # "Task exception was never retrieved".
        await consumer(exploding, delivered, no_ack=False)

    assert seen, "consumer callback was not invoked"
    queue_records = [r for r in caplog.records if r.name == "aio_pika.queue"]
    assert any(
        "simulated network failure during ack" in r.getMessage()
        for r in queue_records
    ), (
        "Expected the consumer wrapper to log the callback exception via "
        "the aio_pika.queue logger; "
        f"got queue records: {[r.getMessage() for r in queue_records]}"
    )


async def test_consumer_propagates_cancellation() -> None:
    """``asyncio.CancelledError`` must still propagate so the aiormq
    task wrapper can shut down cleanly."""

    delivered = _make_delivered_message()

    async def cancelled(message: AbstractIncomingMessage) -> None:
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await consumer(cancelled, delivered, no_ack=False)


async def test_consumer_returns_callback_result() -> None:
    """Successful callbacks must still return their result."""

    delivered = _make_delivered_message()

    async def returns_value(message: AbstractIncomingMessage) -> str:
        return "ok"

    result = await consumer(returns_value, delivered, no_ack=False)
    assert result == "ok"
