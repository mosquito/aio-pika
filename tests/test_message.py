import asyncio
import time
from copy import copy
from datetime import datetime, timezone
from typing import List, Tuple
from unittest.mock import AsyncMock, Mock

import pytest
from aiormq.abc import DeliveredMessage
from pamqp.commands import Basic
from pamqp.header import ContentHeader

import shortuuid

from aio_pika import DeliveryMode, IncomingMessage, Message
from aio_pika.abc import FieldValue, HeadersType, MessageInfo
from aio_pika.exceptions import ChannelInvalidStateError


def test_message_copy():
    msg1 = Message(
        bytes(shortuuid.uuid(), "utf-8"),
        content_type="application/json",
        content_encoding="text",
        timestamp=datetime(2000, 1, 1),
        headers={"h1": "v1", "h2": "v2"},
    )
    msg2 = copy(msg1)

    msg1.lock()

    assert not msg2.locked


def test_message_info():
    body = bytes(shortuuid.uuid(), "utf-8")

    info = MessageInfo(
        app_id="test",
        body_size=len(body),
        cluster_id=None,
        consumer_tag=None,
        content_encoding="text",
        content_type="application/json",
        correlation_id="1",
        delivery_mode=DeliveryMode.PERSISTENT,
        delivery_tag=None,
        exchange=None,
        expiration=1.5,
        headers={"foo": "bar"},
        message_id=shortuuid.uuid(),
        priority=0,
        redelivered=None,
        reply_to="test",
        routing_key=None,
        timestamp=datetime.fromtimestamp(int(time.time()), tz=timezone.utc),
        type="0",
        user_id="guest",
    )

    msg = Message(
        body=body,
        headers={"foo": "bar"},
        content_type="application/json",
        content_encoding="text",
        delivery_mode=DeliveryMode.PERSISTENT,
        priority=0,
        correlation_id="1",
        reply_to="test",
        expiration=1.5,
        message_id=info["message_id"],
        timestamp=info["timestamp"],
        type="0",
        user_id="guest",
        app_id="test",
    )

    assert info == msg.info()


def test_headers_setter():
    data: HeadersType = {"foo": "bar"}
    data_expected = {"foo": "bar"}

    msg = Message(b"", headers={"bar": "baz"})
    msg.headers = data

    assert msg.headers == data_expected


def test_headers_content():
    data: Tuple[List[FieldValue], ...] = (
        [42, 42],
        [b"foo", b"foo"],
        [b"\00", b"\00"],
    )

    for src, value in data:
        msg = Message(b"", headers={"value": src})
        assert msg.headers["value"] == value


def test_headers_set():
    msg = Message(b"", headers={"header": "value"})

    data = (
        ["header-1", 42, 42],
        ["header-2", b"foo", b"foo"],
        ["header-3", b"\00", b"\00"],
        ["header-4", {"foo": "bar"}, {"foo": "bar"}],
    )

    for name, src, value in data:  # type: ignore
        msg.headers[name] = value  # type: ignore
        assert msg.headers[name] == value  # type: ignore

    assert msg.headers["header"] == "value"


@pytest.fixture
def delivered_message():
    channel = Mock(
        is_closed=False, basic_ack=AsyncMock(), basic_reject=AsyncMock(),
    )
    return DeliveredMessage(
        delivery=Basic.Deliver(delivery_tag=1, redelivered=True),
        header=ContentHeader(properties=Basic.Properties()),
        body=b"message",
        channel=channel,
    )


@pytest.mark.parametrize("reject_on_redelivered", [False, True])
@pytest.mark.parametrize("error_type", [ValueError, asyncio.CancelledError])
async def test_process_preserves_error_on_closed_channel(
    delivered_message, reject_on_redelivered, error_type, caplog,
):
    message = IncomingMessage(delivered_message)
    error = error_type("original processing error")
    with pytest.raises(error_type) as raised:
        async with message.process(reject_on_redelivered=reject_on_redelivered):
            delivered_message.channel.is_closed = True
            raise error
    assert raised.value is error
    delivered_message.channel.basic_ack.assert_not_awaited()
    delivered_message.channel.basic_reject.assert_not_awaited()
    assert not message.processed
    assert "Reject is not sent since channel is closed" in caplog.text


@pytest.mark.parametrize("reject_on_redelivered", [False, True])
async def test_process_preserves_error_when_reject_channel_closes(
    delivered_message, reject_on_redelivered,
):
    message = IncomingMessage(delivered_message)
    delivered_message.channel.basic_reject.side_effect = (
        ChannelInvalidStateError
    )
    error = ValueError("original processing error")
    with pytest.raises(ValueError) as raised:
        async with message.process(reject_on_redelivered=reject_on_redelivered):
            raise error
    assert raised.value is error
    assert not message.processed


async def test_process_success_on_closed_channel_still_fails(delivered_message):
    message = IncomingMessage(delivered_message)
    with pytest.raises(ChannelInvalidStateError):
        async with message.process():
            delivered_message.channel.is_closed = True
    assert not message.processed
    delivered_message.channel.basic_ack.assert_not_awaited()


async def test_process_does_not_hide_other_reject_errors(delivered_message):
    message = IncomingMessage(delivered_message)
    delivered_message.channel.basic_reject.side_effect = RuntimeError("reject")
    with pytest.raises(RuntimeError, match="reject"):
        async with message.process():
            raise ValueError("processing")
