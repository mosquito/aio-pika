import time
from copy import copy
from datetime import datetime, timezone
from typing import List, Tuple

import shortuuid

from aio_pika import DeliveryMode, Message
from aio_pika.abc import FieldValue, HeadersType, MessageInfo


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
