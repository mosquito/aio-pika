import time
from copy import copy
from datetime import datetime

import shortuuid

from aio_pika import DeliveryMode, Message


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

    info = {
        "headers": {"foo": "bar"},
        "content_type": "application/json",
        "content_encoding": "text",
        "delivery_mode": DeliveryMode.PERSISTENT.value,
        "priority": 0,
        "correlation_id": "1",
        "reply_to": "test",
        "expiration": 1.5,
        "message_id": shortuuid.uuid(),
        "timestamp": datetime.utcfromtimestamp(int(time.time())),
        "type": "0",
        "user_id": "guest",
        "app_id": "test",
        "body_size": len(body),
    }

    msg = Message(
        body=body,
        headers={"foo": "bar"},
        content_type="application/json",
        content_encoding="text",
        delivery_mode=DeliveryMode.PERSISTENT,
        priority=0,
        correlation_id=1,
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
    data = {"foo": "bar"}
    data_expected = {"foo": "bar"}

    msg = Message(b"", headers={"bar": "baz"})
    msg.headers = data

    assert msg.headers_raw == data_expected


def test_headers_content():
    data = (
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
        ["header-1", 42,  42],
        ["header-2", b"foo", b"foo"],
        ["header-3", b"\00", b"\00"],
    )

    for name, src, value in data:
        msg.headers[name] = value
        assert msg.headers[name] == value

    assert msg.headers["header"] == "value"
