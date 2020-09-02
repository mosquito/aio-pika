import asyncio

import pytest
from yarl import URL

from aio_pika import connect


VARIANTS = (
    (dict(url="amqp://localhost/"), "amqp://localhost/"),
    (dict(url="amqp://localhost"), "amqp://localhost/"),
    (dict(url="amqp://localhost:5674"), "amqp://localhost:5674/"),
    (dict(url="amqp://localhost:5674//"), "amqp://localhost:5674//"),
    (dict(url="amqp://localhost:5674/"), "amqp://localhost:5674/"),
    (dict(host="localhost", port=8888), "amqp://guest:guest@localhost:8888//"),
    (
        dict(host="localhost", port=8888, virtualhost="foo"),
        "amqp://guest:guest@localhost:8888/foo",
    ),
    (
        dict(host="localhost", port=8888, virtualhost="/foo"),
        "amqp://guest:guest@localhost:8888//foo",
    ),
)


class FakeConnection:
    def __init__(self, url, **kwargs):
        self.url = URL(url)
        self.kwargs = kwargs

    async def connect(self, timeout=None, **kwargs):
        return


@pytest.mark.parametrize("kwargs,expected", VARIANTS)
def test_simple(kwargs, expected):
    loop = asyncio.get_event_loop()
    # noinspection PyTypeChecker
    conn = loop.run_until_complete(
        connect(connection_class=FakeConnection, **kwargs),
    )

    assert conn.url == URL(expected)
