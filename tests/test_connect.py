from yarl import URL

from aio_pika import connect
from tests import BaseTestCase


class TestConnectFunction(BaseTestCase):
    class FakeConnection:
        def __init__(self, url, **kwargs):
            self.url = URL(url)
            self.kwargs = kwargs

        async def connect(self):
            return

    VARIANTS = (
        (dict(url="amqp://localhost/"), "amqp://localhost/"),
        (dict(url="amqp://localhost"), "amqp://localhost/"),
        (dict(url="amqp://localhost:5674"), "amqp://localhost:5674/"),
        (dict(url="amqp://localhost:5674//"), "amqp://localhost:5674//"),
        (dict(url="amqp://localhost:5674/"), "amqp://localhost:5674/"),
        (
            dict(host="localhost", port=8888),
            "amqp://guest:guest@localhost:8888//"
        ),
        (
            dict(host="localhost", port=8888, virtualhost="foo"),
            "amqp://guest:guest@localhost:8888/foo"
        ),
        (
            dict(host="localhost", port=8888, virtualhost="/foo"),
            "amqp://guest:guest@localhost:8888//foo"
        ),
    )

    async def test_simple(self):
        for kwargs, excpected in self.VARIANTS:
            with self.subTest(excpected):
                conn = await connect(
                    connection_class=self.FakeConnection,
                    **kwargs
                )

                self.assertEqual(conn.url, URL(excpected))
