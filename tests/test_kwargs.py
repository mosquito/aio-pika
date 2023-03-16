from aiormq.connection import parse_bool, parse_int, parse_timeout

from aio_pika import connect
from aio_pika.connection import Connection
from aio_pika.robust_connection import RobustConnection


class MockConnection(Connection):
    async def connect(self, timeout=None, **kwargs):
        return self


class MockConnectionRobust(RobustConnection):
    async def connect(self, timeout=None, **kwargs):
        return self


VALUE_GENERATORS = {
    parse_int: {
        "-1": -1,
        "0": 0,
        "43": 43,
        "9999999999999999": 9999999999999999,
        "hello": 0,
    },
    parse_bool: {
        "disabled": False,
        "enable": True,
        "yes": True,
        "no": False,
        "": False,
    },
    parse_timeout: {
        "0": 0,
        "Vasyan": 0,
        "0.1": 0.1,
        "0.54": 0.54,
        "1": 1,
        "100": 100,
        "1000:": 0,
    },
}


class TestCase:
    CONNECTION_CLASS = MockConnection

    async def get_instance(self, url, **kwargs):
        return await connect(url, connection_class=self.CONNECTION_CLASS, **kwargs)

    async def test_kwargs(self):
        instance = await self.get_instance("amqp://localhost/")

        for key, parser, default in self.CONNECTION_CLASS.KWARGS_TYPES:
            assert hasattr(instance, key)
            assert getattr(instance, key) is parser(default)

    async def test_kwargs_values(self):
        for key, parser, default in self.CONNECTION_CLASS.KWARGS_TYPES:
            positives = VALUE_GENERATORS[parser]  # type: ignore
            for example, expected in positives.items():  # type: ignore
                instance = await self.get_instance(f"amqp://localhost/?{key}={example}")
                assert hasattr(instance, key)
                assert getattr(instance, key) == expected

                instance = await self.get_instance("amqp://localhost", **{key: example})
                assert hasattr(instance, key)
                assert getattr(instance, key) == expected


class TestCaseRobust(TestCase):
    CONNECTION_CLASS = MockConnectionRobust  # type: ignore
