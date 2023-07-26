from typing import Type

from aiormq.connection import parse_bool, parse_int, parse_timeout

from aio_pika import connect
from aio_pika.abc import AbstractConnection
from aio_pika.connection import Connection
from aio_pika.robust_connection import RobustConnection, connect_robust


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
    float: {
        "0": 0.,
        "0.0": 0.,
        ".0": 0.,
        "0.1": 0.1,
        "1": 1.,
        "hello": None,
    },
}


class TestCase:
    CONNECTION_CLASS: Type[AbstractConnection] = MockConnection

    async def get_instance(self, url, **kwargs) -> AbstractConnection:
        return await connect(       # type: ignore
            url, connection_class=self.CONNECTION_CLASS, **kwargs,
        )

    async def test_kwargs(self):
        instance = await self.get_instance("amqp://localhost/")

        for parameter in self.CONNECTION_CLASS.KWARGS_TYPES:
            if parameter.kwarg:
                continue

            assert hasattr(instance, parameter.name)
            assert (
                getattr(instance, parameter.name) is
                parameter.parse(parameter.default)
            )

    async def test_kwargs_values(self):
        for parameter in self.CONNECTION_CLASS.KWARGS_TYPES:
            positives = VALUE_GENERATORS[parameter.parser]  # type: ignore
            for example, expected in positives.items():  # type: ignore
                instance = await self.get_instance(
                    f"amqp://localhost/?{parameter.name}={example}",
                )

                assert parameter.parse(example) == expected

                if parameter.kwarg:
                    assert instance.kwargs[parameter.name] == expected
                else:
                    assert hasattr(instance, parameter.name)
                    assert getattr(instance, parameter.name) == expected

                    instance = await self.get_instance(
                        "amqp://localhost", **{parameter.name: example},
                    )
                    assert hasattr(instance, parameter.name)
                    assert getattr(instance, parameter.name) == expected


class TestCaseRobust(TestCase):
    CONNECTION_CLASS: Type[MockConnectionRobust] = MockConnectionRobust

    async def get_instance(self, url, **kwargs) -> AbstractConnection:
        return await connect_robust(        # type: ignore
            url,
            connection_class=self.CONNECTION_CLASS,  # type: ignore
            **kwargs,
        )
