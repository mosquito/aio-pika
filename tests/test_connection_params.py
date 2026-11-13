from typing import Type

from aiormq.connection import parse_bool, parse_int, parse_timeout
from yarl import URL

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
        "0": 0.0,
        "0.0": 0.0,
        ".0": 0.0,
        "0.1": 0.1,
        "1": 1.0,
        "hello": None,
    },
}


class TestCase:
    CONNECTION_CLASS: Type[AbstractConnection] = MockConnection

    async def get_instance(self, url, **kwargs) -> AbstractConnection:
        return await connect(  # type: ignore
            url,
            connection_class=self.CONNECTION_CLASS,
            **kwargs,
        )

    async def test_kwargs(self):
        instance = await self.get_instance("amqp://localhost/")

        for parameter in self.CONNECTION_CLASS.PARAMETERS:
            if parameter.is_kwarg:
                continue

            assert hasattr(instance, parameter.name)
            assert getattr(instance, parameter.name) is parameter.parse(
                parameter.default
            )

    async def test_kwargs_values(self):
        for parameter in self.CONNECTION_CLASS.PARAMETERS:
            positives = VALUE_GENERATORS[parameter.parser]  # type: ignore
            for example, expected in positives.items():  # type: ignore
                instance = await self.get_instance(
                    f"amqp://localhost/?{parameter.name}={example}",
                )

                assert parameter.parse(example) == expected

                if parameter.is_kwarg:
                    assert instance.kwargs[parameter.name] == expected
                else:
                    assert hasattr(instance, parameter.name)
                    assert getattr(instance, parameter.name) == expected

                    instance = await self.get_instance(
                        "amqp://localhost",
                        **{parameter.name: example},
                    )
                    assert hasattr(instance, parameter.name)
                    assert getattr(instance, parameter.name) == expected


class TestCaseRobust(TestCase):
    CONNECTION_CLASS: Type[MockConnectionRobust] = MockConnectionRobust

    async def get_instance(self, url, **kwargs) -> AbstractConnection:
        return await connect_robust(  # type: ignore
            url,
            connection_class=self.CONNECTION_CLASS,  # type: ignore
            **kwargs,
        )


def test_connection_interleave(amqp_url: URL):
    url = amqp_url.update_query(interleave="1")
    connection = Connection(url=url)
    assert "interleave" in connection.kwargs
    assert connection.kwargs["interleave"] == 1

    connection = Connection(url=amqp_url)
    assert "interleave" not in connection.kwargs


def test_connection_happy_eyeballs_delay(amqp_url: URL):
    url = amqp_url.update_query(happy_eyeballs_delay=".1")
    connection = Connection(url=url)
    assert "happy_eyeballs_delay" in connection.kwargs
    assert connection.kwargs["happy_eyeballs_delay"] == 0.1

    connection = Connection(url=amqp_url)
    assert "happy_eyeballs_delay" not in connection.kwargs


def test_connection_str_masks_password():
    url = URL("amqp://guest:secret@localhost:5672/vhost")
    assert "secret" not in str(Connection(url=url))
    assert "guest:******@localhost" in str(Connection(url=url))


def test_connection_str_masks_unparsed_password():
    # yarl can not parse these URLs as an authority, so ``url.password`` is
    # None and the previous masking did not apply (#753)
    for raw in ("guest:secret@host", "amqp:/guest:secret@host"):
        connection = Connection(url=URL(raw))
        assert "secret" not in str(connection), raw
        assert "guest:******@" in str(connection), raw


def test_connection_str_without_credentials_untouched():
    for raw in (
        "amqp://user@localhost",
        "amqp://localhost/vhost",
        "amqp://localhost:5672/vhost?timeout=5",
    ):
        assert str(Connection(url=URL(raw))) == raw


def test_robust_connection_interleave(amqp_url: URL):
    url = amqp_url.update_query(interleave="1")
    connection = RobustConnection(url=url)
    assert "interleave" in connection.kwargs
    assert connection.kwargs["interleave"] == 1

    connection = RobustConnection(url=amqp_url)
    assert "interleave" not in connection.kwargs


def test_robust_connection_happy_eyeballs_delay(amqp_url: URL):
    url = amqp_url.update_query(happy_eyeballs_delay=".1")
    connection = RobustConnection(url=url)
    assert "happy_eyeballs_delay" in connection.kwargs
    assert connection.kwargs["happy_eyeballs_delay"] == 0.1

    connection = RobustConnection(url=amqp_url)
    assert "happy_eyeballs_delay" not in connection.kwargs
