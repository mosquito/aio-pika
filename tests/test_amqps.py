import ssl
from functools import partial

import pytest

import aio_pika
from tests.test_amqp import TestCaseAmqp


@pytest.fixture(
    scope="module", params=[aio_pika.connect, aio_pika.connect_robust],
)
def connection_fabric(request):
    return request.param


@pytest.fixture
def create_connection(connection_fabric, loop, amqp_url):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.VerifyMode.CERT_NONE

    return partial(
        connection_fabric,
        amqp_url.with_scheme("amqps").with_port(5671),
        loop=loop,
        ssl_context=ssl_context,
    )


async def test_default_context(connection_fabric, amqp_url):
    with pytest.raises(ConnectionError):
        await connection_fabric(
            amqp_url.with_scheme("amqps").with_port(5671),
            ssl_context=None,
        )

    ssl_context = ssl.create_default_context()

    with pytest.raises(ConnectionError):
        await connection_fabric(
            amqp_url.with_scheme("amqps").with_port(5671),
            ssl_context=ssl_context,
        )


class TestCaseAMQPS(TestCaseAmqp):
    pass
