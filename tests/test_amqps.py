import ssl
from functools import partial

import pytest
from yarl import URL

import aio_pika
from tests import test_amqp as amqp
from tests.docker_client import ContainerInfo


@pytest.fixture(
    scope="module", params=[aio_pika.connect, aio_pika.connect_robust],
)
def connection_fabric(request):
    return request.param


@pytest.fixture
def create_connection(
    connection_fabric, event_loop, rabbitmq_container: ContainerInfo,
):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.VerifyMode.CERT_NONE

    amqps_url = URL.build(
        scheme="amqps", user="guest", password="guest", path="//",
        host=rabbitmq_container.host,
        port=rabbitmq_container.ports["5671/tcp"],
    )

    return partial(
        connection_fabric,
        amqps_url,
        loop=event_loop,
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


class TestCaseAMQPS(amqp.TestCaseAmqp):
    pass
