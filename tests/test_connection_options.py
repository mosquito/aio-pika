import asyncio
from unittest.mock import AsyncMock, Mock
from typing import assert_type

import aiormq.connection
import pytest
from yarl import URL

from aio_pika import Connection, RobustConnection, connect, connect_robust
from aio_pika.abc import SSLOptions
from aio_pika.connection import make_url


@pytest.mark.parametrize("url_type", [str, URL])
def test_make_url_merges_options(url_type):
    url = url_type("amqps://user:pass@broker:5678/vhost?heartbeat=10&name=base")
    options: SSLOptions = {"cafile": "/ca.pem", "no_verify_ssl": True}
    result = make_url(url, heartbeat=20, name=None, ssl_options=options)
    assert result == URL(
        "amqps://user:pass@broker:5678/vhost?heartbeat=20&name=base"
        "&cafile=/ca.pem&no_verify_ssl=1"
    )
    assert options == {"cafile": "/ca.pem", "no_verify_ssl": True}


async def test_connection_keeps_url_parameters_with_kwargs():
    connection = RobustConnection(
        URL("amqp://localhost/?reconnect_interval=7&fail_fast=0&interleave=2"),
        reconnect_interval=3,
        interleave=None,
    )
    assert connection.reconnect_interval == 3
    assert connection.fail_fast is False
    assert connection.kwargs["interleave"] == 2
    await connection.close()


@pytest.mark.parametrize("factory", [connect, connect_robust])
@pytest.mark.parametrize("with_url", [False, True])
async def test_connection_factory_passes_properties_and_timeout(
    factory, with_url
):
    properties = {"connection_name": "worker", "custom": {"enabled": True}}
    fake = Mock(connect=AsyncMock())
    constructor = Mock(return_value=fake)
    kwargs = {"url": "amqp://broker/?heartbeat=10"} if with_url else {}
    result = await factory(
        **kwargs,
        connection_class=constructor,
        timeout=5,
        client_properties=properties,
        heartbeat=20,
    )
    assert result is fake
    fake.connect.assert_awaited_once_with(timeout=5)
    assert constructor.call_args.kwargs["client_properties"] == properties
    url = constructor.call_args.args[0]
    assert url.query["heartbeat"] == "20"
    assert "connection_name" not in url.query
    assert "custom" not in url.query


@pytest.mark.parametrize("factory", [connect, connect_robust])
async def test_client_properties_reach_amqp_handshake(
    amqp_url, factory, monkeypatch
):
    frames = []
    original = aiormq.connection.Connection._client_properties

    def properties_for_handshake(self, **kwargs):
        properties = original(self, **kwargs)
        frames.append(properties)
        return properties

    monkeypatch.setattr(
        aiormq.connection.Connection,
        "_client_properties",
        properties_for_handshake,
    )
    properties = {"connection_name": "worker", "custom": {"enabled": True}}
    connection = await factory(
        amqp_url.update_query(name="url-name", heartbeat=60),
        timeout=5,
        client_properties=properties,
        heartbeat=30,
        reconnect_interval=0.1,
    )
    async with connection:
        assert connection.url.query["heartbeat"] == "30"
        assert frames[-1]["connection_name"] == "worker"
        assert frames[-1]["custom"] == {"enabled": True}
        async with connection.channel() as channel:
            await channel.declare_queue(exclusive=True)
        if isinstance(connection, RobustConnection):
            restored = asyncio.Event()
            connection.reconnect_callbacks.add(lambda *_: restored.set())
            transport = connection.transport
            assert transport is not None
            await transport.connection.close(asyncio.CancelledError)
            await asyncio.wait_for(restored.wait(), 10)
            assert len(frames) == 2
            assert frames[-1]["connection_name"] == "worker"
            assert frames[-1]["custom"] == {"enabled": True}
    assert properties == {
        "connection_name": "worker",
        "custom": {"enabled": True},
    }


@pytest.mark.parametrize("factory", [connect, connect_robust])
async def test_custom_connection_without_client_properties(factory):
    fake = Mock(connect=AsyncMock())

    def constructor(url, *, loop=None, ssl_context=None):
        return fake

    assert (
        await factory(
            "amqp://localhost/",
            connection_class=constructor,
        )
        is fake
    )


async def test_typed_connection_options(amqp_url: URL) -> None:
    class CustomConnection(Connection):
        pass

    class CustomRobustConnection(RobustConnection):
        pass

    ordinary = await connect(amqp_url, heartbeat=30)
    assert_type(ordinary, Connection)
    await ordinary.close()
    robust = await connect_robust(amqp_url, reconnect_interval=0.1)
    assert_type(robust, RobustConnection)
    await robust.close()
    custom = await connect(
        amqp_url,
        connection_class=CustomConnection,
        heartbeat=30,
    )
    assert_type(custom, CustomConnection)
    await custom.close()
    custom_robust = await connect_robust(
        amqp_url,
        connection_class=CustomRobustConnection,
        reconnect_interval=0.1,
    )
    assert_type(custom_robust, CustomRobustConnection)
    await custom_robust.close()
