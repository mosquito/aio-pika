import pytest
from yarl import URL
from aio_pika.robust_connection_rrhost import RobustConnectionRRHost


@pytest.mark.asyncio
async def test_connect_with_rabbitmq_container(amqp_url):
    conn = RobustConnectionRRHost([str(amqp_url)])
    await conn.connect(timeout=2)
    assert conn.urls[0].scheme == "amqp"
    assert conn._connection is not None


@pytest.mark.asyncio
async def test_failover_with_rabbitmq_container(amqp_url):
    urls = ["amqp://guest:guest@invalidhost:5672/", str(amqp_url)]
    conn = RobustConnectionRRHost(urls)
    await conn.connect(timeout=2)
    assert any(u.scheme == "amqp" for u in conn.urls)
    assert conn._connection is not None


@pytest.mark.asyncio
async def test_amqp_scheme_with_rabbitmq(amqp_url):
    url = f"amqp://guest:guest@{amqp_url.host}:5672/"
    conn = RobustConnectionRRHost([url])
    assert conn.urls[0].scheme == "amqp"


@pytest.mark.asyncio
@pytest.mark.skip(reason="AMQPS not configured on the test server")
async def test_amqps_scheme_with_rabbitmq(amqp_url):
    url = f"amqps://guest:guest@{amqp_url.host}:5671/"
    conn = RobustConnectionRRHost([url])
    await conn.connect(timeout=2)
    assert conn.urls[0].scheme == "amqps"
    assert conn._connection is not None


@pytest.mark.asyncio
async def test_no_scheme_defaults_to_amqp(amqp_url):
    raw_url = f"guest:guest@{amqp_url.host}:5672"
    url = f"amqp://{raw_url}"
    parsed = URL(url)
    conn = RobustConnectionRRHost([str(parsed)])
    assert conn.urls[0].scheme == "amqp"


@pytest.mark.asyncio
async def test_host_and_port_only(amqp_url):
    raw_url = f"{amqp_url.host}:5672"
    url = f"amqp://{raw_url}"
    parsed = URL(url)
    conn = RobustConnectionRRHost([str(parsed)])
    assert conn.urls[0].host == amqp_url.host
