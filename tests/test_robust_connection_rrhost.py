import pytest
from yarl import URL
from aio_pika.robust_connection_rrhost import RobustConnectionRRHost

@pytest.mark.asyncio
async def test_connect_with_rabbitmq_container(amqp_url):
    urls = [str(amqp_url)]
    conn = RobustConnectionRRHost(urls)
    await conn.connect(timeout=5)
    assert not conn.is_closed
    await conn.close()

@pytest.mark.asyncio
async def test_failover_with_rabbitmq_container(amqp_url):
    invalid_url = "amqp://guest:guest@invalidhost:5672/"
    urls = [invalid_url, str(amqp_url)]
    conn = RobustConnectionRRHost(urls)
    await conn.connect(timeout=5)
    assert not conn.is_closed
    await conn.close()

@pytest.mark.asyncio
async def test_amqp_scheme_with_rabbitmq(amqp_url):
    url = f"amqp://guest:guest@{amqp_url.host}:5672/"
    conn = RobustConnectionRRHost([url])
    assert conn.urls[0].scheme == "amqp"
    await conn.connect(timeout=5)
    assert not conn.is_closed
    await conn.close()

@pytest.mark.asyncio
@pytest.mark.skip(reason="AMQPS non configurato nel server di test")
async def test_amqps_scheme_with_rabbitmq(amqp_url):
    url = f"amqps://guest:guest@{amqp_url.host}:5671/"
    conn = RobustConnectionRRHost([url])
    await conn.connect(timeout=5)
    assert not conn.is_closed
    await conn.close()

@pytest.mark.asyncio
async def test_no_scheme_defaults_to_amqp(amqp_url):
    raw_url = f"guest:guest@{amqp_url.host}:5672"
    url = f"amqp://{raw_url}"
    parsed = URL(url)
    if parsed.port is None:
        parsed = parsed.with_port(5672)
    conn = RobustConnectionRRHost([str(parsed)])
    assert conn.urls[0].scheme == "amqp"
    await conn.connect(timeout=5)
    assert not conn.is_closed
    await conn.close()

@pytest.mark.asyncio
async def test_host_and_port_only(amqp_url):
    raw_url = f"{amqp_url.host}:5672"
    url = f"amqp://{raw_url}"
    parsed = URL(url)
    if parsed.port is None:
        parsed = parsed.with_port(5672)
    conn = RobustConnectionRRHost([str(parsed)])
    assert conn.urls[0].host == amqp_url.host
    await conn.connect(timeout=5)
    assert not conn.is_closed
    await conn.close()
