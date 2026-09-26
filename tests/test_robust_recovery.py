import asyncio
import gc
import socket
import struct

import aiomisc
import pytest
from aiomisc_pytest import TCPProxy  # type: ignore
from yarl import URL

from aio_pika import Message, connect_robust
from aio_pika.patterns import Master
from tests.docker_client import DockerClient


async def wait_for_broker(url):
    async with asyncio.timeout(45):
        while True:
            try:
                reader, writer = await asyncio.open_connection(
                    url.host, url.port
                )
                try:
                    writer.write(b"AMQP\x00\x00\x09\x01")
                    await writer.drain()
                    await asyncio.wait_for(reader.readexactly(4), 2)
                    return
                finally:
                    writer.close()
                    await writer.wait_closed()
            except (
                OSError,
                EOFError,
                asyncio.IncompleteReadError,
                TimeoutError,
            ):
                await asyncio.sleep(0.2)


async def rabbitmqctl(client, container, command):
    result = await asyncio.to_thread(
        client._request,
        "POST",
        f"/containers/{container.id}/exec",
        {"Cmd": ["rabbitmqctl", command]},
    )
    exec_id = result["Id"]
    await asyncio.to_thread(
        client._request,
        "POST",
        f"/exec/{exec_id}/start",
        {"Detach": True},
    )
    async with asyncio.timeout(30):
        while True:
            state = await asyncio.to_thread(
                client._request,
                "GET",
                f"/exec/{exec_id}/json",
            )
            if not state["Running"]:
                assert state["ExitCode"] == 0, state
                return
            await asyncio.sleep(0.1)


@pytest.fixture
async def recovery_broker(docker):
    # Restart only this test's broker, never the shared session fixture.
    container = await asyncio.to_thread(
        docker,
        "mosquito/aiormq-rabbitmq",
        ["5672/tcp"],
    )
    url = URL.build(
        scheme="amqp",
        host=container.host,
        port=container.ports["5672/tcp"],
        user="guest",
        password="guest",
        path="//",
    )
    await wait_for_broker(url)
    client = DockerClient()
    try:
        yield client, container, url
    finally:
        await asyncio.to_thread(client.kill, container.id)
        await asyncio.to_thread(client.remove, container.id)


@pytest.mark.parametrize(
    "failure",
    ["tcp_fin", "tcp_reset", "heartbeat", "stop_app", "kill"],
)
@aiomisc.timeout(120)
async def test_restore_topology_and_consumers(
    recovery_broker,
    failure,
    monkeypatch,
):
    client, container, direct_url = recovery_broker
    async with TCPProxy(
        direct_url.host, direct_url.port, buffered=False
    ) as proxy:
        url = direct_url.with_host(proxy.proxy_host).with_port(proxy.proxy_port)
        connection = await connect_robust(
            url.update_query(heartbeat=1, reconnect_interval=0.1),
            timeout=3,
        )
        restored = asyncio.Event()
        disconnected = asyncio.Event()
        close_errors = []

        def on_close(_, exc):
            close_errors.append(exc)
            disconnected.set()

        connection.close_callbacks.add(on_close)
        connection.reconnect_callbacks.add(lambda *_: restored.set())

        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)
            publisher = await connection.channel()
            # Force the dependency before its provider. The normal WeakSet
            # can produce either order, making this regression intermittent.
            monkeypatch.setattr(
                connection,
                "_RobustConnection__channels",
                (channel, publisher),
            )
            exchange = await publisher.declare_exchange("recovery-exchange")
            received: asyncio.Queue[bytes] = asyncio.Queue()

            async def callback(message):
                async with message.process():
                    await received.put(message.body)

            async def register_callback():
                queue = await channel.declare_queue("recovery-callback")
                await queue.bind(exchange, "callback")
                await queue.consume(callback)

            await register_callback()
            queue = await channel.declare_queue("recovery-iterator")
            await queue.bind(exchange, "iterator")
            worker_results: asyncio.Queue[int] = asyncio.Queue()

            async def worker(*, value):
                await worker_results.put(value)

            master = Master(channel)
            # Intentionally discard the Worker and callback queue references,
            # as in applications that only register consumers during startup.
            await master.create_worker(
                "recovery-worker",
                worker,
                durable=True,
                auto_delete=False,
            )
            gc.collect()

            async with queue.iterator() as iterator:

                async def check_delivery(value):
                    body = str(value).encode()
                    await exchange.publish(Message(body), "callback", timeout=5)
                    assert await asyncio.wait_for(received.get(), 5) == body
                    await exchange.publish(Message(body), "iterator", timeout=5)
                    message = await asyncio.wait_for(anext(iterator), 5)
                    assert message.body == body
                    await message.ack()
                    await master.create_task(
                        "recovery-worker", {"value": value}
                    )
                    async with asyncio.timeout(5):
                        while True:
                            result = await worker_results.get()
                            assert result <= value
                            if result == value:
                                break
                            # A crash can happen before the previous worker's
                            # ack reaches the broker. Redelivery is legitimate.

                await check_delivery(0)
                for attempt in (1, 2):
                    restored.clear()
                    disconnected.clear()
                    close_errors.clear()
                    old_transport = connection.transport
                    old_channel = await channel.get_underlay_channel()
                    previous_attempts = connection.connection_attempt

                    if failure == "tcp_fin":
                        await proxy.disconnect_all()
                    elif failure == "tcp_reset":
                        for peer in tuple(proxy.clients):
                            sock = peer.client_writer.get_extra_info("socket")
                            sock.setsockopt(
                                socket.SOL_SOCKET,
                                socket.SO_LINGER,
                                struct.pack("ii", 1, 0),
                            )
                        await proxy.disconnect_all()
                    elif failure == "heartbeat":

                        async def drop_frames(_):
                            return b""

                        # Drop server traffic without closing TCP. The client
                        # must detect missing heartbeats itself.
                        proxy.set_content_processors(None, drop_frames)
                        try:
                            await asyncio.wait_for(disconnected.wait(), 15)
                        finally:
                            proxy.set_content_processors(None, None)
                    elif failure == "stop_app":
                        await rabbitmqctl(client, container, "stop_app")
                        try:
                            await asyncio.wait_for(disconnected.wait(), 5)
                        finally:
                            await rabbitmqctl(client, container, "start_app")
                    else:
                        await asyncio.to_thread(client.kill, container.id)
                        try:
                            await asyncio.wait_for(disconnected.wait(), 5)
                        finally:
                            await asyncio.to_thread(client.start, container.id)
                            # Docker can allocate a new random host port when
                            # restarting. Keep the application's proxy address
                            # stable while updating the broker-side endpoint.
                            info = await asyncio.to_thread(
                                client.inspect,
                                container.id,
                            )
                            proxy.target_port = client._parse_ports(info)[
                                "5672/tcp"
                            ]

                    await asyncio.wait_for(restored.wait(), 45)
                    await asyncio.wait_for(channel.ready(), 5)
                    await asyncio.wait_for(publisher.ready(), 5)
                    assert disconnected.is_set()
                    assert connection.transport is not old_transport
                    assert (
                        await channel.get_underlay_channel() is not old_channel
                    )
                    assert not connection.is_closed
                    if failure == "stop_app":
                        assert any(
                            exc.args and exc.args[0] == 320
                            for exc in close_errors
                        )
                    elif failure == "tcp_reset":
                        assert "reset" in str(close_errors[-1]).lower()
                    await check_delivery(attempt)
                    assert connection.connection_attempt > previous_attempts


@aiomisc.timeout(45)
async def test_missing_external_queue_retries_until_recreated(recovery_broker):
    from aio_pika.exceptions import ChannelNotFoundEntity

    _, _, direct_url = recovery_broker
    async with TCPProxy(
        direct_url.host, direct_url.port, buffered=False
    ) as proxy:
        url = direct_url.with_host(proxy.proxy_host).with_port(proxy.proxy_port)
        connection = await connect_robust(
            url.update_query(reconnect_interval=0.2),
            timeout=3,
        )
        admin_connection = await connect_robust(direct_url)
        async with connection, admin_connection:
            admin = await admin_connection.channel()
            external = await admin.declare_queue("external", robust=False)
            channel = await connection.channel()
            await channel.declare_queue(external.name, passive=True)
            restored = asyncio.Event()
            failed_twice = asyncio.Event()
            errors = set()

            def on_close(_, exc):
                if isinstance(exc, ChannelNotFoundEntity):
                    errors.add(exc)
                    if len(errors) >= 2:
                        failed_twice.set()

            connection.close_callbacks.add(on_close)
            connection.reconnect_callbacks.add(lambda *_: restored.set())
            await external.delete()
            await proxy.disconnect_all()
            await asyncio.wait_for(failed_twice.wait(), 10)
            assert not restored.is_set()

            # Passive declarations cannot repair a missing external resource.
            # The connection must keep retrying, without announcing success.
            await admin.declare_queue(external.name, robust=False)
            await asyncio.wait_for(restored.wait(), 10)
            await asyncio.wait_for(channel.ready(), 5)
