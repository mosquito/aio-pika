import asyncio
import uuid
from typing import cast

import aiormq

import pytest

from aio_pika import Message, connect_robust
from aio_pika.abc import AbstractIncomingMessage


pytestmark = pytest.mark.asyncio


async def ignore_message(message):
    pass


async def wait_for(predicate):
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.01)


async def test_broker_cancel_restores_consumer(amqp_url):
    async with await connect_robust(
        amqp_url, reconnect_interval=0.05
    ) as connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(uuid.uuid4().hex)
        received: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()
        tag = await queue.consume(received.put, no_ack=True)
        underlay = cast(aiormq.Channel, await channel.get_underlay_channel())
        cancelled = asyncio.Event()
        underlay.on_consumer_cancel_callbacks.add(lambda _: cancelled.set())
        other = await connection.channel()
        await other.queue_delete(queue.name)
        await asyncio.wait_for(cancelled.wait(), 5)
        await wait_for(lambda: tag in underlay.consumers)
        assert await channel.get_underlay_channel() is underlay
        await channel.default_exchange.publish(
            Message(b"recovered"),
            routing_key=queue.name,
        )
        message = await asyncio.wait_for(received.get(), 5)
        assert message.body == b"recovered"
        await queue.cancel(tag)
        await queue.delete()


async def test_passive_queue_retries_without_closing_main_channel(amqp_url):
    async with await connect_robust(
        amqp_url, reconnect_interval=0.05
    ) as connection:
        other = await connection.channel()
        name = uuid.uuid4().hex
        await other.declare_queue(name)
        channel = await connection.channel()
        queue = await channel.declare_queue(name, passive=True)
        received: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()
        tag = await queue.consume(received.put, no_ack=True)
        underlay = cast(aiormq.Channel, await channel.get_underlay_channel())
        await other.queue_delete(name)
        await wait_for(lambda: tag not in underlay.consumers)
        await asyncio.sleep(0.2)
        assert not underlay.is_closed
        assert not channel.is_closed
        await other.declare_queue(name)
        await wait_for(lambda: tag in underlay.consumers)
        await channel.default_exchange.publish(Message(b"ok"), routing_key=name)
        assert (await asyncio.wait_for(received.get(), 5)).body == b"ok"
        await queue.cancel(tag)
        await queue.delete()


@pytest.mark.parametrize(
    "operation",
    ["cancel", "delete", "channel_close", "connection_close"],
)
async def test_explicit_shutdown_stops_recovery(amqp_url, operation):
    async with await connect_robust(
        amqp_url, reconnect_interval=0.05
    ) as connection:
        other = await connection.channel()
        name = uuid.uuid4().hex
        await other.declare_queue(name)
        channel = await connection.channel()
        queue = await channel.declare_queue(name, passive=True)
        tag = await queue.consume(ignore_message, no_ack=True)
        underlay = cast(aiormq.Channel, await channel.get_underlay_channel())
        await other.queue_delete(name)
        await wait_for(lambda: tag not in underlay.consumers)
        if operation == "cancel":
            await queue.cancel(tag)
        elif operation == "delete":
            await queue.delete(if_unused=False, if_empty=False)
        elif operation == "channel_close":
            await channel.close()
        else:
            await connection.close()
        await asyncio.sleep(0.15)
        assert tag not in underlay.consumers


async def test_recovery_restores_bindings_and_only_cancelled_tags(amqp_url):
    async with await connect_robust(
        amqp_url, reconnect_interval=0.05
    ) as connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange(uuid.uuid4().hex)
        queue = await channel.declare_queue(uuid.uuid4().hex)
        await queue.bind(exchange, "key")
        received: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()
        tags = [
            await queue.consume(received.put, no_ack=True) for _ in range(2)
        ]
        unaffected = await channel.declare_queue(uuid.uuid4().hex)
        other_tag = await unaffected.consume(ignore_message, no_ack=True)
        underlay = cast(aiormq.Channel, await channel.get_underlay_channel())
        callback = underlay.consumers[other_tag]
        other = await connection.channel()
        await other.queue_delete(queue.name)
        await wait_for(lambda: not any(t in underlay.consumers for t in tags))
        await wait_for(lambda: all(t in underlay.consumers for t in tags))
        assert underlay.consumers[other_tag] is callback
        await exchange.publish(Message(b"bound"), routing_key="key")
        assert (await asyncio.wait_for(received.get(), 5)).body == b"bound"
        for tag in tags:
            await queue.cancel(tag)
        await queue.delete()
        await unaffected.cancel(other_tag)
        await unaffected.delete()
        await exchange.delete()


async def test_cancel_during_probe_does_not_restore_consumer(
    amqp_url,
    monkeypatch,
):
    from aio_pika.robust_queue import RobustQueue

    entered = asyncio.Event()
    release = asyncio.Event()
    original_probe = RobustQueue._probe

    async def probe(self, channel):
        await original_probe(self, channel)
        entered.set()
        await release.wait()

    monkeypatch.setattr(RobustQueue, "_probe", probe)
    async with await connect_robust(
        amqp_url, reconnect_interval=0.05
    ) as connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(uuid.uuid4().hex)
        tag = await queue.consume(ignore_message, no_ack=True)
        underlay = cast(aiormq.Channel, await channel.get_underlay_channel())
        other = await connection.channel()
        await other.queue_delete(queue.name)
        await asyncio.wait_for(entered.wait(), 5)
        await queue.cancel(tag)
        release.set()
        await asyncio.sleep(0.15)
        assert tag not in underlay.consumers
        await queue.delete()


async def test_consumer_cancel_hook_survives_reopen(amqp_url):
    async with await connect_robust(
        amqp_url, reconnect_interval=0.05
    ) as connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(uuid.uuid4().hex)
        received: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()
        tag = await queue.consume(received.put, no_ack=True)
        previous = await channel.get_underlay_channel()
        await channel.close()
        await channel.reopen()
        underlay = cast(aiormq.Channel, await channel.get_underlay_channel())
        assert underlay is not previous
        other = await connection.channel()
        await other.queue_delete(queue.name)
        await wait_for(lambda: tag not in underlay.consumers)
        await wait_for(lambda: tag in underlay.consumers)
        await channel.default_exchange.publish(
            Message(b"reopened"),
            routing_key=queue.name,
        )
        assert (await asyncio.wait_for(received.get(), 5)).body == b"reopened"
        await queue.cancel(tag)
        await queue.delete()


@pytest.mark.parametrize("robust_queue", [False, True])
async def test_non_robust_consumers_do_not_recover(amqp_url, robust_queue):
    async with await connect_robust(
        amqp_url, reconnect_interval=0.05
    ) as connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(
            uuid.uuid4().hex,
            robust=robust_queue,
        )
        tag = await queue.consume(
            ignore_message,
            no_ack=True,
            robust=not robust_queue,
        )
        underlay = cast(aiormq.Channel, await channel.get_underlay_channel())
        other = await connection.channel()
        await other.queue_delete(queue.name)
        await wait_for(lambda: tag not in underlay.consumers)
        await asyncio.sleep(0.15)
        assert tag not in underlay.consumers


@pytest.mark.parametrize("close_connection", [False, True])
async def test_close_cancels_inflight_probe(
    amqp_url,
    monkeypatch,
    close_connection,
):
    from aio_pika.robust_queue import RobustQueue

    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def probe(self, channel):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    monkeypatch.setattr(RobustQueue, "_probe", probe)
    async with await connect_robust(
        amqp_url, reconnect_interval=0.05
    ) as connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(uuid.uuid4().hex)
        await queue.consume(ignore_message, no_ack=True)
        other = await connection.channel()
        await other.queue_delete(queue.name)
        await asyncio.wait_for(entered.wait(), 5)
        if close_connection:
            await asyncio.wait_for(connection.close(), 5)
        else:
            await asyncio.wait_for(channel.close(), 5)
        await asyncio.wait_for(cancelled.wait(), 5)


async def test_many_cancelled_queues_share_one_recovery_loop(
    amqp_url,
    monkeypatch,
):
    from aio_pika.robust_channel import RobustChannel
    from aio_pika.robust_queue import RobustQueue

    original_recovery = RobustChannel._restore_consumers
    original_probe = RobustQueue._probe
    tasks = set()
    probes = []

    async def recovery(self):
        tasks.add(asyncio.current_task())
        return await original_recovery(self)

    monkeypatch.setattr(RobustChannel, "_restore_consumers", recovery)

    async def probe(self, channel):
        probes.append(self.name)
        await original_probe(self, channel)

    monkeypatch.setattr(RobustQueue, "_probe", probe)
    async with await connect_robust(
        amqp_url, reconnect_interval=0.05
    ) as connection:
        channel = await connection.channel()
        received: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()
        queues = [
            await channel.declare_queue(uuid.uuid4().hex) for _ in range(20)
        ]
        tags = [await q.consume(received.put, no_ack=True) for q in queues]
        underlay = cast(aiormq.Channel, await channel.get_underlay_channel())
        other = await connection.channel()
        for queue in queues:
            await other.queue_delete(queue.name)
        await wait_for(lambda: len(probes) >= len(queues))
        await wait_for(lambda: all(t in underlay.consumers for t in tags))
        assert tasks == {
            getattr(connection, "_RobustConnection__reconnection_task")
        }
        assert len(probes) == len(queues)
        for queue in queues:
            await channel.default_exchange.publish(
                Message(queue.name.encode()),
                routing_key=queue.name,
            )
        bodies = {
            (await asyncio.wait_for(received.get(), 5)).body for _ in queues
        }
        assert bodies == {q.name.encode() for q in queues}
        for queue, tag in zip(queues, tags):
            await queue.cancel(tag)
            await queue.delete()


async def test_cancel_during_recovery_wakes_next_pass(amqp_url, monkeypatch):
    from aio_pika.robust_queue import RobustQueue

    entered = asyncio.Event()
    release = asyncio.Event()
    original_probe = RobustQueue._probe

    async with await connect_robust(amqp_url, reconnect_interval=0.05) as conn:
        channel = await conn.channel()
        first = await channel.declare_queue(uuid.uuid4().hex)
        second = await channel.declare_queue(uuid.uuid4().hex)
        received: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()
        tags = [
            await q.consume(received.put, no_ack=True) for q in (first, second)
        ]
        underlay = cast(aiormq.Channel, await channel.get_underlay_channel())

        async def probe(self, probe_channel):
            await original_probe(self, probe_channel)
            if self is second:
                entered.set()
                await release.wait()

        monkeypatch.setattr(RobustQueue, "_probe", probe)
        other = await conn.channel()
        await other.queue_delete(second.name)
        await asyncio.wait_for(entered.wait(), 5)
        await other.queue_delete(first.name)
        release.set()
        await wait_for(lambda: all(tag in underlay.consumers for tag in tags))
        for queue in (first, second):
            await channel.default_exchange.publish(
                Message(queue.name.encode()),
                routing_key=queue.name,
            )
        assert {
            (await asyncio.wait_for(received.get(), 5)).body for _ in tags
        } == {first.name.encode(), second.name.encode()}
        for queue, tag in zip((first, second), tags):
            await queue.cancel(tag)
            await queue.delete()


async def test_reconnect_interrupts_probe(amqp_url, monkeypatch):
    from aio_pika.robust_queue import RobustQueue

    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def probe(self, channel):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    monkeypatch.setattr(RobustQueue, "_probe", probe)
    async with await connect_robust(amqp_url, reconnect_interval=0.05) as conn:
        channel = await conn.channel()
        queue = await channel.declare_queue(uuid.uuid4().hex)
        received: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()
        tag = await queue.consume(received.put, no_ack=True)
        previous = await channel.get_underlay_channel()
        other = await conn.channel()
        await other.queue_delete(queue.name)
        await asyncio.wait_for(entered.wait(), 5)
        reconnected = asyncio.Event()
        conn.reconnect_callbacks.add(lambda *_: reconnected.set())
        assert conn.transport is not None
        await conn.transport.connection.close(asyncio.CancelledError)
        await asyncio.wait_for(cancelled.wait(), 5)
        await asyncio.wait_for(reconnected.wait(), 5)
        underlay = cast(aiormq.Channel, await channel.get_underlay_channel())
        assert underlay is not previous
        assert tag in underlay.consumers
        await channel.default_exchange.publish(Message(b"ok"), queue.name)
        assert (await asyncio.wait_for(received.get(), 5)).body == b"ok"
        await queue.cancel(tag)
        await queue.delete()


async def test_cancel_during_consume_does_not_restore_consumer(
    amqp_url,
    monkeypatch,
):
    entered = asyncio.Event()
    release = asyncio.Event()
    original_consume = aiormq.Channel.basic_consume

    async with await connect_robust(amqp_url, reconnect_interval=0.05) as conn:
        channel = await conn.channel()
        queue = await channel.declare_queue(uuid.uuid4().hex)
        tag = await queue.consume(ignore_message, no_ack=True)
        underlay = cast(aiormq.Channel, await channel.get_underlay_channel())

        async def consume(self, *args, **kwargs):
            result = await original_consume(self, *args, **kwargs)
            if self is underlay:
                entered.set()
                await release.wait()
            return result

        monkeypatch.setattr(aiormq.Channel, "basic_consume", consume)
        other = await conn.channel()
        await other.queue_delete(queue.name)
        await asyncio.wait_for(entered.wait(), 5)
        await queue.cancel(tag)
        release.set()
        await asyncio.sleep(0.15)
        assert tag not in underlay.consumers
        await queue.delete()


async def test_queue_disappears_after_probe(amqp_url, monkeypatch):
    from aio_pika.robust_queue import RobustQueue

    original_probe = RobustQueue._probe
    failed_once = False
    async with await connect_robust(amqp_url, reconnect_interval=0.05) as conn:
        other = await conn.channel()
        name = uuid.uuid4().hex
        await other.declare_queue(name)
        channel = await conn.channel()
        queue = await channel.declare_queue(name, passive=True)
        received: asyncio.Queue[AbstractIncomingMessage] = asyncio.Queue()
        tag = await queue.consume(received.put, no_ack=True)
        previous = await channel.get_underlay_channel()

        async def probe(self, probe_channel):
            nonlocal failed_once
            await original_probe(self, probe_channel)
            if self is queue and not failed_once:
                failed_once = True
                await other.queue_delete(name)

        monkeypatch.setattr(RobustQueue, "_probe", probe)
        await other.queue_delete(name)
        await other.declare_queue(name)
        await wait_for(lambda: failed_once and previous.is_closed)
        await asyncio.sleep(0.15)
        await other.declare_queue(name)
        await asyncio.wait_for(channel.ready(), 5)
        underlay = cast(aiormq.Channel, await channel.get_underlay_channel())
        assert tag in underlay.consumers
        await channel.default_exchange.publish(Message(b"ok"), queue.name)
        assert (await asyncio.wait_for(received.get(), 5)).body == b"ok"
        await queue.cancel(tag)
        await queue.delete()
