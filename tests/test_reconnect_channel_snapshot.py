import asyncio
import logging

from aio_pika import Message, connect_robust


async def test_create_channels_during_reconnect(amqp_url, monkeypatch, caplog):
    caplog.set_level(logging.ERROR)
    async with await connect_robust(
        amqp_url.update_query(reconnect_interval=0.1),
    ) as connection:
        existing = await connection.channel()
        restoring = asyncio.Event()
        release = asyncio.Event()
        reconnected = asyncio.Event()
        connection.reconnect_callbacks.add(lambda *_: reconnected.set())
        close_reasons = []
        connection.close_callbacks.add(
            lambda _, exc: close_reasons.append(str(exc)),
        )
        original_restore = existing.restore

        async def delayed_restore(*args, **kwargs):
            restoring.set()
            await release.wait()
            await original_restore(*args, **kwargs)

        monkeypatch.setattr(existing, "restore", delayed_restore)
        transport = connection.transport
        assert transport is not None
        await transport.connection.close(asyncio.CancelledError)
        try:
            await asyncio.wait_for(restoring.wait(), 10)
            # The new transport is connected, but restoration of the original
            # channels is still suspended. Model concurrent health checks.
            channels = [await connection.channel() for _ in range(3)]
            queues = [await ch.declare_queue(exclusive=True) for ch in channels]
        finally:
            release.set()
        await asyncio.wait_for(reconnected.wait(), 10)
        assert not any(
            "Set changed size during iteration" in reason
            for reason in close_reasons
        ), close_reasons
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

        for channel, queue in zip(channels, queues):
            await channel.default_exchange.publish(
                Message(b"healthy"), queue.name
            )
            message = await queue.get(timeout=5)
            assert message is not None and message.body == b"healthy"
            await message.ack()

        # Newly registered channels must participate in the next restoration.
        previous = [await ch.get_underlay_channel() for ch in channels]
        reconnected.clear()
        transport = connection.transport
        assert transport is not None
        await transport.connection.close(asyncio.CancelledError)
        await asyncio.wait_for(reconnected.wait(), 10)
        for channel, old, queue in zip(channels, previous, queues):
            assert await channel.get_underlay_channel() is not old
            await channel.default_exchange.publish(
                Message(b"restored"), queue.name
            )
            message = await queue.get(timeout=5)
            assert message is not None and message.body == b"restored"
            await message.ack()
        assert not any(
            "Set changed size during iteration" in reason
            for reason in close_reasons
        ), close_reasons
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
