import asyncio
from functools import partial

import pytest
from aiormq import ChannelNotFoundEntity
from aiormq.exceptions import ChannelPreconditionFailed

import aio_pika
from aio_pika import RobustChannel
from tests import get_random_name
from tests.test_amqp import (
    TestCaseAmqp,
    TestCaseAmqpNoConfirms,
    TestCaseAmqpWithConfirms,
)


@pytest.fixture
def connection_fabric():
    return aio_pika.connect_robust


@pytest.fixture
def create_connection(connection_fabric, event_loop, amqp_url):
    return partial(connection_fabric, amqp_url, loop=event_loop)


class TestCaseNoRobust(TestCaseAmqp):  # type: ignore
    PARAMS = [{"robust": True}, {"robust": False}]
    IDS = ["robust=1", "robust=0"]

    @staticmethod
    @pytest.fixture(name="declare_queue", params=PARAMS, ids=IDS)
    def declare_queue_(request, declare_queue):  # type: ignore
        async def fabric(*args, **kwargs) -> aio_pika.Queue:
            kwargs.update(request.param)
            return await declare_queue(*args, **kwargs)

        return fabric

    @staticmethod
    @pytest.fixture(name="declare_exchange", params=PARAMS, ids=IDS)
    def declare_exchange_(request, declare_exchange):  # type: ignore
        async def fabric(*args, **kwargs) -> aio_pika.Queue:
            kwargs.update(request.param)
            return await declare_exchange(*args, **kwargs)

        return fabric

    async def test_add_reconnect_callback(self, create_connection):
        connection = await create_connection()

        def cb(*a, **kw):
            pass

        connection.reconnect_callbacks.add(cb)

        del cb
        assert len(connection.reconnect_callbacks) == 1

    async def test_channel_blocking_timeout_reopen(self, connection):
        channel: RobustChannel = await connection.channel()  # type: ignore
        close_reasons = []
        close_event = asyncio.Event()
        reopen_event = asyncio.Event()
        channel.reopen_callbacks.add(lambda *_: reopen_event.set())

        queue_name = get_random_name("test_channel_blocking_timeout_reopen")

        def on_done(*args):
            close_reasons.append(args)
            close_event.set()
            return

        channel.close_callbacks.add(on_done)

        with pytest.raises(ChannelNotFoundEntity):
            await channel.declare_queue(queue_name, passive=True)

        await close_event.wait()
        assert channel.is_closed

        # Ensure close callback has been called
        assert close_reasons

        await asyncio.wait_for(reopen_event.wait(), timeout=60)
        await channel.declare_queue(queue_name, auto_delete=True)

    async def test_get_queue_fail(self, connection):
        channel: RobustChannel = await connection.channel()  # type: ignore
        close_event = asyncio.Event()
        reopen_event = asyncio.Event()
        channel.close_callbacks.add(lambda *_: close_event.set())
        channel.reopen_callbacks.add(lambda *_: reopen_event.set())

        name = get_random_name("passive", "queue")

        await channel.declare_queue(
            name,
            auto_delete=True,
            arguments={"x-max-length": 1},
        )
        with pytest.raises(ChannelPreconditionFailed):
            await channel.declare_queue(name, auto_delete=True)
        await asyncio.sleep(0)
        await close_event.wait()
        await reopen_event.wait()
        with pytest.raises(ChannelPreconditionFailed):
            await channel.declare_queue(name, auto_delete=True)

    async def test_channel_is_ready_after_close_and_reopen(self, connection):
        channel: RobustChannel = await connection.channel()  # type: ignore
        await channel.ready()
        await channel.close()
        assert channel.is_closed is True

        await channel.reopen()
        await asyncio.wait_for(channel.ready(), timeout=1)

        assert channel.is_closed is False

    async def test_channel_can_be_closed(self, connection):
        channel: RobustChannel = await connection.channel()  # type: ignore
        await channel.ready()
        await channel.close()

        assert channel.is_closed

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(channel.ready(), timeout=1)

        assert channel.is_closed

    async def test_get_exchange(self, connection, declare_exchange):
        channel = await self.create_channel(connection)
        name = get_random_name("passive", "exchange")

        with pytest.raises(aio_pika.exceptions.ChannelNotFoundEntity):
            await channel.get_exchange(name)

        channel = await self.create_channel(connection)
        exchange = await declare_exchange(
            name,
            auto_delete=True,
            channel=channel,
        )
        exchange_passive = await channel.get_exchange(name)

        assert exchange.name is exchange_passive.name

    async def test_get_exchange_returns_same_instance(self, connection):
        """Test that get_exchange returns the cached instance on repeated calls.

        This tests the fix for the memory leak where each call to get_exchange
        (which uses passive=True) was creating a new exchange instance instead
        of returning the cached one.

        Note: This only applies when robust=True (default), as non-robust
        exchanges are not cached.
        """
        channel = await self.create_channel(connection)
        name = get_random_name("passive", "exchange", "memory")

        # Declare exchange with robust=True (default) so it gets cached
        exchange = await channel.declare_exchange(
            name,
            auto_delete=True,
        )

        # Get the exchange multiple times - should return the same instance
        exchange_passive1 = await channel.get_exchange(name)
        exchange_passive2 = await channel.get_exchange(name)
        exchange_passive3 = await channel.get_exchange(name)

        # All should be the exact same object (same id)
        assert exchange is exchange_passive1
        assert exchange_passive1 is exchange_passive2
        assert exchange_passive2 is exchange_passive3

    async def test_get_queue_returns_same_instance(self, connection):
        """Test that get_queue returns the cached instance on repeated calls.

        This tests the fix for the memory leak where each call to get_queue
        (which uses passive=True) was creating a new queue instance instead
        of returning the cached one.

        Note: This only applies when robust=True (default), as non-robust
        queues are not cached.
        """
        channel = await self.create_channel(connection)
        name = get_random_name("passive", "queue", "memory")

        # Declare queue with robust=True (default) so it gets cached
        queue = await channel.declare_queue(
            name,
            auto_delete=True,
        )

        # Get the queue multiple times using passive=True - should return same
        queue_passive1 = await channel.get_queue(name)
        queue_passive2 = await channel.get_queue(name)
        queue_passive3 = await channel.get_queue(name)

        # All should be the exact same object (same id)
        assert queue is queue_passive1
        assert queue_passive1 is queue_passive2
        assert queue_passive2 is queue_passive3


class TestCaseAmqpNoConfirmsRobust(TestCaseAmqpNoConfirms):
    pass


class TestCaseAmqpWithConfirmsRobust(TestCaseAmqpWithConfirms):
    pass
