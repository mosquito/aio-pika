import asyncio
from functools import partial

import pytest
from aiormq import ChannelNotFoundEntity
from aiormq.exceptions import ChannelPreconditionFailed

import aio_pika
from aio_pika import RobustChannel
from tests import get_random_name
from tests.test_amqp import (
    TestCaseAmqp, TestCaseAmqpNoConfirms, TestCaseAmqpWithConfirms,
)


@pytest.fixture
def connection_fabric():
    return aio_pika.connect_robust


@pytest.fixture
def create_connection(connection_fabric, event_loop, amqp_url):
    return partial(connection_fabric, amqp_url, loop=event_loop)


class TestCaseNoRobust(TestCaseAmqp):
    PARAMS = [{"robust": True}, {"robust": False}]
    IDS = ["robust=1", "robust=0"]

    @staticmethod
    @pytest.fixture(name="declare_queue", params=PARAMS, ids=IDS)
    def declare_queue_(request, declare_queue):
        async def fabric(*args, **kwargs) -> aio_pika.Queue:
            kwargs.update(request.param)
            return await declare_queue(*args, **kwargs)

        return fabric

    @staticmethod
    @pytest.fixture(name="declare_exchange", params=PARAMS, ids=IDS)
    def declare_exchange_(request, declare_exchange):
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
        channel: RobustChannel = await connection.channel()     # type: ignore
        close_reasons = []
        close_event = asyncio.Event()
        reopen_event = asyncio.Event()
        channel.reopen_callbacks.add(lambda _: reopen_event.set())

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
        channel: RobustChannel = await connection.channel()     # type: ignore
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


class TestCaseAmqpNoConfirmsRobust(TestCaseAmqpNoConfirms):
    pass


class TestCaseAmqpWithConfirmsRobust(TestCaseAmqpWithConfirms):
    pass
