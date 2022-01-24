import asyncio
import time
from functools import partial

import pytest

import aio_pika
from aio_pika import RobustChannel
from tests.test_amqp import (
    TestCaseAmqp, TestCaseAmqpNoConfirms, TestCaseAmqpWithConfirms,
)


@pytest.fixture
def connection_fabric():
    return aio_pika.connect_robust


@pytest.fixture
def create_connection(connection_fabric, loop, amqp_url):
    return partial(connection_fabric, amqp_url, loop=loop)


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

        connection.add_reconnect_callback(cb)

        del cb
        assert len(connection.reconnect_callbacks) == 1

    async def test_channel_blocking_timeout_reopen(self, connection):
        channel: RobustChannel = await connection.channel()     # type: ignore
        close_reasons = []
        close_event = asyncio.Event()
        reopen_event = asyncio.Event()
        channel.reopen_callbacks.add(lambda _: reopen_event.set(), weak=False)

        def on_done(*args):
            close_reasons.append(args)
            close_event.set()
            return

        channel.add_close_callback(on_done)

        async def run(sleep_time=1):
            await channel.set_qos(1)
            if sleep_time:
                time.sleep(sleep_time)
            await channel.set_qos(0)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(run(), timeout=0.2)

        await close_event.wait()

        with pytest.raises(RuntimeError):
            await channel.channel.closing

        assert channel.is_closed

        # Ensure close callback has been called
        assert close_reasons

        await asyncio.wait_for(reopen_event.wait(), timeout=2)
        await asyncio.wait_for(run(sleep_time=0), timeout=2)


class TestCaseAmqpNoConfirmsRobust(TestCaseAmqpNoConfirms):
    pass


class TestCaseAmqpWithConfirmsRobust(TestCaseAmqpWithConfirms):
    pass
