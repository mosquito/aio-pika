import asyncio
import logging

import pytest

import aio_pika
from aio_pika import Message
from aio_pika.exceptions import MessageProcessError
from aio_pika.message import IncomingMessage
from aio_pika.patterns.rpc import RPC
from aio_pika.patterns.rpc import log as rpc_logger
from tests import get_random_name


def rpc_func(*, foo, bar):
    assert not foo
    assert not bar

    return {"foo": "bar"}


class TestCase:
    async def test_simple(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register("test.rpc", rpc_func, auto_delete=True)

        result = await rpc.proxy.test.rpc(foo=None, bar=None)
        assert result == {"foo": "bar"}

        await rpc.unregister(rpc_func)
        await rpc.close()

        # Close already closed
        await rpc.close()

    async def test_error(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register("test.rpc", rpc_func, auto_delete=True)

        with pytest.raises(AssertionError):
            await rpc.proxy.test.rpc(foo=True, bar=None)

        await rpc.unregister(rpc_func)
        await rpc.close()

    async def test_unroutable(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register("test.rpc", rpc_func, auto_delete=True)

        with pytest.raises(MessageProcessError):
            await rpc.proxy.unroutable()

        await rpc.unregister(rpc_func)
        await rpc.close()

    async def test_timed_out(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register("test.rpc", rpc_func, auto_delete=True)

        await channel.declare_queue(
            "test.timed_out",
            auto_delete=True,
            arguments={"x-dead-letter-exchange": RPC.DLX_NAME},
        )

        with pytest.raises(asyncio.TimeoutError):
            await rpc.call("test.timed_out", expiration=1)

        await rpc.unregister(rpc_func)
        await rpc.close()

    async def test_close_twice(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.close()
        await rpc.close()

    async def test_init_twice(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.initialize()

        await rpc.close()

    async def test_send_unknown_message(
        self, channel: aio_pika.Channel, caplog,
    ):
        rpc = await RPC.create(channel, auto_delete=True)

        body = b"test body"
        with caplog.at_level(logging.WARNING, logger=rpc_logger.name):
            await channel.default_exchange.publish(
                Message(body), routing_key=rpc.result_queue.name,
            )

            await asyncio.sleep(0.5)

            for log_record in caplog.records:
                if log_record.levelno == logging.WARNING:
                    break
            else:
                raise pytest.fail("Expected log message")

            incoming = log_record.args[0]

            assert isinstance(incoming, IncomingMessage)
            assert incoming.body == body
            assert (
                "Message without correlation_id was received:"
                in log_record.message
            )

        with caplog.at_level(logging.WARNING, logger=rpc_logger.name):
            await channel.default_exchange.publish(
                Message(body), routing_key="should-returned",
            )

            await asyncio.sleep(0.5)

            for log_record in caplog.records:
                if log_record.levelno == logging.WARNING:
                    break
            else:
                raise pytest.fail("Expected log message")

            incoming = log_record.args[0]
            assert isinstance(incoming, IncomingMessage)
            assert incoming.body == body
            assert (
                "Message without correlation_id was received:"
                in log_record.message
            )

        await rpc.close()

    async def test_close_cancelling(
        self, channel: aio_pika.Channel, event_loop,
    ):
        rpc = await RPC.create(channel, auto_delete=True)

        async def sleeper():
            await asyncio.sleep(60)

        method_name = get_random_name("test", "sleeper")

        await rpc.register(method_name, sleeper, auto_delete=True)

        tasks = set()

        for _ in range(10):
            tasks.add(event_loop.create_task(rpc.call(method_name)))

        await rpc.close()

        logging.info("Waiting for results")
        for task in tasks:
            with pytest.raises(asyncio.CancelledError):
                await task

    async def test_register_twice(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register("test.sleeper", lambda x: None, auto_delete=True)

        with pytest.raises(RuntimeError):
            await rpc.register(
                "test.sleeper", lambda x: None, auto_delete=True,
            )

        await rpc.register("test.one", rpc_func, auto_delete=True)

        with pytest.raises(RuntimeError):
            await rpc.register("test.two", rpc_func, auto_delete=True)

        await rpc.unregister(rpc_func)
        await rpc.unregister(rpc_func)

        await rpc.close()
