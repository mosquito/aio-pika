import asyncio
import logging
import warnings
from functools import partial

import pytest

import aio_pika
from aio_pika import Message
from aio_pika.exceptions import MessageProcessError
from aio_pika.message import IncomingMessage
from aio_pika.patterns.rpc import RPC
from aio_pika.patterns.rpc import log as rpc_logger
from tests import get_random_name


async def rpc_func(*, foo, bar):
    assert not foo
    assert not bar

    return {"foo": "bar"}


async def rpc_func2(*, foo, bar):
    assert not foo
    assert not bar

    return {"foo": "bar2"}


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

        async def bypass(_: aio_pika.abc.AbstractIncomingMessage):
            return

        await rpc.register("test.sleeper", bypass, auto_delete=True)

        with pytest.raises(RuntimeError):
            await rpc.register(
                "test.sleeper", bypass, auto_delete=True,
            )

        await rpc.register("test.one", rpc_func, auto_delete=True)

        with pytest.raises(RuntimeError):
            await rpc.register("test.two", rpc_func, auto_delete=True)

        await rpc.unregister(rpc_func)
        await rpc.unregister(rpc_func)

        await rpc.close()

    async def test_register_non_coroutine(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        def bypass(_):
            return

        with pytest.deprecated_call():
            await rpc.register(
                "test.non-coroutine",
                bypass,         # type: ignore
                auto_delete=True,
            )

        async def coro(_):
            return

        with pytest.warns(UserWarning) as record:
            warnings.warn("Test", UserWarning)
            await rpc.register(
                "test.coroutine",
                coro,  # type: ignore
                auto_delete=True,
            )

        assert len(record) == 1

        with pytest.warns() as record:
            warnings.warn("Test", UserWarning)
            await rpc.register(
                "test.coroutine_partial",
                partial(partial(coro)),  # type: ignore
                auto_delete=True,
            )

        assert len(record) == 1

    async def test_non_serializable_result(self, channel: aio_pika.Channel):
        rpc = await RPC.create(channel, auto_delete=True)

        async def bad_func():
            async def inner():
                await asyncio.sleep(0)
            return inner()

        await rpc.register(
            "test.not-serializable",
            bad_func,
            auto_delete=True,
        )

        with pytest.raises(TypeError):
            await rpc.call("test.not-serializable")

    async def test_custom_exchange(self, channel: aio_pika.Channel):
        rpc_ex1 = await RPC.create(channel, auto_delete=True, exchange="ex1")
        rpc_ex2 = await RPC.create(channel, auto_delete=True, exchange="ex2")
        rpc_default = await RPC.create(channel, auto_delete=True)

        await rpc_ex1.register("test.rpc", rpc_func, auto_delete=True)
        result = await rpc_ex1.proxy.test.rpc(foo=None, bar=None)
        assert result == {"foo": "bar"}

        with pytest.raises(MessageProcessError):
            await rpc_ex2.proxy.test.rpc(foo=None, bar=None)

        await rpc_ex2.register("test.rpc", rpc_func2, auto_delete=True)
        result = await rpc_ex2.proxy.test.rpc(foo=None, bar=None)
        assert result == {"foo": "bar2"}

        with pytest.raises(MessageProcessError):
            await rpc_default.proxy.test.rpc(foo=None, bar=None)

        await rpc_default.register("test.rpc", rpc_func, auto_delete=True)
        result = await rpc_default.proxy.test.rpc(foo=None, bar=None)
        assert result == {"foo": "bar"}

        await rpc_ex1.unregister(rpc_func)
        await rpc_ex1.close()

        await rpc_ex2.unregister(rpc_func2)
        await rpc_ex2.close()

        await rpc_default.unregister(rpc_func)
        await rpc_default.close()
