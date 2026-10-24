import asyncio

import pytest

import aio_pika
from aio_pika.patterns.rpc import JsonRPC, JsonRPCError
from tests import get_random_name


async def rpc_func(*, foo, bar):
    assert not foo
    assert not bar

    return {"foo": "bar"}


class CustomException(Exception):
    pass


async def rpc_raise_exception(*, foo, bar):
    raise CustomException("foo bar")


class TestCase:
    async def test_jsonrpc_simple(self, channel: aio_pika.Channel):
        rpc = await JsonRPC.create(channel, auto_delete=True)

        await rpc.register("test.rpc", rpc_func, auto_delete=True)

        result = await rpc.proxy.test.rpc(foo=None, bar=None)
        assert result == {"foo": "bar"}

        await rpc.unregister(rpc_func)
        await rpc.close()

        # Close already closed
        await rpc.close()

    async def test_jsonrpc_assert(self, channel: aio_pika.Channel):
        rpc = await JsonRPC.create(channel, auto_delete=True)

        await rpc.register("test.rpc", rpc_func, auto_delete=True)

        with pytest.raises(JsonRPCError) as excinfo:
            await rpc.proxy.test.rpc(foo=True, bar=None)
        assert excinfo.value.args[1]["error"]["type"] == "AssertionError"

        await rpc.unregister(rpc_func)
        await rpc.close()

    async def test_jsonrpc_error(self, channel: aio_pika.Channel):
        rpc = await JsonRPC.create(channel, auto_delete=True)

        await rpc.register(
            "test.rpc_error", rpc_raise_exception, auto_delete=True
        )

        with pytest.raises(Exception) as excinfo:
            await rpc.proxy.test.rpc_error(foo=True, bar=None)
        assert excinfo.value.args[1]["error"]["type"] == "CustomException"

        with pytest.raises(JsonRPCError):
            await rpc.proxy.test.rpc_error(foo=True, bar=None)

        await rpc.unregister(rpc_raise_exception)
        await rpc.close()


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"text": "Привет, 世界 👋"}, id="unicode"),
        pytest.param(
            {
                "error": {
                    "type": "ValueError",
                    "message": "ordinary result",
                    "args": [],
                },
            },
            id="result-with-error-key",
        ),
    ],
)
async def test_jsonrpc_result(channel: aio_pika.Channel, payload):
    async def echo(*, value):
        return value

    rpc = await JsonRPC.create(channel, auto_delete=True)
    method = get_random_name("jsonrpc")
    await rpc.register(method, echo, auto_delete=True)
    try:
        result = await asyncio.wait_for(
            rpc.call(method, kwargs={"value": payload}), timeout=5
        )
        assert result == payload
    finally:
        await rpc.unregister(echo)
        await rpc.close()


@pytest.mark.parametrize("exception_type", [ValueError, CustomException])
async def test_jsonrpc_remote_error(channel: aio_pika.Channel, exception_type):
    original = exception_type("Ошибка 👋", 42)

    async def fail():
        raise original

    rpc = await JsonRPC.create(channel, auto_delete=True)
    method = get_random_name("jsonrpc")
    await rpc.register(method, fail, auto_delete=True)
    try:
        with pytest.raises(JsonRPCError) as error:
            await asyncio.wait_for(rpc.call(method), timeout=5)

        assert type(error.value) is JsonRPCError
        assert error.value.args == (
            "RPC exception",
            {
                "error": {
                    "type": exception_type.__name__,
                    "message": repr(original),
                    "args": list(original.args),
                },
            },
        )
    finally:
        await rpc.unregister(fail)
        await rpc.close()
