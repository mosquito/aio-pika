import pytest

import aio_pika
from aio_pika.patterns.rpc import JsonRPC, JsonRPCError


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
