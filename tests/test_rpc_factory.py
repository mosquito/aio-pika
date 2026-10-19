from unittest.mock import MagicMock, create_autospec

import pytest

from aio_pika.abc import AbstractChannel, AbstractQueue
from aio_pika.patterns import RPC, JsonRPC


class DefaultLoggingRPC(RPC):
    def __init__(self, channel):
        super().__init__(channel, host_exceptions=True)


@pytest.mark.parametrize(
    ("rpc_class", "options", "host_exceptions"),
    [
        (RPC, {}, False),
        (RPC, {"host_exceptions": False}, False),
        (RPC, {"host_exceptions": True}, True),
        (JsonRPC, {}, False),
        (JsonRPC, {"host_exceptions": False}, False),
        (JsonRPC, {"host_exceptions": True}, True),
        (DefaultLoggingRPC, {}, True),
        (DefaultLoggingRPC, {"host_exceptions": False}, False),
        (DefaultLoggingRPC, {"host_exceptions": True}, True),
    ],
)
async def test_create_handles_host_exceptions(
    rpc_class,
    options,
    host_exceptions,
):
    channel = create_autospec(AbstractChannel, instance=True)
    channel.close_callbacks = MagicMock()
    channel.return_callbacks = MagicMock()
    result_queue = create_autospec(AbstractQueue, instance=True)
    result_queue.name = "rpc-results"
    result_queue.consume.return_value = "result-consumer"
    channel.declare_queue.return_value = result_queue
    arguments = {"x-expires": 1000}

    rpc = await rpc_class.create(
        channel,
        auto_delete=False,
        durable=True,
        arguments=arguments,
        **options,
    )
    try:
        assert isinstance(rpc, rpc_class)
        assert rpc.host_exceptions is host_exceptions
        channel.declare_queue.assert_awaited_once_with(
            None,
            auto_delete=False,
            durable=True,
            arguments=arguments,
        )
    finally:
        await rpc.close()
