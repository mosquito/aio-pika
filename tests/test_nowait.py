from typing import assert_type

import aiormq
import pytest

from aio_pika import Channel, Queue, RobustChannel, RobustQueue
from aio_pika.abc import AbstractChannel, AbstractQueue
from tests import get_random_name


async def check_literal_types(
    channel: Channel | RobustChannel,
    queue: Queue | RobustQueue,
) -> None:
    assert_type(await queue.cancel("tag"), aiormq.spec.Basic.CancelOk)
    assert_type(await queue.cancel("tag", nowait=True), None)
    assert_type(await queue.purge(), aiormq.spec.Queue.PurgeOk)
    assert_type(await queue.purge(no_wait=True), None)
    assert_type(await channel.queue_delete("queue"), aiormq.spec.Queue.DeleteOk)
    assert_type(await channel.queue_delete("queue", nowait=True), None)
    assert_type(
        await channel.exchange_delete("exchange"), aiormq.spec.Exchange.DeleteOk
    )
    assert_type(await channel.exchange_delete("exchange", nowait=True), None)


@pytest.mark.parametrize("nowait", [False, True])
async def test_nowait_results(
    amqp_url, connection_fabric, nowait: bool
) -> None:
    async def consume(message):
        pass

    async with await connection_fabric(amqp_url) as connection:
        channel: AbstractChannel = await connection.channel()
        queue: AbstractQueue = await channel.declare_queue(
            get_random_name("nowait"),
        )
        tag = await queue.consume(consume, no_ack=True)
        cancelled = assert_type(
            await queue.cancel(tag, nowait=nowait),
            aiormq.spec.Basic.CancelOk | None,
        )
        purged = assert_type(
            await queue.purge(no_wait=nowait), aiormq.spec.Queue.PurgeOk | None
        )
        deleted = assert_type(
            await channel.queue_delete(queue.name, nowait=nowait),
            aiormq.spec.Queue.DeleteOk | None,
        )
        exchange = await channel.declare_exchange(get_random_name("nowait"))
        exchange_deleted = assert_type(
            await channel.exchange_delete(exchange.name, nowait=nowait),
            aiormq.spec.Exchange.DeleteOk | None,
        )
        for result, frame in (
            (cancelled, aiormq.spec.Basic.CancelOk),
            (purged, aiormq.spec.Queue.PurgeOk),
            (deleted, aiormq.spec.Queue.DeleteOk),
            (exchange_deleted, aiormq.spec.Exchange.DeleteOk),
        ):
            if nowait:
                assert result is None
            else:
                assert isinstance(result, frame)
        # An RPC after the no-wait calls must still receive its own reply.
        await channel.set_qos(prefetch_count=1, timeout=5)
