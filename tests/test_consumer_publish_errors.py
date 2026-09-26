import asyncio

import pytest
from aiormq.exceptions import ChannelAccessRefused

import aio_pika
from tests import get_random_name


@pytest.mark.parametrize("separate_channel", [False, True])
async def test_consumer_process_preserves_publish_error(
    amqp_url,
    connection_fabric,
    separate_channel,
):
    async with await aio_pika.connect(amqp_url) as admin_connection:
        admin = await admin_connection.channel()
        source = await admin.declare_queue(get_random_name("source"))
        # Publishing to an internal exchange makes RabbitMQ send a real
        # Channel.Close(403), without modifying the broker's user permissions.
        forbidden = await admin.declare_exchange(
            get_random_name("internal"),
            internal=True,
        )
        try:
            async with await connection_fabric(amqp_url) as connection:
                channel = await connection.channel()
                publisher = (
                    await connection.channel() if separate_channel else channel
                )
                exchange = await publisher.get_exchange(
                    forbidden.name,
                    ensure=False,
                )
                queue = await channel.declare_queue(source.name)
                failed = asyncio.Event()
                recovered = asyncio.Event()
                errors = []
                deliveries = []
                original_errors = []

                async def consume(message):
                    deliveries.append(message)
                    if message.redelivered:
                        # Application policy: do not retry a permanent
                        # publishing failure on every redelivery.
                        await message.reject(requeue=False)
                        recovered.set()
                        return
                    try:
                        async with message.process():
                            try:
                                await exchange.publish(
                                    aio_pika.Message(b"reply"),
                                    "reply",
                                )
                            except Exception as exc:
                                original_errors.append(exc)
                                raise
                    except Exception as exc:
                        errors.append(exc)
                        failed.set()

                tag = await queue.consume(consume)
                await admin.default_exchange.publish(
                    aio_pika.Message(b"request"),
                    source.name,
                )
                await asyncio.wait_for(failed.wait(), timeout=10)
                assert len(errors) == 1
                assert isinstance(errors[0], ChannelAccessRefused)
                assert errors[0] is original_errors[0]
                assert not deliveries[0].redelivered

                if separate_channel:
                    # The source channel survives and process() can reject
                    # the request even though the publishing channel closed.
                    assert deliveries[0].processed
                    assert not channel.is_closed
                    await queue.cancel(tag)
                elif isinstance(connection, aio_pika.RobustConnection):
                    await asyncio.wait_for(recovered.wait(), timeout=10)
                    await queue.cancel(tag)
                    assert len(deliveries) == 2
                    assert deliveries[1].processed
                    assert not deliveries[0].processed
                else:
                    # With no automatic restoration the broker still
                    # requeues the delivery from the closed channel.
                    message = await source.get(timeout=10)
                    assert message is not None and message.redelivered
                    await message.reject(requeue=False)
                    assert not deliveries[0].processed

            # Closing the consumer connection flushes any pending disposition.
            assert await source.get(fail=False, timeout=10) is None
        finally:
            await source.delete(if_empty=False, if_unused=False)
            await forbidden.delete()
