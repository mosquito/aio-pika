from contextlib import AbstractAsyncContextManager

import aio_pika
import aio_pika.abc
import aio_pika.exceptions


async def test_connect_robust(amqp_url) -> None:
    async with await aio_pika.connect_robust(amqp_url) as connection:
        assert isinstance(connection, aio_pika.abc.AbstractRobustConnection)
        assert isinstance(connection, aio_pika.abc.AbstractConnection)

        channel = await connection.channel()
        assert isinstance(channel, aio_pika.abc.AbstractRobustChannel)
        assert isinstance(channel, aio_pika.abc.AbstractChannel)


async def test_connect(amqp_url) -> None:
    async with await aio_pika.connect(amqp_url) as connection:
        assert isinstance(connection, aio_pika.abc.AbstractConnection)
        assert not isinstance(
            connection,
            aio_pika.abc.AbstractRobustConnection,
        )

        channel = await connection.channel()
        assert isinstance(channel, aio_pika.abc.AbstractChannel)
        assert not isinstance(
            channel,
            aio_pika.abc.AbstractRobustChannel,
        )


async def test_robust_channel_ready(amqp_url) -> None:
    async with await aio_pika.connect_robust(amqp_url) as connection:
        # The annotation makes mypy check ready() on the abstract class.
        channel: aio_pika.abc.AbstractRobustChannel = await connection.channel()
        await channel.ready()
        assert not channel.is_closed


async def test_callback_signatures(amqp_url) -> None:
    calls: list[tuple[str, object, object]] = []

    def on_close(
        sender: aio_pika.abc.AbstractConnection | None,
        exc: BaseException | None,
    ) -> None:
        calls.append(("close", sender, exc))

    async def on_channel_close(
        sender: aio_pika.abc.AbstractChannel | None,
        exc: BaseException | None,
    ) -> None:
        calls.append(("channel", sender, exc))

    async def on_reconnect(
        sender: aio_pika.abc.AbstractRobustConnection | None,
    ) -> None:
        calls.append(("reconnect", sender, None))

    class Service:
        def on_return(
            self,
            sender: aio_pika.abc.AbstractChannel | None,
            message: aio_pika.abc.AbstractIncomingMessage,
        ) -> None:
            calls.append(("return", sender, message))

    connection = await aio_pika.connect_robust(amqp_url)
    # mypy checks these signatures against the typed collections.
    connection.close_callbacks.add(on_close)
    connection.reconnect_callbacks.add(on_reconnect)

    channel = await connection.channel()
    channel.close_callbacks.add(on_channel_close)
    channel.return_callbacks.add(Service().on_return)

    await channel.default_exchange.publish(
        aio_pika.Message(b"returned"),
        routing_key="no-such-queue",
    )
    await channel.close()
    await connection.close()

    kinds = [kind for kind, _, _ in calls]
    assert kinds == ["return", "channel", "close"]
    assert calls[0][1] is channel
    assert isinstance(calls[0][2], aio_pika.abc.AbstractIncomingMessage)
    assert calls[1][1] is channel
    assert isinstance(calls[1][2], aio_pika.exceptions.ChannelClosed)
    assert calls[2][1] is connection
    assert calls[2][2] is None or isinstance(calls[2][2], BaseException)


async def test_queue_iterator_is_async_context_manager(amqp_url) -> None:
    async with await aio_pika.connect(amqp_url) as connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(auto_delete=True)

        # The annotation makes mypy check the abstract signatures
        # against contextlib.AbstractAsyncContextManager.
        iterator: AbstractAsyncContextManager[
            aio_pika.abc.AbstractQueueIterator
        ] = queue.iterator()

        async with iterator as queue_iterator:
            assert isinstance(
                queue_iterator,
                aio_pika.abc.AbstractQueueIterator,
            )
