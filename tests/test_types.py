import aio_pika
import aio_pika.abc


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
