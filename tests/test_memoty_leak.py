import gc
import weakref


class TestCase:
    async def test_on_successful_cleanup_message(self, create_connection):
        rabbitmq_connection = await create_connection()

        weakset = weakref.WeakSet()

        async def f(rabbitmq_connection, weakset):
            async with rabbitmq_connection.channel() as channel:
                weakset.add(channel)

        async with rabbitmq_connection:
            for i in range(5):
                await f(rabbitmq_connection, weakset)

        gc.collect()
        assert len(weakset) == 0
