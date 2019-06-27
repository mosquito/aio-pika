import asyncio

from aio_pika.patterns.master import Master, RejectMessage, NackMessage
from tests.test_amqp import BaseTestCase


class TestCase(BaseTestCase):

    async def test_simple(self):
        channel = await self.create_channel()
        master = Master(channel)
        event = asyncio.Event()

        self.state = []

        def worker_func(*, foo, bar):
            nonlocal event
            self.state.append((foo, bar))
            event.set()

        worker = await master.create_worker(
            'worker.foo', worker_func, auto_delete=True
        )

        await master.proxy.worker.foo(foo=1, bar=2)

        await event.wait()

        self.assertSequenceEqual(self.state, [(1, 2)])

        await worker.close()

    async def test_simple_coro(self):
        channel = await self.create_channel()
        master = Master(channel)
        event = asyncio.Event()

        self.state = []

        async def worker_func(*, foo, bar):
            nonlocal event
            self.state.append((foo, bar))
            event.set()

        worker = await master.create_worker(
            'worker.foo', worker_func, auto_delete=True
        )

        await master.proxy.worker.foo(foo=1, bar=2)

        await event.wait()

        self.assertSequenceEqual(self.state, [(1, 2)])

        await worker.close()

    async def test_simple_many(self):
        channel = await self.create_channel()
        master = Master(channel)
        tasks = 100

        self.state = []

        def worker_func(*, foo):
            nonlocal tasks

            self.state.append(foo)
            tasks -= 1

        worker = await master.create_worker(
            'worker.foo', worker_func, auto_delete=True
        )

        for item in range(100):
            await master.proxy.worker.foo(foo=item)

        while tasks > 0:
            await asyncio.sleep(0)

        self.assertSequenceEqual(self.state, range(100))

        await worker.close()

    async def test_exception_classes(self):
        channel = await self.create_channel()
        master = Master(channel)
        counter = 200

        self.state = []

        def worker_func(*, foo):
            nonlocal counter
            counter -= 1

            if foo < 50:
                raise RejectMessage(requeue=False)
            if foo > 100:
                raise NackMessage(requeue=False)

            self.state.append(foo)

        worker = await master.create_worker(
            'worker.foo', worker_func, auto_delete=True
        )

        for item in range(200):
            await master.proxy.worker.foo(foo=item)

        while counter > 0:
            await asyncio.sleep(0)

        self.assertSequenceEqual(self.state, range(50, 101))

        await worker.close()
