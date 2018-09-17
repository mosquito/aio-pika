import asyncio
import pytest

from aio_pika.patterns.master import Master
from tests.test_amqp import BaseTestCase


pytestmark = pytest.mark.asyncio


class TestCase(BaseTestCase):

    async def test_simple(self):
        channel = await self.create_channel()
        master = Master(channel)

        self.state = []

        def worker_func(*, foo, bar):
            self.state.append((foo, bar))

        worker = await master.create_worker(
            'worker.foo', worker_func, auto_delete=True
        )

        await master.proxy.worker.foo(foo=1, bar=2)

        await asyncio.sleep(0.5, loop=self.loop)

        self.assertSequenceEqual(self.state, [(1, 2)])

        await worker.close()

    async def test_simple_coro(self):
        channel = await self.create_channel()
        master = Master(channel)

        self.state = []

        async def worker_func(*, foo, bar):
            self.state.append((foo, bar))

        worker = await master.create_worker(
            'worker.foo', worker_func, auto_delete=True
        )

        await master.proxy.worker.foo(foo=1, bar=2)

        await asyncio.sleep(0.5, loop=self.loop)

        self.assertSequenceEqual(self.state, [(1, 2)])

        await worker.close()

    async def test_simple_many(self):
        channel = await self.create_channel()
        master = Master(channel)

        self.state = []

        def worker_func(*, foo):
            self.state.append(foo)

        worker = await master.create_worker(
            'worker.foo', worker_func, auto_delete=True
        )

        for item in range(1000):
            await master.proxy.worker.foo(foo=item)

        await asyncio.sleep(2, loop=self.loop)

        self.assertSequenceEqual(self.state, range(1000))

        await worker.close()
