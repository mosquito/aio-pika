import asyncio

import pytest
from aio_pika.pool import Pool
from tests import AsyncTestCase


pytestmark = pytest.mark.asyncio


class TestCase(AsyncTestCase):
    max_size = 10

    def setUp(self):
        super().setUp()
        self.counter = 0
        self.pool = Pool(
            self.create_instance,
            max_size=self.max_size,
            loop=self.loop
        )

    async def create_instance(self):
        await asyncio.sleep(0, loop=self.loop)
        self.counter += 1
        return self.counter

    async def test_simple(self):
        async def getter():
            async with self.pool.acquire() as instance:
                assert instance > 0
                return self.counter

        results = await asyncio.gather(
            *[getter() for _ in range(200)],
            loop=self.loop, return_exceptions=True
        )

        for result in results:
            self.assertGreater(result, -1)

        self.assertEqual(self.counter, self.max_size)

    async def test_errored(self):
        async def getter():
            async with self.pool.acquire() as instance:
                raise RuntimeError(instance)

        results = await asyncio.gather(
            *[getter() for _ in range(200)],
            loop=self.loop, return_exceptions=True
        )

        for result in results:
            self.assertIsInstance(result, RuntimeError)

        self.assertEqual(self.counter, self.max_size)
