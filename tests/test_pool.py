import asyncio
from collections import Counter

import pytest
from aio_pika.pool import Pool
from tests import AsyncTestCase


pytestmark = pytest.mark.asyncio


class BaseTestCase(AsyncTestCase):
    max_size = None

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


class TestCase(BaseTestCase):
    max_size = 10

    async def test_simple(self):
        async def getter():
            async with self.pool.acquire() as instance:
                assert instance > 0
                await asyncio.sleep(0.01, loop=self.loop)
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
                await asyncio.sleep(0.01, loop=self.loop)
                raise RuntimeError(instance)

        results = await asyncio.gather(
            *[getter() for _ in range(200)],
            loop=self.loop, return_exceptions=True
        )

        for result in results:
            self.assertIsInstance(result, RuntimeError)

        self.assertEqual(self.counter, self.max_size)


class TestCaseNoMaxSize(BaseTestCase):
    max_size = None

    async def test_simple(self):
        call_count = 200

        async def getter():
            async with self.pool.acquire() as instance:
                await asyncio.sleep(1, loop=self.loop)
                assert instance > 0
                return self.counter

        results = await asyncio.gather(
            *[getter() for _ in range(call_count)],
            loop=self.loop, return_exceptions=True
        )

        for result in results:
            self.assertGreater(result, -1)

        self.assertEqual(self.counter, call_count)


class TestCaseItemReuse(BaseTestCase):
    max_size = 10
    call_count = max_size * 5

    def setUp(self):
        super().setUp()
        self.counter = set()

        self.pool = Pool(
            self.create_instance,
            max_size=self.max_size,
            loop=self.loop
        )

    async def create_instance(self):
        obj = object()
        self.counter.add(obj)
        return obj

    async def test_simple(self):
        counter = Counter()

        async def getter():
            nonlocal counter

            async with self.pool.acquire() as instance:
                await asyncio.sleep(0.05, loop=self.loop)
                counter[instance] += 1

        await asyncio.gather(
            *[getter() for _ in range(self.call_count)],
            loop=self.loop, return_exceptions=True
        )

        self.assertEqual(sum(counter.values()), self.call_count)
        self.assertEqual(self.counter, set(counter))
        self.assertEqual(len(set(counter.values())), 1)
