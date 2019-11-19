import asyncio
from collections import Counter

from aio_pika.pool import Pool
from tests import AsyncTestCase


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
        await asyncio.sleep(0)
        self.counter += 1
        return self.counter


class TestCase(BaseTestCase):
    max_size = 10

    async def test_simple(self):
        async def getter():
            async with self.pool.acquire() as instance:
                assert instance > 0
                await asyncio.sleep(0.01)
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
                await asyncio.sleep(0.01)
                raise RuntimeError(instance)

        results = await asyncio.gather(
            *[getter() for _ in range(200)],
            loop=self.loop, return_exceptions=True
        )

        for result in results:
            self.assertIsInstance(result, RuntimeError)

        self.assertEqual(self.counter, self.max_size)


class TestCaseClose(BaseTestCase):
    max_size = 10

    class Instance:
        def __init__(self):
            self.closed = False

        async def close(self):
            if self.closed:
                raise RuntimeError

            self.closed = True

    def setUp(self):
        self.instances = set()
        super().setUp()

    async def create_instance(self):
        obj = TestCaseClose.Instanse()
        self.instances.add(obj)
        return obj

    async def test_close(self):
        async def getter():
            async with self.pool.acquire() as instance:
                assert instance > 0
                await asyncio.sleep(0.01)
                return self.counter

        await asyncio.gather(
            *[getter() for _ in range(200)],
            loop=self.loop, return_exceptions=True
        )

        for instance in self.instances:
            self.assertFalse(instance.closed)

        await self.pool.close()

        for instance in self.instances:
            self.assertTrue(instance.closed)

    async def test_close_context_manager(self):
        async def getter():
            async with self.pool.acquire() as instance:
                assert instance > 0
                await asyncio.sleep(0.01)
                return self.counter

        async with self.pool:
            await asyncio.gather(
                *[getter() for _ in range(200)],
                loop=self.loop, return_exceptions=True
            )

            for instance in self.instances:
                self.assertFalse(instance.closed)

        for instance in self.instances:
            self.assertTrue(instance.closed)

        self.assertTrue(self.pool.is_closed)


class TestCaseNoMaxSize(BaseTestCase):
    max_size = None

    async def test_simple(self):
        call_count = 200

        async def getter():
            async with self.pool.acquire() as instance:
                await asyncio.sleep(1)
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
                await asyncio.sleep(0.05)
                counter[instance] += 1

        await asyncio.gather(
            *[getter() for _ in range(self.call_count)],
            loop=self.loop, return_exceptions=True
        )

        self.assertEqual(sum(counter.values()), self.call_count)
        self.assertEqual(self.counter, set(counter))
        self.assertEqual(len(set(counter.values())), 1)
