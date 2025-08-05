import asyncio
from collections import Counter

import pytest

from aio_pika.pool import Pool, PoolInstance


@pytest.mark.parametrize("max_size", [50, 10, 5, 1])
async def test_simple(max_size, event_loop):
    counter = 0

    async def create_instance():
        await asyncio.sleep(0)
        counter += 1
        return counter

    pool: Pool = Pool(create_instance, max_size=max_size, loop=event_loop)

    async def getter():
        async with pool.acquire() as instance:
            assert instance > 0
            await asyncio.sleep(1 if counter < max_size else 0)
            return instance, counter

    results = await asyncio.gather(*[getter() for _ in range(200)])

    for instance, total in results:
        assert instance > -1
        assert total > -1

    assert counter == max_size


class TestInstanceBase:
    class Instance(PoolInstance):
        def __init__(self):
            self.closed = False

        async def close(self):
            if self.closed:
                raise RuntimeError

            self.closed = True

    @pytest.fixture
    def instances(self):
        return set()

    @pytest.fixture(params=[50, 40, 30, 20, 10])
    def max_size(self, request):
        return request.param

    @pytest.fixture
    def pool(self, max_size, instances, event_loop):
        async def create_instance():
            obj = TestInstanceBase.Instance()
            instances.add(obj)
            return obj

        return Pool(create_instance, max_size=max_size, loop=event_loop)


class TestInstance(TestInstanceBase):
    async def test_close(self, pool, instances, event_loop, max_size):
        async def getter():
            async with pool.acquire():
                await asyncio.sleep(0.05)

        assert not pool.is_closed
        assert len(instances) == 0

        await asyncio.gather(*[getter() for _ in range(200)])

        assert len(instances) == max_size

        for instance in instances:
            assert not instance.closed

        await pool.close()

        for instance in instances:
            assert instance.closed

        assert pool.is_closed

    async def test_close_context_manager(self, pool, instances):
        async def getter():
            async with pool.acquire():
                await asyncio.sleep(0.05)

        async with pool:
            assert not pool.is_closed

            assert len(instances) == 0

            await asyncio.gather(*[getter() for _ in range(200)])

            assert len(instances) > 1

            for instance in instances:
                assert not instance.closed

            assert not pool.is_closed

        assert pool.is_closed

        for instance in instances:
            assert instance.closed


class TestCaseNoMaxSize(TestInstance):
    async def test_simple(self, pool, event_loop):
        call_count = 200
        counter = 0

        async def getter():
            async with pool.acquire() as instance:
                await asyncio.sleep(1)
                assert isinstance(instance, TestInstanceBase.Instance)
                counter += 1
                return counter

        results = await asyncio.gather(*[getter() for _ in range(call_count)])

        for result in results:
            assert result > -1

        assert counter == call_count


class TestCaseItemReuse(TestInstanceBase):
    @pytest.fixture
    def call_count(self, max_size):
        return max_size * 5

    async def test_simple(self, pool, call_count, instances):
        counter: Counter = Counter()

        async def getter():
            async with pool.acquire() as instance:
                await asyncio.sleep(0.05)
                counter[instance] += 1

        await asyncio.gather(*[getter() for _ in range(call_count)])

        assert sum(counter.values()) == call_count
        assert set(counter) == set(instances)
        assert len(set(counter.values())) == 1
