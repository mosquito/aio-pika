import asyncio
from collections import Counter

import pytest

from aio_pika.pool import Pool, PoolInstance, PoolInvalidStateError


@pytest.mark.parametrize("max_size", [50, 10, 5, 1])
async def test_simple(max_size, event_loop):
    counter = 0

    async def create_instance():
        nonlocal counter
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


async def test_close_while_acquisition_waits_for_released_item():
    creation_started = asyncio.Event()
    finish_creation = asyncio.Event()
    waiter_started = asyncio.Event()
    instance = TestInstanceBase.Instance()

    async def create_instance():
        creation_started.set()
        await finish_creation.wait()
        return instance

    pool: Pool[TestInstanceBase.Instance] = Pool(create_instance, max_size=1)
    first = pool.acquire()
    first_task = asyncio.create_task(first.__aenter__())
    await creation_started.wait()

    async def acquire_second():
        waiter_started.set()
        async with pool.acquire():
            pass

    waiter = asyncio.create_task(acquire_second())
    await waiter_started.wait()
    finish_creation.set()
    await first_task

    try:
        await asyncio.wait_for(pool.close(), timeout=1)
        assert pool.is_closed
        assert instance.closed
        with pytest.raises(PoolInvalidStateError):
            await asyncio.wait_for(waiter, timeout=1)
    finally:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        if not pool.is_closed:
            await pool.close()


@pytest.mark.parametrize("waiter_count", [1, 3])
@pytest.mark.parametrize("release_before_close", [False, True])
async def test_close_wakes_waiting_acquisitions(
    waiter_count,
    release_before_close,
):
    instance = TestInstanceBase.Instance()
    waiting: asyncio.Queue[None] = asyncio.Queue()

    async def create_instance():
        return instance

    pool: Pool[TestInstanceBase.Instance] = Pool(create_instance, max_size=1)
    assert await pool.acquire().__aenter__() is instance

    async def acquire():
        waiting.put_nowait(None)
        return await pool.acquire().__aenter__()

    waiters = [asyncio.create_task(acquire()) for _ in range(waiter_count + 1)]
    try:
        for _ in waiters:
            await waiting.get()
        cancelled = waiters.pop()
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        if release_before_close:
            pool.put(instance)
        await pool.close()
        done, pending = await asyncio.wait(waiters, timeout=1)
        assert not pending
        assert all(
            isinstance(task.exception(), PoolInvalidStateError) for task in done
        )
        assert instance.closed
    finally:
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        if not pool.is_closed:
            await pool.close()


async def test_acquisition_queued_after_close_does_not_create_item():
    creation_started = asyncio.Event()
    finish_creation = asyncio.Event()
    close_started = asyncio.Event()
    waiter_started = asyncio.Event()
    instances = []

    async def create_instance():
        creation_started.set()
        await finish_creation.wait()
        instance = TestInstanceBase.Instance()
        instances.append(instance)
        return instance

    pool: Pool[TestInstanceBase.Instance] = Pool(create_instance, max_size=2)
    first = pool.acquire()
    first_task = asyncio.create_task(first.__aenter__())
    await creation_started.wait()

    async def close_pool():
        close_started.set()
        await pool.close()

    close_task = asyncio.create_task(close_pool())
    await close_started.wait()

    async def acquire_second():
        waiter_started.set()
        return await pool.acquire().__aenter__()

    waiter = asyncio.create_task(acquire_second())
    await waiter_started.wait()
    finish_creation.set()
    await first_task
    await close_task

    with pytest.raises(PoolInvalidStateError):
        await waiter

    assert len(instances) == 1
    assert instances[0].closed


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
            nonlocal counter
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
