import asyncio
from typing import Any, List

import aio_pika
from aio_pika.patterns.master import (
    CompressedJsonMaster,
    JsonMaster,
    Master,
    NackMessage,
    RejectMessage,
)


class TestMaster:
    MASTER_CLASS = Master

    async def test_simple(self, channel: aio_pika.Channel):
        master = self.MASTER_CLASS(channel)
        event = asyncio.Event()

        self.state: List[Any] = []

        def worker_func(*, foo, bar):
            self.state.append((foo, bar))
            event.set()

        worker = await master.create_worker(
            "worker.foo",
            worker_func,
            auto_delete=True,
        )

        await master.proxy.worker.foo(foo=1, bar=2)

        await event.wait()

        assert self.state == [(1, 2)]

        await worker.close()

    async def test_simple_coro(self, channel: aio_pika.Channel):
        master = self.MASTER_CLASS(channel)
        event = asyncio.Event()

        self.state = []

        async def worker_func(*, foo, bar):
            self.state.append((foo, bar))
            event.set()

        worker = await master.create_worker(
            "worker.foo",
            worker_func,
            auto_delete=True,
        )

        await master.proxy.worker.foo(foo=1, bar=2)

        await event.wait()

        assert self.state == [(1, 2)]

        await worker.close()

    async def test_simple_many(self, channel: aio_pika.Channel):
        master = self.MASTER_CLASS(channel)
        tasks = 100

        state = []

        def worker_func(*, foo):
            nonlocal tasks
            state.append(foo)
            tasks -= 1

        worker = await master.create_worker(
            "worker.foo",
            worker_func,
            auto_delete=True,
        )

        for item in range(100):
            await master.proxy.worker.foo(foo=item)

        while tasks > 0:
            await asyncio.sleep(0)

        assert state == list(range(100))

        await worker.close()

    async def test_exception_classes(self, channel: aio_pika.Channel):
        master = self.MASTER_CLASS(channel)
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
            "worker.foo",
            worker_func,
            auto_delete=True,
        )

        for item in range(200):
            await master.proxy.worker.foo(foo=item)

        while counter > 0:
            await asyncio.sleep(0)

        assert self.state == list(range(50, 101))

        await worker.close()


class TestJsonMaster(TestMaster):
    MASTER_CLASS = JsonMaster


class TestCompressedJsonMaster(TestMaster):
    MASTER_CLASS = CompressedJsonMaster
