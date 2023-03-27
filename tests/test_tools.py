import asyncio
import logging
from copy import copy
from typing import Any, List
from unittest import mock

import pytest

from aio_pika.tools import CallbackCollection


log = logging.getLogger(__name__)


# noinspection PyTypeChecker
class TestCase:
    @pytest.fixture
    def instance(self) -> mock.MagicMock:
        return mock.MagicMock()

    @pytest.fixture
    def collection(self, instance):
        return CallbackCollection(instance)

    def test_basic(self, collection):
        def func(sender, *args, **kwargs):
            pass

        collection.add(func)

        assert func in collection

        with pytest.raises(ValueError):
            collection.add(None)

        collection.remove(func)

        with pytest.raises(LookupError):
            collection.remove(func)

        for _ in range(10):
            collection.add(func)

        assert len(collection) == 1

        collection.freeze()

        with pytest.raises(RuntimeError):
            collection.freeze()

        assert len(collection) == 1

        with pytest.raises(RuntimeError):
            collection.add(func)

        with pytest.raises(RuntimeError):
            collection.remove(func)

        with pytest.raises(RuntimeError):
            collection.clear()

        collection2 = copy(collection)
        collection.unfreeze()

        assert not copy(collection).is_frozen

        assert collection.is_frozen != collection2.is_frozen

        with pytest.raises(RuntimeError):
            collection.unfreeze()

        collection.clear()
        assert collection2
        assert not collection

    def test_callback_call(self, collection):
        l1: List[Any] = list()
        l2: List[Any] = list()

        assert l1 == l2

        collection.add(lambda sender, x: l1.append(x))
        collection.add(lambda sender, x: l2.append(x))

        collection(1)
        collection(2)

        assert l1 == l2
        assert l1 == [1, 2]

    async def test_blank_awaitable_callback(self, collection):
        await collection()

    async def test_awaitable_callback(
        self, event_loop, collection, instance,
    ):
        future = event_loop.create_future()

        shared = []

        async def coro(arg):
            nonlocal shared
            shared.append(arg)

        def task_maker(arg):
            return event_loop.create_task(coro(arg))

        collection.add(future.set_result)
        collection.add(coro)
        collection.add(task_maker)

        await collection()

        assert shared == [instance, instance]
        assert await future == instance

    async def test_collection_create_tasks(self, event_loop, collection, instance):
        future = event_loop.create_future()

        async def coro(arg):
            await asyncio.sleep(0.5)
            future.set_result(arg)

        collection.add(coro)

        # noinspection PyAsyncCall
        collection()

        assert await future == instance

    async def test_collection_run_tasks_parallel(self, collection):
        class Callable:
            def __init__(self):
                self.counter = 0

            async def __call__(self, *args, **kwargs):
                await asyncio.sleep(1)
                self.counter += 1

        callables = [Callable() for _ in range(100)]

        for callable in callables:
            collection.add(callable)

        await asyncio.wait_for(collection(), timeout=2)

        assert [c.counter for c in callables] == [1] * 100
