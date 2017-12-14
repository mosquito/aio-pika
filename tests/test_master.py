import asyncio
import pytest

from aio_pika.patterns.master import Master
from tests.test_amqp import BaseTestCase


class TestCase(BaseTestCase):

    @pytest.mark.asyncio
    def test_simple(self):
        channel = yield from self.create_channel()
        master = Master(channel)

        self.state = []

        def worker_func(*, foo, bar):
            self.state.append((foo, bar))

        worker = yield from master.create_worker(
            'worker.foo', worker_func, auto_delete=True
        )

        yield from master.worker.foo(foo=1, bar=2)

        yield from asyncio.sleep(0.5, loop=self.loop)

        self.assertSequenceEqual(self.state, [(1, 2)])

        yield from worker.close()

    @pytest.mark.asyncio
    def test_simple_many(self):
        channel = yield from self.create_channel()
        master = Master(channel)

        self.state = []

        def worker_func(*, foo):
            self.state.append(foo)

        worker = yield from master.create_worker(
            'worker.foo', worker_func, auto_delete=True
        )

        for item in range(1000):
            yield from master.worker.foo(foo=item)

        yield from asyncio.sleep(2, loop=self.loop)

        self.assertSequenceEqual(self.state, range(1000))

        yield from worker.close()
