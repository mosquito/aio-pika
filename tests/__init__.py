import asyncio
import unittest.mock

import asynctest
import logging
import os

from functools import wraps

import shortuuid
from yarl import URL

from aio_pika import Connection, connect, Channel, Queue, Exchange

log = logging.getLogger(__name__)


for logger_name in ('pika.channel', 'pika.callback', 'pika.connection'):
    logging.getLogger(logger_name).setLevel(logging.INFO)


logging.basicConfig(level=logging.DEBUG)


AMQP_URL = URL(os.getenv("AMQP_URL", "amqp://guest:guest@localhost"))

if not AMQP_URL.path:
    AMQP_URL.path = '/'


try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


class AsyncTestCase(asynctest.TestCase):
    use_default_loop = False
    forbid_get_event_loop = False

    TEST_TIMEOUT = int(os.getenv('ASYNCIO_TEST_TIMEOUT', '30'))

    def _run_test_method(self, method):
        result = method()
        if asyncio.iscoroutine(result):
            self.loop.run_until_complete(
                asyncio.wait_for(result, timeout=self.TEST_TIMEOUT)
            )

    @property
    def _all_tasks(self):
        return getattr(asyncio, 'all_tasks', asyncio.Task.all_tasks)

    async def doCleanups(self):
        outcome = self._outcome or unittest.mock._Outcome()

        while self._cleanups:
            function, args, kwargs = self._cleanups.pop()
            with outcome.testPartExecutor(self):
                if asyncio.iscoroutinefunction(function):
                    await self.loop.create_task(function(*args, **kwargs))
                elif asyncio.iscoroutine(function):
                    await function
                else:
                    function(*args, **kwargs)

        return outcome.success

    def get_random_name(self, *args):
        prefix = ['test']
        for item in args:
            prefix.append(item)
        prefix.append(shortuuid.uuid())

        return ".".join(prefix)


class BaseTestCase(AsyncTestCase):
    async def create_connection(self, cleanup=True) -> Connection:
        client = await connect(str(AMQP_URL), loop=self.loop)

        if cleanup:
            self.addCleanup(client.close)

        return client

    async def create_channel(self, connection=None,
                             cleanup=True, **kwargs) -> Channel:
        if connection is None:
            connection = await self.create_connection()

        channel = await connection.channel(**kwargs)

        if cleanup:
            self.addCleanup(channel.close)

        return channel

    async def declare_queue(self, *args, **kwargs) -> Queue:
        if 'channel' not in kwargs:
            channel = await self.create_channel()
        else:
            channel = kwargs.pop('channel')

        queue = await channel.declare_queue(*args, **kwargs)
        self.addCleanup(queue.delete)
        return queue

    async def declare_exchange(self, *args, **kwargs) -> Exchange:
        if 'channel' not in kwargs:
            channel = await self.create_channel()
        else:
            channel = kwargs.pop('channel')

        exchange = await channel.declare_exchange(*args, **kwargs)
        self.addCleanup(exchange.delete)
        return exchange


def timeout(timeout_sec=5):
    def decorator(func):
        @wraps(func)
        async def wrap(self, *args, **kwargs):
            loop = self.loop

            task = loop.create_task(func(self, *args, **kwargs))

            def on_timeout():
                if task.done():
                    return

                task.cancel()

            self.loop.call_later(timeout_sec, on_timeout)

            return await task

        return wrap
    return decorator
