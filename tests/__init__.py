import asyncio
import asynctest
import logging
import os

from functools import wraps

import shortuuid
from typing import Generator, Any
from yarl import URL

from aio_pika import Connection, connect, Channel, Queue, Exchange

log = logging.getLogger(__name__)


for logger_name in ('pika.channel', 'pika.callback', 'pika.connection'):
    logging.getLogger(logger_name).setLevel(logging.INFO)


logging.basicConfig(level=logging.DEBUG)


AMQP_URL = URL(os.getenv("AMQP_URL", "amqp://guest:guest@localhost"))

if not AMQP_URL.path:
    AMQP_URL.path = '/'


class AsyncTestCase(asynctest.TestCase):
    forbid_get_event_loop = True

    def get_random_name(self, *args):
        prefix = ['test']
        for item in args:
            prefix.append(item)
        prefix.append(shortuuid.uuid())

        return ".".join(prefix)


class BaseTestCase(AsyncTestCase):
    async def create_connection(self, cleanup=True) -> Generator[Any, None, Connection]:
        client = await connect(AMQP_URL, loop=self.loop)

        if cleanup:
            self.addCleanup(client.close)

        return client

    async def create_channel(self, connection=None, cleanup=True, **kwargs) -> Generator[Any, None, Channel]:
        if connection is None:
            connection = await self.create_connection()

        channel = await connection.channel(**kwargs)

        if cleanup:
            self.addCleanup(channel.close)

        return channel

    async def declare_queue(self, *args, **kwargs) -> Generator[Any, None, Queue]:
        if 'channel' not in kwargs:
            channel = await self.create_channel()
        else:
            channel = kwargs.pop('channel')

        queue = await channel.declare_queue(*args, **kwargs)
        self.addCleanup(queue.delete)
        return queue

    async def declare_exchange(self, *args, **kwargs) -> Generator[Any, None, Exchange]:
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
