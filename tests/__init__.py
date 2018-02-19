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
    @asyncio.coroutine
    def create_connection(self, cleanup=True) -> Generator[Any, None, Connection]:
        client = yield from connect(AMQP_URL, loop=self.loop)

        if cleanup:
            self.addCleanup(client.close)

        return client

    @asyncio.coroutine
    def create_channel(self, connection=None, cleanup=True, **kwargs) -> Generator[Any, None, Channel]:
        if connection is None:
            connection = yield from self.create_connection()

        channel = yield from connection.channel(**kwargs)

        if cleanup:
            self.addCleanup(channel.close)

        return channel

    @asyncio.coroutine
    def declare_queue(self, *args, **kwargs) -> Generator[Any, None, Queue]:
        if 'channel' not in kwargs:
            channel = yield from self.create_channel()
        else:
            channel = kwargs.pop('channel')

        queue = yield from channel.declare_queue(*args, **kwargs)
        self.addCleanup(queue.delete)
        return queue

    @asyncio.coroutine
    def declare_exchange(self, *args, **kwargs) -> Generator[Any, None, Exchange]:
        if 'channel' not in kwargs:
            channel = yield from self.create_channel()
        else:
            channel = kwargs.pop('channel')

        exchange = yield from channel.declare_exchange(*args, **kwargs)
        self.addCleanup(exchange.delete)
        return exchange


def timeout(timeout_sec=5):
    def decorator(func):
        @asyncio.coroutine
        @wraps(func)
        def wrap(self, *args, **kwargs):
            future = asyncio.Future(loop=self.loop)

            def on_timeout(future: asyncio.Future):
                if future.done():
                    return

                future.set_exception(asyncio.TimeoutError)

            self.loop.call_later(timeout_sec, on_timeout, future)

            result = yield from asyncio.coroutine(func)(self, *args, **kwargs)

            if not future.done():
                future.set_result(result)

            return (yield from future)

        return wrap
    return decorator
