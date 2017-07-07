import asyncio
import asynctest
import logging
import os

from functools import wraps

import shortuuid
from yarl import URL

from aio_pika import connect

log = logging.getLogger(__name__)


for logger_name in ('pika.channel', 'pika.callback', 'pika.connection'):
    logging.getLogger(logger_name).setLevel(logging.INFO)


logging.basicConfig(level=logging.DEBUG)

AMQP_URL = URL(os.getenv("AMQP_URL", "amqp://guest:guest@localhost"))

if not AMQP_URL.path:
    AMQP_URL = AMQP_URL.with_path('/')


class AsyncTestCase(asynctest.TestCase):
    forbid_get_event_loop = True

    @staticmethod
    def get_random_name(*args):
        prefix = ['test']
        for item in args:
            prefix.append(item)
        prefix.append(shortuuid.uuid())

        return ".".join(prefix)

    @asyncio.coroutine
    def create_client(self):
        client = yield from connect(AMQP_URL, loop=self.loop)

        @asyncio.coroutine
        def cleanup():
            nonlocal client
            yield from client.close()
            yield from client.closing

        self.addCleanup(cleanup)
        return client


def timeout(timeout_sec=5):
    def decorator(func):
        @asyncio.coroutine
        @wraps(func)
        def wrap(self, *args, **kwargs):
            future = asyncio.Future(loop=self.loop)

            def on_timeout(future: asyncio.Future):
                if future.done():
                    return

                future.set_exception(TimeoutError)

            self.loop.call_later(timeout_sec, on_timeout, future)

            result = yield from asyncio.coroutine(func)(self, *args, **kwargs)

            if not future.done():
                future.set_result(result)

            return (yield from future)

        return wrap
    return decorator
