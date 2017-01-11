import asyncio
import asynctest
import logging
import os

from functools import wraps
from yarl import URL


log = logging.getLogger(__name__)


for logger_name in ('pika.channel', 'pika.callback', 'pika.connection'):
    logging.getLogger(logger_name).setLevel(logging.INFO)


logging.basicConfig(level=logging.DEBUG)


class AsyncTestCase(asynctest.TestCase):
    forbid_get_event_loop = True


AMQP_URL = URL(os.getenv("AMQP_URL", "amqp://guest:guest@localhost"))

if not AMQP_URL.path:
    AMQP_URL.path = '/'


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
