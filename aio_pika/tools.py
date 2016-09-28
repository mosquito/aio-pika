import asyncio
from functools import partial


def _on_result(future: asyncio.Future, new_future: asyncio.Future=None):
    if not new_future.done():
        exc = future.exception()
        if exc:
            return new_future.set_exception(exc)

        new_future.set_result(future.result())


def copy_future(future: asyncio.Future, new_future: asyncio.Future=None):
    new_future = new_future or asyncio.Future(loop=future._loop)

    handler = partial(_on_result, new_future=new_future)

    future.add_done_callback(handler)
    return new_future


@asyncio.coroutine
def wait(tasks, loop=None):
    loop = loop or asyncio.get_event_loop()
    done = yield from asyncio.gather(*list(tasks), loop=loop)
    return tuple(map(lambda x: x.result() if isinstance(x, asyncio.Future) else x, done))
