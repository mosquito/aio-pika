import asyncio
from functools import partial


def iscoroutinepartial(fn):
    # http://bugs.python.org/issue23519

    while True:
        parent = fn

        fn = getattr(parent, 'func', None)

        if fn is None:
            break

    return asyncio.iscoroutinefunction(parent)


def create_future(*, loop):
        # https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.AbstractEventLoop.create_task
    try:
        return loop.create_future()
    except AttributeError:
        return asyncio.Future(loop=loop)


def create_task(*, loop=None):
    # https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.AbstractEventLoop.create_task
    loop = loop or asyncio.get_event_loop()

    try:
        return loop.create_task
    except AttributeError:
        return partial(asyncio.ensure_future, loop=loop)


def _on_result(future: asyncio.Future, new_future: asyncio.Future=None):
    if not new_future.done():
        exc = future.exception()
        if exc:
            return new_future.set_exception(exc)

        new_future.set_result(future.result())


def copy_future(future: asyncio.Future, new_future: asyncio.Future=None):
    new_future = new_future or create_future(loop=future._loop)

    handler = partial(_on_result, new_future=new_future)

    future.add_done_callback(handler)
    return new_future


@asyncio.coroutine
def wait(tasks, loop=None):
    loop = loop or asyncio.get_event_loop()
    done = yield from asyncio.gather(*list(tasks), loop=loop)
    return tuple(map(lambda x: x.result() if isinstance(x, asyncio.Future) else x, done))
