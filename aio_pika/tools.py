import asyncio
from functools import wraps


__all__ = 'create_task', 'iscoroutinepartial', 'shield'


def iscoroutinepartial(fn):
    """
    Function returns True if function it's a partial instance of coroutine.
    See additional information here_.

    :param fn: Function
    :return: bool

    .. _here: https://goo.gl/C0S4sQ

    """

    while True:
        parent = fn

        fn = getattr(parent, 'func', None)

        if fn is None:
            break

    return asyncio.iscoroutinefunction(parent)


def create_task(func, *args, loop=None, **kwargs):
    loop = loop or asyncio.get_event_loop()

    if iscoroutinepartial(func):
        return loop.create_task(func(*args, **kwargs))

    def run(future):
        if future.done():
            return

        try:
            future.set_result(func(*args, **kwargs))
        except Exception as e:
            future.set_exception(e)

        return future

    future = loop.create_future()
    loop.call_soon(run, future)
    return future


def shield(func):
    """
    Simple and useful decorator for wrap the coroutine to `asyncio.shield`.
    """

    async def awaiter(future):
        return await future

    @wraps(func)
    def wrap(*args, **kwargs):
        return wraps(func)(awaiter)(asyncio.shield(func(*args, **kwargs)))

    return wrap
