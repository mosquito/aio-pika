import asyncio
from functools import partial, wraps

__all__ = 'wait', 'create_task', 'iscoroutinepartial', 'shield'


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


def create_task(*, loop=None):
    """ Helper for `create a new Task`_ with backward compatibility
    for Python 3.4

    .. _create a new Task: https://goo.gl/g4pMV9
    """

    loop = loop or asyncio.get_event_loop()

    try:
        return loop.create_task
    except AttributeError:
        return partial(asyncio.ensure_future, loop=loop)


async def wait(tasks, loop=None):
    """
    Simple helper for gathering all passed :class:`Task`s.

    :param tasks: list of the :class:`asyncio.Task`s
    :param loop:
        Event loop (:func:`asyncio.get_event_loop()` when :class:`None`)
    :return: :class:`tuple` of results
    """

    loop = loop or asyncio.get_event_loop()
    done = await asyncio.gather(*list(tasks), loop=loop)
    return tuple(
        map(
            lambda x: x.result() if isinstance(x, asyncio.Future) else x, done
        )
    )


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
