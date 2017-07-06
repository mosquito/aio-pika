import asyncio
from functools import partial


__all__ = 'wait', 'create_future', 'create_task', 'iscoroutinepartial'


def iscoroutinepartial(fn):
    """
    Function returns True if function it's a partial instance of coroutine. See additional information here_.

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


def create_future(*, loop):
    """ Helper for `create a new future`_ with backward compatibility for Python 3.4

    .. _create a new future: https://goo.gl/YrzGQ6
    """

    try:
        return loop.create_future()
    except AttributeError:
        return asyncio.Future(loop=loop)


def create_task(*, loop=None):
    """ Helper for `create a new Task`_ with backward compatibility for Python 3.4

    .. _create a new Task: https://goo.gl/g4pMV9
    """

    loop = loop or asyncio.get_event_loop()

    try:
        return loop.create_task
    except AttributeError:
        return partial(asyncio.ensure_future, loop=loop)


@asyncio.coroutine
def wait(tasks, loop=None):
    """
    Simple helper for gathering all passed :class:`Task`s.

    :param tasks: list of the :class:`asyncio.Task`s
    :param loop: Event loop (:func:`asyncio.get_event_loop()` when :class:`None`)
    :return: :class:`tuple` of results
    """

    loop = loop or asyncio.get_event_loop()
    done = yield from asyncio.gather(*list(tasks), loop=loop)
    return tuple(map(lambda x: x.result() if isinstance(x, asyncio.Future) else x, done))
