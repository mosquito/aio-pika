import asyncio
from itertools import chain
from threading import Lock
from typing import (
    AbstractSet, Any, Awaitable, Callable, Coroutine, Generator, Iterator, List,
    MutableSet, Optional, TypeVar, Union,
)
from weakref import ReferenceType, WeakSet, ref

from aio_pika.log import get_logger


log = get_logger(__name__)
T = TypeVar("T")


def iscoroutinepartial(fn: Callable[..., Any]) -> bool:
    """
    Function returns True if function is a partial instance of coroutine.
    See additional information here_.

    :param fn: Function
    :return: bool

    .. _here: https://goo.gl/C0S4sQ

    """

    while True:
        parent = fn

        fn = getattr(parent, "func", None)  # type: ignore

        if fn is None:
            break

    return asyncio.iscoroutinefunction(parent)


def _task_done(future: asyncio.Future) -> None:
    if not future.cancelled():
        exc = future.exception()
        if exc is not None:
            raise exc


def create_task(
    func: Callable[..., Union[Coroutine[Any, Any, T], Awaitable[T]]],
    *args: Any,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    **kwargs: Any,
) -> Awaitable[T]:
    loop = loop or asyncio.get_event_loop()

    if iscoroutinepartial(func):
        task = loop.create_task(func(*args, **kwargs))      # type: ignore
        task.add_done_callback(_task_done)
        return task

    def run(future: asyncio.Future) -> Optional[asyncio.Future]:
        if future.done():
            return None

        try:
            future.set_result(func(*args, **kwargs))
        except Exception as e:
            future.set_exception(e)

        return future

    future = loop.create_future()
    future.add_done_callback(_task_done)
    loop.call_soon(run, future)
    return future


CallbackType = Callable[..., Union[T, Awaitable[T]]]
CallbackSetType = AbstractSet[CallbackType]


class StubAwaitable:
    __slots__ = ()

    def __await__(self) -> Generator[Any, Any, None]:
        yield


STUB_AWAITABLE = StubAwaitable()


class CallbackCollection(MutableSet):
    __slots__ = (
        "__weakref__",
        "__sender",
        "__callbacks",
        "__weak_callbacks",
        "__lock",
    )

    def __init__(self, sender: Union[T, ReferenceType]):
        self.__sender: ReferenceType
        if isinstance(sender, ReferenceType):
            self.__sender = sender
        else:
            self.__sender = ref(sender)

        self.__callbacks: CallbackSetType = set()
        self.__weak_callbacks: MutableSet[CallbackType] = WeakSet()
        self.__lock: Lock = Lock()

    def add(self, callback: CallbackType, weak: bool = False) -> None:
        if self.is_frozen:
            raise RuntimeError("Collection frozen")
        if not callable(callback):
            raise ValueError("Callback is not callable")

        with self.__lock:
            if weak:
                self.__weak_callbacks.add(callback)
            else:
                self.__callbacks.add(callback)      # type: ignore

    def discard(self, callback: CallbackType) -> None:
        if self.is_frozen:
            raise RuntimeError("Collection frozen")

        with self.__lock:
            if callback in self.__callbacks:
                self.__callbacks.remove(callback)    # type: ignore
            elif callback in self.__weak_callbacks:
                self.__weak_callbacks.remove(callback)

    def clear(self) -> None:
        if self.is_frozen:
            raise RuntimeError("Collection frozen")

        with self.__lock:
            self.__callbacks.clear()        # type: ignore
            self.__weak_callbacks.clear()

    @property
    def is_frozen(self) -> bool:
        return isinstance(self.__callbacks, frozenset)

    def freeze(self) -> None:
        if self.is_frozen:
            raise RuntimeError("Collection already frozen")

        with self.__lock:
            self.__callbacks = frozenset(self.__callbacks)
            self.__weak_callbacks = WeakSet(self.__weak_callbacks)

    def unfreeze(self) -> None:
        if not self.is_frozen:
            raise RuntimeError("Collection is not frozen")

        with self.__lock:
            self.__callbacks = set(self.__callbacks)
            self.__weak_callbacks = WeakSet(self.__weak_callbacks)

    def __contains__(self, x: object) -> bool:
        return x in self.__callbacks or x in self.__weak_callbacks

    def __len__(self) -> int:
        return len(self.__callbacks) + len(self.__weak_callbacks)

    def __iter__(self) -> Iterator[CallbackType]:
        return iter(chain(self.__callbacks, self.__weak_callbacks))

    def __bool__(self) -> bool:
        return bool(self.__callbacks) or bool(self.__weak_callbacks)

    def __copy__(self) -> "CallbackCollection":
        instance = self.__class__(self.__sender)

        with self.__lock:
            for cb in self.__callbacks:
                instance.add(cb, weak=False)

            for cb in self.__weak_callbacks:
                instance.add(cb, weak=True)

        if self.is_frozen:
            instance.freeze()

        return instance

    def __call__(self, *args: Any, **kwargs: Any) -> Awaitable[Any]:
        futures: List[asyncio.Future] = []

        with self.__lock:
            sender = self.__sender()

            for cb in self:
                try:
                    result = cb(sender, *args, **kwargs)
                    if hasattr(result, "__await__"):
                        futures.append(asyncio.ensure_future(result))
                except Exception:
                    log.exception("Callback %r error", cb)

        if not futures:
            return STUB_AWAITABLE
        return asyncio.gather(*futures, return_exceptions=True)

    def __hash__(self) -> int:
        return id(self)


class OneShotCallback:
    __slots__ = ("loop", "finished", "__lock", "callback", "__task")

    def __init__(self, callback: Callable[..., Awaitable[T]]):
        self.callback: Callable[..., Awaitable[T]] = callback
        self.loop = asyncio.get_event_loop()
        self.finished: asyncio.Event = asyncio.Event()
        self.__lock: asyncio.Lock = asyncio.Lock()
        self.__task: Optional[asyncio.Future] = None

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: cb={self.callback!r}>"

    def wait(self) -> Awaitable[Any]:
        try:
            return self.finished.wait()
        except asyncio.CancelledError:
            if self.__task is not None:
                self.__task.cancel()
            raise

    async def __task_inner(self, *args: Any, **kwargs: Any) -> None:
        async with self.__lock:
            if self.finished.is_set():
                return

            try:
                await self.callback(*args, **kwargs)
            finally:
                self.loop.call_soon(self.finished.set)
                del self.callback

    def __call__(self, *args: Any, **kwargs: Any) -> Awaitable[Any]:
        if self.finished.is_set() or self.__task is not None:
            return STUB_AWAITABLE

        self.__task = self.loop.create_task(
            self.__task_inner(*args, **kwargs),
        )
        return self.__task


__all__ = (
    "CallbackCollection",
    "CallbackType",
    "CallbackSetType",
    "OneShotCallback",
    "create_task",
    "iscoroutinepartial",
)
