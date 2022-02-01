import asyncio
import logging
from functools import wraps
from itertools import chain
from threading import Lock
from typing import (
    AbstractSet, Any, Awaitable, Callable, Coroutine, Iterator, MutableSet,
    Optional, TypeVar, Union,
)
from weakref import ReferenceType, WeakSet, ref


log = logging.getLogger(__name__)
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


def create_task(
    func: Callable[..., Union[Coroutine[Any, Any, T], Awaitable[T]]],
    *args: Any,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    **kwargs: Any
) -> Awaitable[T]:
    loop = loop or asyncio.get_event_loop()

    if iscoroutinepartial(func):
        return loop.create_task(func(*args, **kwargs))

    def run(future: asyncio.Future) -> Optional[asyncio.Future]:
        if future.done():
            return None

        try:
            future.set_result(func(*args, **kwargs))
        except Exception as e:
            future.set_exception(e)

        return future

    future = loop.create_future()
    loop.call_soon(run, future)
    return future


def task(
    func: Callable[..., Coroutine[Any, Any, T]],
) -> Callable[..., Awaitable[T]]:
    @wraps(func)
    def wrap(*args: Any, **kwargs: Any) -> Awaitable[T]:
        return create_task(func, *args, **kwargs)
    return wrap


def shield(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
    @wraps(func)
    def wrap(*args: Any, **kwargs: Any) -> Awaitable[T]:
        return asyncio.shield(func(*args, **kwargs))

    return wrap


CallbackType = Callable[..., Any]
CallbackSetType = Union[AbstractSet[CallbackType]]


class CallbackCollection(MutableSet):
    __slots__ = "__sender", "__callbacks", "__weak_callbacks", "__lock"

    def __init__(self, sender: Union[T, ReferenceType]):
        self.__sender: ReferenceType
        if isinstance(sender, ReferenceType):
            self.__sender = sender
        else:
            self.__sender = ref(sender)

        self.__callbacks: CallbackSetType = set()
        self.__weak_callbacks: MutableSet[CallbackType] = WeakSet()
        self.__lock: Lock = Lock()

    def add(
        self, callback: Callable[..., Any], weak: bool = False,
    ) -> None:
        if self.is_frozen:
            raise RuntimeError("Collection frozen")
        if not callable(callback):
            raise ValueError("Callback is not callable")

        with self.__lock:
            if weak:
                self.__weak_callbacks.add(callback)
            else:
                self.__callbacks.add(callback)      # type: ignore

    def discard(self, callback: Callable[..., Any]) -> None:
        if self.is_frozen:
            raise RuntimeError("Collection frozen")

        with self.__lock:
            try:
                self.__callbacks.remove(callback)    # type: ignore
            except KeyError:
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

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        with self.__lock:
            sender = self.__sender()

            for cb in self:
                try:
                    cb(sender, *args, **kwargs)
                except Exception:
                    log.exception("Callback %r error", cb)

    def __hash__(self) -> int:
        return id(self)


__all__ = (
    "CallbackCollection",
    "create_task",
    "iscoroutinepartial",
    "shield",
    "task",
)
