import abc
import asyncio
import logging
from typing import (
    Any, AsyncContextManager, Awaitable, Callable, Coroutine, Generic, TypeVar,
    Union,
)

from aiormq.tools import awaitable


log = logging.getLogger(__name__)


class PoolInstance(abc.ABC):
    @abc.abstractclassmethod
    async def close(cls):
        raise NotImplementedError


T = TypeVar("T")
ConstructorType = Union[
    Awaitable[PoolInstance],
    Callable[..., PoolInstance],
    Callable[..., Coroutine[Any, Any, PoolInstance]],
]


class PoolInvalidStateError(RuntimeError):
    pass


class Pool(Generic[T]):
    __slots__ = (
        "loop",
        "__max_size",
        "__items",
        "__constructor",
        "__created",
        "__lock",
        "__constructor_args",
        "__item_set",
        "__closed",
    )

    def __init__(
        self,
        constructor: ConstructorType,
        *args,
        max_size: int = None,
        loop: asyncio.AbstractEventLoop = None
    ):
        self.loop = loop or asyncio.get_event_loop()
        self.__closed = False
        self.__constructor = awaitable(constructor)
        self.__constructor_args = args or ()
        self.__created = 0
        self.__item_set = set()
        self.__items = asyncio.Queue()
        self.__lock = asyncio.Lock()
        self.__max_size = max_size

    @property
    def is_closed(self) -> bool:
        return self.__closed

    def acquire(self) -> "PoolItemContextManager[T]":
        if self.__closed:
            raise PoolInvalidStateError("acquire operation on closed pool")

        return PoolItemContextManager[T](self)

    @property
    def _has_released(self):
        return self.__items.qsize() > 0

    @property
    def _is_overflow(self) -> bool:
        if self.__max_size:
            return self.__created >= self.__max_size or self._has_released
        return self._has_released

    async def _create_item(self) -> T:
        if self.__closed:
            raise PoolInvalidStateError("create item operation on closed pool")

        async with self.__lock:
            if self._is_overflow:
                return await self.__items.get()

            log.debug("Creating a new instance of %r", self.__constructor)
            item = await self.__constructor(*self.__constructor_args)
            self.__created += 1
            self.__item_set.add(item)
            return item

    async def _get(self) -> T:
        if self.__closed:
            raise PoolInvalidStateError("get operation on closed pool")

        if self._is_overflow:
            return await self.__items.get()

        return await self._create_item()

    def put(self, item: T):
        if self.__closed:
            raise PoolInvalidStateError("put operation on closed pool")

        return self.__items.put_nowait(item)

    async def close(self):
        async with self.__lock:
            self.__closed = True
            tasks = []

            for item in self.__item_set:
                tasks.append(self.loop.create_task(item.close()))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def __aenter__(self) -> "Pool":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.__closed:
            return

        await asyncio.shield(self.close())


class PoolItemContextManager(Generic[T], AsyncContextManager):
    __slots__ = "pool", "item"

    def __init__(self, pool: Pool):
        self.pool = pool
        self.item = None

    async def __aenter__(self) -> T:
        # noinspection PyProtectedMember
        self.item = await self.pool._get()
        return self.item

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.item is not None:
            self.pool.put(self.item)
