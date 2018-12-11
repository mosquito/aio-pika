import asyncio
import logging
from typing import AsyncContextManager, TypeVar, Coroutine, Callable, Any


T = TypeVar("T")
ItemType = Coroutine[Any, None, T]
ConstructorType = Callable[[], ItemType]


log = logging.getLogger(__name__)


class Pool:
    __slots__ = (
        'loop', '__max_size', '__items',
        '__constructor', '__created', '__lock'
    )

    def __init__(self, constructor: ConstructorType, max_size: int = None,
                 loop: asyncio.AbstractEventLoop = None):

        self.loop = loop or asyncio.get_event_loop()
        self.__max_size = max_size or -1
        self.__items = asyncio.Queue(loop=self.loop)
        self.__constructor = constructor
        self.__created = 0
        self.__lock = asyncio.Lock(loop=self.loop)

    def acquire(self) -> 'PoolItemContextManager':
        return PoolItemContextManager(self)

    @property
    def _is_overflow(self):
        return self.__created >= self.__max_size

    async def _get(self) -> T:
        if self._is_overflow:
            return await self.__items.get()

        async with self.__lock:
            if self._is_overflow:
                return await self._get()

            log.debug('Creating a new instance of %r', self.__constructor)
            item = await self.__constructor()
            self.__created += 1
            return item

    def put(self, item: ItemType):
        return self.__items.put_nowait(item)


class PoolItemContextManager(AsyncContextManager):
    __slots__ = 'pool', 'item'

    def __init__(self, pool: Pool):
        self.pool = pool
        self.item = None

    async def __aenter__(self) -> T:
        self.item = await self.pool._get()
        return self.item

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.pool.put(self.item)
