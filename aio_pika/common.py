import asyncio
from contextlib import suppress

from logging import getLogger
from functools import wraps
from enum import Enum, unique
from typing import Union

from .tools import create_future
from . import exceptions


log = getLogger(__name__)


@unique
class ConfirmationTypes(Enum):
    ACK = 'ack'
    NACK = 'nack'


def future_with_timeout(loop: asyncio.AbstractEventLoop,
                        timeout: Union[int, float],
                        future: asyncio.Future=None) -> asyncio.Future:

    loop = loop or asyncio.get_event_loop()
    f = future or create_future(loop=loop)

    def on_timeout():
        if f.done():
            return
        f.set_exception(asyncio.TimeoutError)

    if timeout:
        handler = loop.call_later(timeout, on_timeout)

        def on_result(*_):
            with suppress(Exception):
                handler.cancel()

        f.add_done_callback(on_result)

    return f


class FutureStore:
    __slots__ = "__collection", "__loop", "__main_store"

    def __init__(self, loop: asyncio.AbstractEventLoop, main_store: 'FutureStore'=None):
        self.__main_store = main_store
        self.__collection = set()
        self.__loop = loop or asyncio.get_event_loop()

    def _on_future_done(self, future):
        if future in self.__collection:
            self.__collection.remove(future)

    @staticmethod
    def _reject_future(future: asyncio.Future, exception: Exception):
        if future.done():
            return

        future.set_exception(exception)

    def add(self, future: asyncio.Future):
        if self.__main_store:
            self.__main_store.add(future)

        self.__collection.add(future)
        future.add_done_callback(self._on_future_done)

    def reject_all(self, exception: Exception):
        for future in list(self.__collection):
            self.__collection.remove(future)
            self.__loop.call_soon(self._reject_future, future, exception)

    @staticmethod
    def _on_timeout(future: asyncio.Future):
        if future.done():
            return

        future.set_exception(asyncio.TimeoutError)

    def create_future(self, timeout=None):
        future = future_with_timeout(self.__loop, timeout)

        self.add(future)

        if self.__main_store:
            self.__main_store.add(future)

        return future

    def get_child(self):
        return FutureStore(self.__loop, main_store=self)


class BaseChannel:
    __slots__ = ('_channel_futures', 'loop', '_futures', '_closing')

    def __init__(self, loop: asyncio.AbstractEventLoop, future_store: FutureStore):
        self.loop = loop
        self._futures = future_store
        self._closing = create_future(loop=self.loop)

    @property
    def is_closed(self):
        return self._closing.done()

    def _create_future(self, timeout=None):
        f = self._futures.create_future(timeout)
        return f

    @staticmethod
    def _ensure_channel_is_open(func):
        @wraps(func)
        @asyncio.coroutine
        def wrap(self, *args, **kwargs):
            if self.is_closed:
                raise exceptions.ChannelClosed

            return (yield from func(self, *args, **kwargs))

        return wrap

    def __repr__(self):
        return "<{}: {}>".format(self.__class__.__name__, getattr(self, 'name', id(self)))
