try:
    from typing import Awaitable
except ImportError:
    from typing import Generic, TypeVar
    from abc import ABCMeta, abstractmethod

    T_co = TypeVar('T_co', covariant=True)

    class _Awaitable(metaclass=ABCMeta):

        __slots__ = ()

        @abstractmethod
        def __await__(self):
            yield

    class Awaitable(Generic[T_co], extra=_Awaitable):
        __slots__ = ()
