import pickle
from typing import Any


class Method:
    __slots__ = 'name', 'func',

    def __init__(self, name, func):
        self.name = name
        self.func = func

    def __getattr__(self, item):
        return Method(".".join((self.name, item)), func=self.func)

    def __call__(self, **kwargs):
        return self.func(self.name, kwargs=kwargs)


class Proxy:
    __slots__ = 'func',

    def __init__(self, func):
        self.func = func

    def __getattr__(self, item):
        return Method(item, self.func)


class Base:
    @staticmethod
    def serialize(data: Any) -> bytes:
        return pickle.dumps(data)

    @staticmethod
    def deserialize(data: bytes) -> Any:
        return pickle.loads(data)
