import logging
from copy import copy
from unittest import mock

import pytest

from aio_pika.tools import CallbackCollection


log = logging.getLogger(__name__)


# noinspection PyTypeChecker
class TestCase:
    @pytest.fixture
    def instance(self):
        return mock.MagicMock()

    @pytest.fixture
    def collection(self, instance):
        return CallbackCollection(instance)

    def test_basic(self, collection):
        def func(sender, *args, **kwargs):
            pass

        collection.add(func)

        assert func in collection

        with pytest.raises(ValueError):
            collection.add(None)

        collection.remove(func)

        with pytest.raises(LookupError):
            collection.remove(func)

        for _ in range(10):
            collection.add(func)

        assert len(collection) == 1

        collection.freeze()

        with pytest.raises(RuntimeError):
            collection.freeze()

        assert len(collection) == 1

        with pytest.raises(RuntimeError):
            collection.add(func)

        with pytest.raises(RuntimeError):
            collection.remove(func)

        with pytest.raises(RuntimeError):
            collection.clear()

        collection2 = copy(collection)
        collection.unfreeze()

        assert not copy(collection).is_frozen

        assert collection.is_frozen != collection2.is_frozen

        with pytest.raises(RuntimeError):
            collection.unfreeze()

        collection.clear()
        assert collection2
        assert not collection

    def test_callback_call(self, collection):
        l1 = list()
        l2 = list()

        assert l1 == l2

        collection.add(lambda sender, x: l1.append(x), weak=False)
        collection.add(lambda sender, x: l2.append(x), weak=False)

        collection(1)
        collection(2)

        assert l1 == l2
        assert l1 == [1, 2]
