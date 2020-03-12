import logging
from copy import copy
from unittest import TestCase as BaseTestCase

from aio_pika.tools import CallbackCollection


log = logging.getLogger(__name__)


# noinspection PyTypeChecker
class TestCase(BaseTestCase):
    @classmethod
    def make_collection(cls):
        return CallbackCollection(None)

    def test_basic(self):
        collection = self.make_collection()

        def func(sender, *args, **kwargs):
            pass

        collection.add(func)

        self.assertTrue(func in collection)

        with self.assertRaises(ValueError):
            collection.add(None)

        collection.remove(func)

        with self.assertRaises(LookupError):
            collection.remove(func)

        for _ in range(10):
            collection.add(func)

        self.assertEqual(len(collection), 1)

        collection.freeze()

        with self.assertRaises(RuntimeError):
            collection.freeze()

        self.assertEqual(len(collection), 1)

        with self.assertRaises(RuntimeError):
            collection.add(func)

        with self.assertRaises(RuntimeError):
            collection.remove(func)

        with self.assertRaises(RuntimeError):
            collection.clear()

        collection2 = copy(collection)
        collection.unfreeze()

        self.assertFalse(copy(collection).is_frozen)

        self.assertNotEqual(collection.is_frozen, collection2.is_frozen)

        with self.assertRaises(RuntimeError):
            collection.unfreeze()

        collection.clear()
        self.assertTrue(collection2)
        self.assertFalse(collection)

    def test_callback_call(self):
        l1 = list()
        l2 = list()

        self.assertListEqual(l1, l2)

        cbs = self.make_collection()

        cbs.add(lambda x: l1.append(x))
        cbs.add(lambda x: l2.append(x))

        cbs(1)
        cbs(2)

        self.assertListEqual(l1, l2)
        self.assertListEqual(l1, [1, 2])
