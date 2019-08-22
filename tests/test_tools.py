import logging
from unittest import TestCase as BaseTestCase

from aio_pika.tools import CallbackCollection


log = logging.getLogger(__name__)


# noinspection PyTypeChecker
class TestCase(BaseTestCase):
    @classmethod
    def make_collection(cls):
        return CallbackCollection()

    def test_basic(self):
        collection = self.make_collection()

        def func():
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

        collection.unfreeze()

        with self.assertRaises(RuntimeError):
            collection.unfreeze()

        collection.clear()
