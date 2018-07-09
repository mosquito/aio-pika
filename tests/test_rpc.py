import asyncio
import logging

import pytest

from aio_pika import Message
from aio_pika.exceptions import UnroutableError
from aio_pika.patterns.rpc import RPC, log as rpc_logger
from tests.test_amqp import BaseTestCase


def rpc_func(*, foo, bar):
    assert not foo
    assert not bar

    return {'foo': 'bar'}


class TestCase(BaseTestCase):

    @pytest.mark.asyncio
    def test_simple(self):
        channel = yield from self.create_channel()
        rpc = yield from RPC.create(channel, auto_delete=True)

        yield from rpc.register('test.rpc', rpc_func, auto_delete=True)

        result = yield from rpc.proxy.test.rpc(foo=None, bar=None)
        self.assertDictEqual(result, {'foo': 'bar'})

        yield from rpc.unregister(rpc_func)
        yield from rpc.close()

        # Close already closed
        yield from rpc.close()

    @asyncio.coroutine
    def test_error(self):
        channel = yield from self.create_channel()
        rpc = yield from RPC.create(channel, auto_delete=True)

        yield from rpc.register('test.rpc', rpc_func, auto_delete=True)

        with self.assertRaises(AssertionError):
            yield from rpc.proxy.test.rpc(foo=True, bar=None)

        yield from rpc.unregister(rpc_func)
        yield from rpc.close()

    @asyncio.coroutine
    def test_unroutable(self):
        channel = yield from self.create_channel()
        rpc = yield from RPC.create(channel, auto_delete=True)

        yield from rpc.register('test.rpc', rpc_func, auto_delete=True)

        with self.assertRaises(UnroutableError):
            yield from rpc.proxy.unroutable()

        yield from rpc.unregister(rpc_func)
        yield from rpc.close()

    @asyncio.coroutine
    def test_timed_out(self):
        channel = yield from self.create_channel()
        rpc = yield from RPC.create(channel, auto_delete=True)

        yield from rpc.register('test.rpc', rpc_func, auto_delete=True)

        yield from channel.declare_queue(
            'test.timed_out', auto_delete=True, arguments={
                'x-dead-letter-exchange': RPC.DLX_NAME,
            }
        )

        with self.assertRaises(asyncio.TimeoutError):
            yield from rpc.call('test.timed_out', expiration=1)

        yield from rpc.unregister(rpc_func)
        yield from rpc.close()

    @asyncio.coroutine
    def test_close_twice(self):
        channel = yield from self.create_channel()
        rpc = yield from RPC.create(channel, auto_delete=True)

        yield from rpc.close()
        yield from rpc.close()

    @asyncio.coroutine
    def test_init_twice(self):
        channel = yield from self.create_channel()
        rpc = yield from RPC.create(channel, auto_delete=True)

        yield from rpc.initialize()

        yield from rpc.close()

    @asyncio.coroutine
    def test_send_unknown_message(self):
        channel = yield from self.create_channel()
        rpc = yield from RPC.create(channel, auto_delete=True)

        with self.assertLogs(rpc_logger, logging.WARNING) as capture:
            yield from channel.default_exchange.publish(
                Message(b''), routing_key=rpc.result_queue.name
            )

            yield from asyncio.sleep(0.5, loop=self.loop)

        self.assertIn('Unknown message: ', capture.output[0])

        with self.assertLogs(rpc_logger, logging.WARNING) as capture:
            yield from channel.default_exchange.publish(
                Message(b''), routing_key='should-returned'
            )

            yield from asyncio.sleep(0.5, loop=self.loop)

        self.assertIn('Unknown message was returned: ', capture.output[0])

        yield from rpc.close()

    @asyncio.coroutine
    def test_close_cancelling(self):
        channel = yield from self.create_channel()
        rpc = yield from RPC.create(channel, auto_delete=True)

        @asyncio.coroutine
        def sleeper():
            yield from asyncio.sleep(60, loop=self.loop)

        yield from rpc.register('test.sleeper', sleeper, auto_delete=True)

        tasks = set()

        for _ in range(10):
            tasks.add(self.loop.create_task(rpc.proxy.test.sleeper()))

        yield from rpc.close()

        for task in tasks:
            with self.assertRaises(asyncio.CancelledError):
                yield from task

    @asyncio.coroutine
    def test_register_twice(self):
        channel = yield from self.create_channel()
        rpc = yield from RPC.create(channel, auto_delete=True)

        yield from rpc.register('test.sleeper', lambda x: None, auto_delete=True)

        with self.assertRaises(RuntimeError):
            yield from rpc.register(
                'test.sleeper', lambda x: None, auto_delete=True
            )

        yield from rpc.register(
            'test.one', rpc_func, auto_delete=True
        )

        with self.assertRaises(RuntimeError):
            yield from rpc.register(
                'test.two', rpc_func, auto_delete=True
            )

        yield from rpc.unregister(rpc_func)
        yield from rpc.unregister(rpc_func)

        yield from rpc.close()
