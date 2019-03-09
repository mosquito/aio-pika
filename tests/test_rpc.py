import asyncio
import logging

import pytest

from aio_pika import Message, connect_robust
from aio_pika.exceptions import DeliveryError
from aio_pika.patterns.rpc import RPC, log as rpc_logger
from tests import AMQP_URL
from tests.test_amqp import BaseTestCase


def rpc_func(*, foo, bar):
    assert not foo
    assert not bar

    return {'foo': 'bar'}


class TestCase(BaseTestCase):

    async def test_simple(self):
        channel = await self.create_channel()
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register('test.rpc', rpc_func, auto_delete=True)

        result = await rpc.proxy.test.rpc(foo=None, bar=None)
        self.assertDictEqual(result, {'foo': 'bar'})

        await rpc.unregister(rpc_func)
        await rpc.close()

        # Close already closed
        await rpc.close()

    async def test_error(self):
        channel = await self.create_channel()
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register('test.rpc', rpc_func, auto_delete=True)

        with pytest.raises(AssertionError):
            await rpc.proxy.test.rpc(foo=True, bar=None)

        await rpc.unregister(rpc_func)
        await rpc.close()

    async def test_unroutable(self):
        channel = await self.create_channel()
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register('test.rpc', rpc_func, auto_delete=True)

        with pytest.raises(DeliveryError):
            await rpc.proxy.unroutable()

        await rpc.unregister(rpc_func)
        await rpc.close()

    async def test_timed_out(self):
        channel = await self.create_channel()
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register('test.rpc', rpc_func, auto_delete=True)

        await channel.declare_queue(
            'test.timed_out', auto_delete=True, arguments={
                'x-dead-letter-exchange': RPC.DLX_NAME,
            }
        )

        with pytest.raises(asyncio.TimeoutError):
            await rpc.call('test.timed_out', expiration=1)

        await rpc.unregister(rpc_func)
        await rpc.close()

    async def test_close_twice(self):
        channel = await self.create_channel()
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.close()
        await rpc.close()

    async def test_init_twice(self):
        channel = await self.create_channel()
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.initialize()

        await rpc.close()

    async def test_send_unknown_message(self):
        channel = await self.create_channel()
        rpc = await RPC.create(channel, auto_delete=True)

        with self.assertLogs(rpc_logger, logging.WARNING) as capture:
            await channel.default_exchange.publish(
                Message(b''), routing_key=rpc.result_queue.name
            )

            await asyncio.sleep(0.5, loop=self.loop)

        self.assertIn('Unknown message: ', capture.output[0])

        with self.assertLogs(rpc_logger, logging.WARNING) as capture:
            await channel.default_exchange.publish(
                Message(b''), routing_key='should-returned'
            )

            await asyncio.sleep(0.5, loop=self.loop)

        self.assertIn('Unknown message was returned: ', capture.output[0])

        await rpc.close()

    async def test_close_cancelling(self):
        channel = await self.create_channel()
        rpc = await RPC.create(channel, auto_delete=True)

        async def sleeper():
            await asyncio.sleep(60, loop=self.loop)

        method_name = self.get_random_name('test', 'sleeper')

        await rpc.register(method_name, sleeper, auto_delete=True)

        tasks = set()

        for _ in range(10):
            tasks.add(self.loop.create_task(rpc.call(method_name)))

        await rpc.close()

        logging.info("Waiting for results")
        for task in tasks:
            with pytest.raises(asyncio.CancelledError):
                await task

    async def test_register_twice(self):
        channel = await self.create_channel()
        rpc = await RPC.create(channel, auto_delete=True)

        await rpc.register('test.sleeper', lambda x: None, auto_delete=True)

        with pytest.raises(RuntimeError):
            await rpc.register(
                'test.sleeper', lambda x: None, auto_delete=True
            )

        await rpc.register(
            'test.one', rpc_func, auto_delete=True
        )

        with pytest.raises(RuntimeError):
            await rpc.register(
                'test.two', rpc_func, auto_delete=True
            )

        await rpc.unregister(rpc_func)
        await rpc.unregister(rpc_func)

        await rpc.close()


class TestCaseRobust(TestCase):
    async def create_connection(self, cleanup=True):
        client = await connect_robust(str(AMQP_URL), loop=self.loop)

        if cleanup:
            self.addCleanup(client.close)

        return client
