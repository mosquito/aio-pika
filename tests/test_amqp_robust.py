from functools import partial

import pytest

import aio_pika
from tests.test_amqp import (
    TestCaseAmqp, TestCaseAmqpNoConfirms, TestCaseAmqpWithConfirms,
)


@pytest.fixture
def connection_fabric():
    return aio_pika.connect_robust


@pytest.fixture
def create_connection(connection_fabric, loop, amqp_url):
    return partial(connection_fabric, amqp_url, loop=loop)


class TestCaseNoRobust(TestCaseAmqp):
    PARAMS = [{"robust": True}, {"robust": False}]
    IDS = ["robust=1", "robust=0"]

    @staticmethod
    @pytest.fixture(name="declare_queue", params=PARAMS, ids=IDS)
    def declare_queue_(request, declare_queue):
        async def fabric(*args, **kwargs) -> aio_pika.Queue:
            kwargs.update(request.param)
            return await declare_queue(*args, **kwargs)

        return fabric

    @staticmethod
    @pytest.fixture(name="declare_exchange", params=PARAMS, ids=IDS)
    def declare_exchange_(request, declare_exchange):
        async def fabric(*args, **kwargs) -> aio_pika.Queue:
            kwargs.update(request.param)
            return await declare_exchange(*args, **kwargs)

        return fabric

    async def test_robust_duplicate_queue(self):
        queue_name = "test"
        channel1 = await self.create_channel()
        channel2 = await self.create_channel()

        shared = []
        queue1 = await channel1.declare_queue(queue_name)
        queue2 = await channel1.declare_queue(queue_name)

        async def reader(queue):
            nonlocal shared
            async with queue.iterator() as q:
                async for message in q:
                    shared.append(message)
                    await message.ack()

        reader_task1 = self.loop.create_task(reader(queue1))
        reader_task2 = self.loop.create_task(reader(queue2))
        self.addCleanup(reader_task1.cancel)
        self.addCleanup(reader_task2.cancel)

        for _ in range(5):
            await channel2.default_exchange.publish(
                Message(b''), queue_name,
            )

        logging.info("Disconnect all clients")
        await self.proxy.disconnect()

        logging.info("Waiting for reconnect")
        await asyncio.sleep(5)

        logging.info("Waiting connections")
        await asyncio.wait([
            channel1._connection.ready(),
            channel2._connection.ready()
        ])

        for _ in range(5):
            await channel2.default_exchange.publish(
                Message(b''), queue_name,
            )

        while len(shared) < 10:
            await asyncio.sleep(0.1)

        self.assertEqual(len(shared), 10)


class TestCaseAmqpNoConfirmsRobust(TestCaseAmqpNoConfirms):
    pass


class TestCaseAmqpWithConfirmsRobust(TestCaseAmqpWithConfirms):
    pass
