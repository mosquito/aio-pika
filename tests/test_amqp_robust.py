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

    async def test_add_reconnect_callback(self, create_connection):
        connection = await create_connection()

        def cb(*a, **kw):
            pass

        connection.add_reconnect_callback(cb)

        del cb
        assert len(connection.reconnect_callbacks) == 1


class TestCaseAmqpNoConfirmsRobust(TestCaseAmqpNoConfirms):
    pass


class TestCaseAmqpWithConfirmsRobust(TestCaseAmqpWithConfirms):
    pass
