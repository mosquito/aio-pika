import asyncio
from enum import Enum

import aiormq

from .tools import OPERATION_TIMEOUT
from .types import TimeoutType


class TransactionStates(Enum):
    created = 'created'
    commited = 'commited'
    rolled_back = 'rolled back'
    started = 'started'


class Transaction:
    def __str__(self):
        return self.state.value

    def __init__(self, connection, channel):
        self.loop = connection.loop
        self._connection = connection
        self._channel = channel
        self.state = TransactionStates.created      # type: TransactionStates

    @property
    def channel(self) -> aiormq.Channel:
        if self._channel is None:
            raise RuntimeError("Channel not opened")

        if self._channel.is_closed:
            raise RuntimeError('Closed channel')

        return self._channel

    def _get_operation_timeout(self, timeout: TimeoutType):
        return (
            self._connection.operation_timeout if timeout is OPERATION_TIMEOUT
            else timeout
        )

    async def select(
        self, timeout: TimeoutType = OPERATION_TIMEOUT
    ) -> aiormq.spec.Tx.SelectOk:
        result = await asyncio.wait_for(
            self.channel.tx_select(),
            timeout=self._get_operation_timeout(timeout)
        )

        self.state = TransactionStates.started
        return result

    async def rollback(self, timeout: TimeoutType = OPERATION_TIMEOUT):
        result = await asyncio.wait_for(
            self.channel.tx_rollback(),
            timeout=self._get_operation_timeout(timeout)
        )
        self.state = TransactionStates.rolled_back
        return result

    async def commit(self, timeout: TimeoutType = OPERATION_TIMEOUT):
        result = await asyncio.wait_for(
            self.channel.tx_commit(),
            timeout=self._get_operation_timeout(timeout)
        )

        self.state = TransactionStates.commited
        return result

    async def __aenter__(self):
        return await self.select()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.rollback()
        else:
            await self.commit()
