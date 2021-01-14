import asyncio
from enum import Enum

import aiormq


class TransactionStates(Enum):
    created = "created"
    commited = "commited"
    rolled_back = "rolled back"
    started = "started"


class Transaction:
    def __str__(self):
        return self.state.value

    def __init__(self, channel):
        self.loop = channel.loop
        self._channel = channel
        self.state = TransactionStates.created  # type: TransactionStates

    @property
    def channel(self) -> aiormq.Channel:
        if self._channel is None:
            raise RuntimeError("Channel not opened")

        if self._channel.is_closed:
            raise RuntimeError("Closed channel")

        return self._channel

    async def select(self, timeout=None) -> aiormq.spec.Tx.SelectOk:
        result = await asyncio.wait_for(
            self.channel.tx_select(), timeout=timeout,
        )

        self.state = TransactionStates.started
        return result

    async def rollback(self, timeout=None):
        result = await asyncio.wait_for(
            self.channel.tx_rollback(), timeout=timeout,
        )
        self.state = TransactionStates.rolled_back
        return result

    async def commit(self, timeout=None):
        result = await asyncio.wait_for(
            self.channel.tx_commit(), timeout=timeout,
        )

        self.state = TransactionStates.commited
        return result

    async def __aenter__(self):
        await self.select()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.rollback()
        else:
            await self.commit()
