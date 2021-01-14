import asyncio
from contextvars import ContextVar
from enum import Enum
from weakref import WeakKeyDictionary

import aiormq

current_transaction = ContextVar('current_transaction')


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

        self.restore_object_operations = WeakKeyDictionary()

        self.token = None

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

        self.token = current_transaction.set(self)
        self.state = TransactionStates.started

        return result

    async def rollback(self, timeout=None):
        try:
            result = await asyncio.wait_for(
                self.channel.tx_rollback(), timeout=timeout,
            )
            self.state = TransactionStates.rolled_back

            return result
        finally:
            current_transaction.reset(self.token)

            for obj, callback in self.restore_object_operations.items():
                callback(obj)

            self.restore_object_operations.clear()

    async def commit(self, timeout=None):
        try:
            result = await asyncio.wait_for(
                self.channel.tx_commit(), timeout=timeout,
            )
            self.state = TransactionStates.commited

            return result
        finally:
            current_transaction.reset(self.token)

            self.restore_object_operations.clear()

    async def __aenter__(self):
        return await self.select()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.rollback()
        else:
            await self.commit()

    def _add_restore_operation(self, obj, operation):
        self.restore_object_operations[obj] = operation
