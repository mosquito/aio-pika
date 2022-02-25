from typing import Any

import msgpack  # type: ignore

from aio_pika.patterns import RPC, Master


class MsgpackRPC(RPC):
    CONTENT_TYPE = "application/msgpack"

    def serialize(self, data: Any) -> bytes:
        return msgpack.dumps(data)

    def deserialize(self, data: bytes) -> bytes:
        return msgpack.loads(data)


class MsgpackMaster(Master):
    CONTENT_TYPE = "application/msgpack"

    def serialize(self, data: Any) -> bytes:
        return msgpack.dumps(data)

    def deserialize(self, data: bytes) -> bytes:
        return msgpack.loads(data)
