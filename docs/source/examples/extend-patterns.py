import gzip
import json

from typing import Any
from aio_pika.patterns import RPC, Master


class JsonMaster(Master):
    # deserializer will use SERIALIZER.loads(body)
    SERIALIZER = json
    CONTENT_TYPE = 'application/json'

    def serialize(self, data: Any) -> bytes:
        return self.SERIALIZER.dumps(data, ensure_ascii=False)


class JsonRPC(RPC):
    SERIALIZER = json
    CONTENT_TYPE = 'application/json'

    def serialize(self, data: Any) -> bytes:
        return self.SERIALIZER.dumps(
            data, ensure_ascii=False, default=repr
        )

    def serialize_exception(self, exception: Exception) -> bytes:
        return self.serialize({
            "error": {
                "type": exception.__class__.__name__,
                "message": repr(exception),
                "args": exception.args,
            }
        })


class JsonGZipRPC(JsonRPC):
    CONTENT_TYPE = 'application/octet-stream'

    def serialize(self, data: Any) -> bytes:
        return gzip.compress(super().serialize(data))

    def deserialize(self, data: Any) -> bytes:
        return super().deserialize(gzip.decompress(data))
