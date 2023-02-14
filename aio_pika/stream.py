from typing import Any, Callable, Optional, Union

from pamqp.common import Arguments

from .abc import (
    AbstractIncomingMessage, AbstractStream, ConsumerTag, TimeoutType,
    AbstractStreamIterator,
)
from .queue import Queue, QueueIterator


class StreamIterator(QueueIterator, AbstractStreamIterator):
    pass


class Stream(Queue, AbstractStream):
    async def consume(
        self,
        callback: Callable[[AbstractIncomingMessage], Any],
        no_ack: bool = False,
        exclusive: bool = False,
        arguments: Arguments = None,
        consumer_tag: Optional[ConsumerTag] = None,
        timeout: TimeoutType = None,
        *,
        offset: Optional[Union[int, str]] = None,
    ) -> ConsumerTag:
        arguments = arguments or {}
        arguments["x-stream-offset"] = offset

        return await super(Queue, self).consume(
            callback=callback,
            no_ack=no_ack,
            exclusive=exclusive,
            arguments=arguments,
            consumer_tag=consumer_tag,
            timeout=timeout,
        )

    def iterator(self, **kwargs: Any) -> AbstractStreamIterator:
        return StreamIterator(self, **kwargs)
