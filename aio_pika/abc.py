import asyncio
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from enum import Enum, IntEnum, unique
from functools import singledispatch
from types import TracebackType
from typing import (
    Any, AsyncIterable, AsyncIterator, Awaitable, Callable, Dict, FrozenSet,
    Generator, Iterator, MutableMapping, NamedTuple, Optional, Set, Tuple, Type,
    TypeVar, Union,
)


try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

import aiormq.abc
from aiormq.abc import ExceptionType
from pamqp.common import Arguments
from yarl import URL

from .pool import PoolInstance
from .tools import (
    CallbackCollection, CallbackSetType, CallbackType, OneShotCallback,
)


TimeoutType = Optional[Union[int, float]]

NoneType = type(None)
DateType = Union[int, datetime, float, timedelta, None]
ExchangeParamType = Union["AbstractExchange", str]
ConsumerTag = str

MILLISECONDS = 1000
ZERO_TIME = datetime.utcfromtimestamp(0)


class SSLOptions(TypedDict, total=False):
    cafile: str
    capath: str
    cadata: str
    keyfile: str
    certfile: str
    no_verify_ssl: int


@unique
class ExchangeType(str, Enum):
    FANOUT = "fanout"
    DIRECT = "direct"
    TOPIC = "topic"
    HEADERS = "headers"
    X_DELAYED_MESSAGE = "x-delayed-message"
    X_CONSISTENT_HASH = "x-consistent-hash"
    X_MODULUS_HASH = "x-modulus-hash"


@unique
class DeliveryMode(IntEnum):
    NOT_PERSISTENT = 1
    PERSISTENT = 2


@unique
class TransactionState(str, Enum):
    CREATED = "created"
    COMMITED = "commited"
    ROLLED_BACK = "rolled back"
    STARTED = "started"


class DeclarationResult(NamedTuple):
    message_count: int
    consumer_count: int


class AbstractTransaction:
    state: TransactionState

    @property
    @abstractmethod
    def channel(self) -> "AbstractChannel":
        raise NotImplementedError

    @abstractmethod
    async def select(
        self, timeout: TimeoutType = None,
    ) -> aiormq.spec.Tx.SelectOk:
        raise NotImplementedError

    @abstractmethod
    async def rollback(
        self, timeout: TimeoutType = None,
    ) -> aiormq.spec.Tx.RollbackOk:
        raise NotImplementedError

    async def commit(
        self, timeout: TimeoutType = None,
    ) -> aiormq.spec.Tx.CommitOk:
        raise NotImplementedError

    async def __aenter__(self) -> "AbstractTransaction":
        raise NotImplementedError

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        raise NotImplementedError


HeadersValue = Union[aiormq.abc.FieldValue, bytes]
HeadersPythonValues = Union[
    HeadersValue,
    Set[HeadersValue],
    Tuple[HeadersValue, ...],
    FrozenSet[HeadersValue],
]
HeadersType = MutableMapping[str, HeadersPythonValues]


class AbstractMessage(ABC):
    body: bytes
    body_size: int
    headers_raw: aiormq.abc.FieldTable
    content_type: Optional[str]
    content_encoding: Optional[str]
    delivery_mode: DeliveryMode
    priority: Optional[int]
    correlation_id: Optional[str]
    reply_to: Optional[str]
    expiration: Optional[DateType]
    message_id: Optional[str]
    timestamp: Optional[datetime]
    type: Optional[str]
    user_id: Optional[str]
    app_id: Optional[str]

    @property
    @abstractmethod
    def headers(self) -> HeadersType:
        raise NotImplementedError

    @headers.setter
    def headers(self, value: HeadersType) -> None:
        raise NotImplementedError

    @abstractmethod
    def info(self) -> Dict[str, HeadersValue]:
        raise NotImplementedError

    @property
    @abstractmethod
    def locked(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def properties(self) -> aiormq.spec.Basic.Properties:
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> Iterator[int]:
        raise NotImplementedError

    @abstractmethod
    def lock(self) -> None:
        raise NotImplementedError

    def __copy__(self) -> "AbstractMessage":
        raise NotImplementedError


class AbstractIncomingMessage(AbstractMessage, ABC):
    cluster_id: Optional[str]
    consumer_tag: Optional["ConsumerTag"]
    delivery_tag: Optional[int]
    redelivered: Optional[bool]
    message_count: Optional[int]
    routing_key: Optional[str]
    exchange: Optional[str]

    @property
    @abstractmethod
    def channel(self) -> aiormq.abc.AbstractChannel:
        raise NotImplementedError

    @abstractmethod
    @asynccontextmanager
    async def process(
        self,
        requeue: bool = False,
        reject_on_redelivered: bool = False,
        ignore_processed: bool = False,
    ) -> AsyncIterator["AbstractMessage"]:
        yield self

    @abstractmethod
    async def ack(self, multiple: bool = False) -> None:
        raise NotImplementedError

    @abstractmethod
    async def reject(self, requeue: bool = False) -> None:
        raise NotImplementedError

    @abstractmethod
    async def nack(self, multiple: bool = False, requeue: bool = True) -> None:
        raise NotImplementedError

    def info(self) -> Dict[str, Any]:
        raise NotImplementedError

    @property
    @abstractmethod
    def processed(self) -> bool:
        raise NotImplementedError


class AbstractQueue:
    channel: aiormq.abc.AbstractChannel
    name: str
    durable: bool
    exclusive: bool
    auto_delete: bool
    arguments: Arguments
    passive: bool
    declaration_result: aiormq.spec.Queue.DeclareOk
    close_callbacks: CallbackCollection

    @abstractmethod
    def __init__(
        self,
        channel: aiormq.abc.AbstractChannel,
        name: Optional[str],
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        arguments: Arguments,
        passive: bool = False,
    ):
        raise NotImplementedError

    @abstractmethod
    async def declare(
        self, timeout: TimeoutType = None,
    ) -> aiormq.spec.Queue.DeclareOk:
        raise NotImplementedError

    @abstractmethod
    async def bind(
        self,
        exchange: ExchangeParamType,
        routing_key: Optional[str] = None,
        *,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
    ) -> aiormq.spec.Queue.BindOk:
        raise NotImplementedError

    @abstractmethod
    async def unbind(
        self,
        exchange: ExchangeParamType,
        routing_key: Optional[str] = None,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
    ) -> aiormq.spec.Queue.UnbindOk:
        raise NotImplementedError

    @abstractmethod
    async def consume(
        self,
        callback: Callable[[AbstractIncomingMessage], Any],
        no_ack: bool = False,
        exclusive: bool = False,
        arguments: Arguments = None,
        consumer_tag: Optional[ConsumerTag] = None,
        timeout: TimeoutType = None,
    ) -> ConsumerTag:
        raise NotImplementedError

    @abstractmethod
    async def cancel(
        self, consumer_tag: ConsumerTag,
        timeout: TimeoutType = None,
        nowait: bool = False,
    ) -> aiormq.spec.Basic.CancelOk:
        raise NotImplementedError

    @abstractmethod
    async def get(
        self, *, no_ack: bool = False,
        fail: bool = True, timeout: TimeoutType = 5,
    ) -> Optional[AbstractIncomingMessage]:
        raise NotImplementedError

    @abstractmethod
    async def purge(
        self, no_wait: bool = False, timeout: TimeoutType = None,
    ) -> aiormq.spec.Queue.PurgeOk:
        raise NotImplementedError

    @abstractmethod
    async def delete(
        self, *, if_unused: bool = True, if_empty: bool = True,
        timeout: TimeoutType = None,
    ) -> aiormq.spec.Queue.DeleteOk:
        raise NotImplementedError

    @abstractmethod
    def iterator(self, **kwargs: Any) -> "AbstractQueueIterator":
        raise NotImplementedError


class AbstractQueueIterator(AsyncIterable):
    _amqp_queue: AbstractQueue
    _queue: asyncio.Queue
    _consumer_tag: ConsumerTag
    _consume_kwargs: Dict[str, Any]

    @abstractmethod
    def close(self, *_: Any) -> Awaitable[Any]:
        raise NotImplementedError

    @abstractmethod
    async def on_message(self, message: AbstractIncomingMessage) -> None:
        raise NotImplementedError

    @abstractmethod
    async def consume(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def __aiter__(self) -> "AbstractQueueIterator":
        raise NotImplementedError

    @abstractmethod
    def __aenter__(self) -> Awaitable["AbstractQueueIterator"]:
        raise NotImplementedError

    @abstractmethod
    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def __anext__(self) -> AbstractIncomingMessage:
        raise NotImplementedError


class AbstractExchange(ABC):
    name: str

    @abstractmethod
    def __init__(
        self,
        channel: aiormq.abc.AbstractChannel,
        name: str,
        type: Union[ExchangeType, str] = ExchangeType.DIRECT,
        *,
        auto_delete: bool = False,
        durable: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: Arguments = None,
    ):
        raise NotImplementedError

    @abstractmethod
    async def declare(
        self, timeout: TimeoutType = None,
    ) -> aiormq.spec.Exchange.DeclareOk:
        raise NotImplementedError

    @abstractmethod
    async def bind(
        self,
        exchange: ExchangeParamType,
        routing_key: str = "",
        *,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
    ) -> aiormq.spec.Exchange.BindOk:
        raise NotImplementedError

    @abstractmethod
    async def unbind(
        self,
        exchange: ExchangeParamType,
        routing_key: str = "",
        arguments: Arguments = None,
        timeout: TimeoutType = None,
    ) -> aiormq.spec.Exchange.UnbindOk:
        raise NotImplementedError

    @abstractmethod
    async def publish(
        self,
        message: "AbstractMessage",
        routing_key: str,
        *,
        mandatory: bool = True,
        immediate: bool = False,
        timeout: TimeoutType = None,
    ) -> Optional[aiormq.abc.ConfirmationFrameType]:
        raise NotImplementedError

    @abstractmethod
    async def delete(
        self, if_unused: bool = False, timeout: TimeoutType = None,
    ) -> aiormq.spec.Exchange.DeleteOk:
        raise NotImplementedError


class UnderlayChannel(NamedTuple):
    channel: aiormq.abc.AbstractChannel
    close_callback: OneShotCallback

    @classmethod
    async def create(
        cls, connection: aiormq.abc.AbstractConnection,
        close_callback: Callable[..., Awaitable[Any]], **kwargs: Any,
    ) -> "UnderlayChannel":
        close_callback = OneShotCallback(close_callback)

        await connection.ready()
        connection.closing.add_done_callback(close_callback)
        channel = await connection.channel(**kwargs)
        channel.closing.add_done_callback(close_callback)

        return cls(
            channel=channel,
            close_callback=close_callback,
        )

    async def close(self, exc: Optional[ExceptionType] = None) -> Any:
        if self.close_callback.finished.is_set():
            return

        # close callbacks must be fired when closing
        # and should be deleted later to prevent memory leaks
        await self.channel.close(exc)
        await self.close_callback.wait()

        self.channel.closing.remove_done_callback(self.close_callback)
        self.channel.connection.closing.remove_done_callback(
            self.close_callback,
        )


class AbstractChannel(PoolInstance, ABC):
    QUEUE_CLASS: Type[AbstractQueue]
    EXCHANGE_CLASS: Type[AbstractExchange]

    close_callbacks: CallbackCollection
    return_callbacks: CallbackCollection
    default_exchange: AbstractExchange

    publisher_confirms: bool

    @property
    @abstractmethod
    def is_initialized(self) -> bool:
        return hasattr(self, "_channel")

    @property
    @abstractmethod
    def is_closed(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def close(self, exc: Optional[ExceptionType] = None) -> Awaitable[None]:
        raise NotImplementedError

    @property
    @abstractmethod
    def channel(self) -> aiormq.abc.AbstractChannel:
        raise NotImplementedError

    @property
    @abstractmethod
    def number(self) -> Optional[int]:
        raise NotImplementedError

    @abstractmethod
    async def __aenter__(self) -> "AbstractChannel":
        raise NotImplementedError

    @abstractmethod
    def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Awaitable[None]:
        raise NotImplementedError

    @abstractmethod
    async def initialize(self, timeout: TimeoutType = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def reopen(self) -> Awaitable[None]:
        raise NotImplementedError

    @abstractmethod
    async def declare_exchange(
        self,
        name: str,
        type: Union[ExchangeType, str] = ExchangeType.DIRECT,
        *,
        durable: bool = False,
        auto_delete: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
    ) -> AbstractExchange:
        raise NotImplementedError

    @abstractmethod
    async def get_exchange(
        self, name: str, *, ensure: bool = True,
    ) -> AbstractExchange:
        raise NotImplementedError

    @abstractmethod
    async def declare_queue(
        self,
        name: Optional[str] = None,
        *,
        durable: bool = False,
        exclusive: bool = False,
        passive: bool = False,
        auto_delete: bool = False,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
    ) -> AbstractQueue:
        raise NotImplementedError

    @abstractmethod
    async def get_queue(
        self, name: str, *, ensure: bool = True,
    ) -> AbstractQueue:
        raise NotImplementedError

    @abstractmethod
    async def set_qos(
        self,
        prefetch_count: int = 0,
        prefetch_size: int = 0,
        global_: bool = False,
        timeout: TimeoutType = None,
        all_channels: Optional[bool] = None,
    ) -> aiormq.spec.Basic.QosOk:
        raise NotImplementedError

    @abstractmethod
    async def queue_delete(
        self,
        queue_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        if_empty: bool = False,
        nowait: bool = False,
    ) -> aiormq.spec.Queue.DeleteOk:
        raise NotImplementedError

    @abstractmethod
    async def exchange_delete(
        self,
        exchange_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        nowait: bool = False,
    ) -> aiormq.spec.Exchange.DeleteOk:
        raise NotImplementedError

    @abstractmethod
    def transaction(self) -> AbstractTransaction:
        raise NotImplementedError

    @abstractmethod
    async def flow(self, active: bool = True) -> aiormq.spec.Channel.FlowOk:
        raise NotImplementedError

    @abstractmethod
    def __await__(self) -> Generator[Any, Any, "AbstractChannel"]:
        raise NotImplementedError


class UnderlayConnection(NamedTuple):
    connection: aiormq.abc.AbstractConnection
    close_callback: OneShotCallback

    @classmethod
    async def make_connection(
        cls, url: URL, timeout: TimeoutType = None, **kwargs: Any,
    ) -> aiormq.abc.AbstractConnection:
        connection: aiormq.abc.AbstractConnection = await asyncio.wait_for(
            aiormq.connect(url, **kwargs), timeout=timeout,
        )
        await connection.ready()
        return connection

    @classmethod
    async def connect(
        cls, url: URL, close_callback: Callable[..., Awaitable[Any]],
        timeout: TimeoutType = None, **kwargs: Any,
    ) -> "UnderlayConnection":
        try:
            connection = await cls.make_connection(
                url, timeout=timeout, **kwargs,
            )
            close_callback = OneShotCallback(close_callback)
            connection.closing.add_done_callback(close_callback)
        except Exception as e:
            closing = asyncio.get_event_loop().create_future()
            closing.set_exception(e)
            await close_callback(closing)
            raise

        await connection.ready()
        return cls(
            connection=connection,
            close_callback=close_callback,
        )

    def ready(self) -> Awaitable[Any]:
        return self.connection.ready()

    async def close(self, exc: Optional[aiormq.abc.ExceptionType]) -> Any:
        if self.close_callback.finished.is_set():
            return
        try:
            return await self.connection.close(exc)
        except asyncio.CancelledError:
            raise
        finally:
            await self.close_callback.wait()


class AbstractConnection(PoolInstance, ABC):
    close_callbacks: CallbackCollection
    connected: asyncio.Event
    transport: Optional[UnderlayConnection]

    @abstractmethod
    def __init__(
        self, url: URL, loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs: Any,
    ):
        raise NotImplementedError(
            f"Method not implemented, passed: url={url}, loop={loop!r}",
        )

    @property
    @abstractmethod
    def is_closed(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def close(self, exc: ExceptionType = asyncio.CancelledError) -> None:
        raise NotImplementedError

    @abstractmethod
    async def connect(self, timeout: TimeoutType = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def channel(
        self,
        channel_number: Optional[int] = None,
        publisher_confirms: bool = True,
        on_return_raises: bool = False,
    ) -> AbstractChannel:
        raise NotImplementedError

    @abstractmethod
    async def ready(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def __aenter__(self) -> "AbstractConnection":
        raise NotImplementedError

    @abstractmethod
    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def update_secret(
        self, new_secret: str, *,
        reason: str = "", timeout: TimeoutType = None,
    ) -> aiormq.spec.Connection.UpdateSecretOk:
        raise NotImplementedError


class AbstractRobustQueue(AbstractQueue):
    @abstractmethod
    def restore(self, channel: aiormq.abc.AbstractChannel) -> Awaitable[None]:
        raise NotImplementedError

    @abstractmethod
    async def bind(
        self,
        exchange: ExchangeParamType,
        routing_key: Optional[str] = None,
        *,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
        robust: bool = True,
    ) -> aiormq.spec.Queue.BindOk:
        raise NotImplementedError

    @abstractmethod
    async def consume(
        self,
        callback: Callable[[AbstractIncomingMessage], Any],
        no_ack: bool = False,
        exclusive: bool = False,
        arguments: Arguments = None,
        consumer_tag: Optional[ConsumerTag] = None,
        timeout: TimeoutType = None,
        robust: bool = True,
    ) -> ConsumerTag:
        raise NotImplementedError


class AbstractRobustExchange(AbstractExchange):
    @abstractmethod
    def restore(self, channel: aiormq.abc.AbstractChannel) -> Awaitable[None]:
        raise NotImplementedError

    @abstractmethod
    async def bind(
        self,
        exchange: ExchangeParamType,
        routing_key: str = "",
        *,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
        robust: bool = True,
    ) -> aiormq.spec.Exchange.BindOk:
        raise NotImplementedError


class AbstractRobustChannel(AbstractChannel):
    reopen_callbacks: CallbackCollection

    @abstractmethod
    def reopen(self) -> Awaitable[None]:
        raise NotImplementedError

    @abstractmethod
    async def restore(self, connection: aiormq.abc.AbstractConnection) -> None:
        raise NotImplementedError

    @abstractmethod
    async def declare_exchange(
        self,
        name: str,
        type: Union[ExchangeType, str] = ExchangeType.DIRECT,
        *,
        durable: bool = False,
        auto_delete: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
        robust: bool = True,
    ) -> AbstractRobustExchange:
        raise NotImplementedError

    @abstractmethod
    async def declare_queue(
        self,
        name: Optional[str] = None,
        *,
        durable: bool = False,
        exclusive: bool = False,
        passive: bool = False,
        auto_delete: bool = False,
        arguments: Optional[Dict[str, Any]] = None,
        timeout: TimeoutType = None,
        robust: bool = True,
    ) -> AbstractRobustQueue:
        raise NotImplementedError


class AbstractRobustConnection(AbstractConnection):
    reconnect_callbacks: CallbackCollection

    @property
    @abstractmethod
    def reconnecting(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def reconnect(self) -> Awaitable[None]:
        raise NotImplementedError

    @abstractmethod
    def channel(
        self,
        channel_number: Optional[int] = None,
        publisher_confirms: bool = True,
        on_return_raises: bool = False,
    ) -> AbstractRobustChannel:
        raise NotImplementedError


ChannelCloseCallback = Callable[
    [AbstractChannel, Optional[BaseException]], Any,
]
ConnectionCloseCallback = Callable[
    [AbstractConnection, Optional[BaseException]], Any,
]
ConnectionType = TypeVar("ConnectionType", bound=AbstractConnection)


@singledispatch
def get_exchange_name(value: Any) -> str:
    raise ValueError(
        "exchange argument must be an exchange "
        f"instance or str not {value!r}",
    )


@get_exchange_name.register(AbstractExchange)
def _get_exchange_name_from_exchnage(value: AbstractExchange) -> str:
    return value.name


@get_exchange_name.register(str)
def _get_exchange_name_from_str(value: str) -> str:
    return value


__all__ = (
    "AbstractChannel",
    "AbstractConnection",
    "AbstractExchange",
    "AbstractIncomingMessage",
    "AbstractMessage",
    "AbstractQueue",
    "AbstractQueueIterator",
    "AbstractRobustChannel",
    "AbstractRobustConnection",
    "AbstractRobustExchange",
    "AbstractRobustQueue",
    "AbstractTransaction",
    "CallbackSetType",
    "CallbackType",
    "ChannelCloseCallback",
    "ConnectionCloseCallback",
    "ConsumerTag",
    "DateType",
    "DeclarationResult",
    "DeliveryMode",
    "ExchangeParamType",
    "ExchangeType",
    "HeadersPythonValues",
    "HeadersType",
    "HeadersValue",
    "MILLISECONDS",
    "SSLOptions",
    "TimeoutType",
    "TransactionState",
    "UnderlayChannel",
    "UnderlayConnection",
    "ZERO_TIME",
    "get_exchange_name",
)
