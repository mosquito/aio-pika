import abc
import asyncio
import logging
import os
import platform
import ssl
import typing
from binascii import hexlify
from contextlib import suppress
from enum import Enum
from functools import wraps, partial
from io import BytesIO

import pamqp.frame
from pamqp import ProtocolHeader, ContentHeader
from pamqp import specification as spec
from pamqp.body import ContentBody
from pamqp.heartbeat import Heartbeat
from yarl import URL

from .exceptions import InvalidFrameError, ChannelClosed
from .version import __version__

T = typing.TypeVar('T')
log = logging.getLogger(__name__)

PRODUCT = 'aio-pika'
PLATFORM = '{} {} ({} build {})'.format(
    platform.python_implementation(),
    platform.python_version(),
    *platform.python_build()
)

SSLCerts = typing.NamedTuple(
    'SSLCerts', [
        ('cert', str),
        ('key', str),
        ('ca', str),
    ]
)

FrameReceived = typing.NamedTuple(
    'FrameReceived', [
        ('channel', int),
        ('frame', str),
    ]
)

DeliveredMessage = typing.NamedTuple(
    'DeliveredMessage', [
        ('delivery', spec.Basic.Deliver),
        ('header', ContentHeader),
        ('body', bytes),
    ]
)


def create_default_ssl_context(certs: SSLCerts,
                               auth_type=ssl.Purpose.SERVER_AUTH):
    return ssl.create_default_context(
        auth_type,
        cafile=certs.ca,
    )


def create_client_ssl_context(certs: SSLCerts):
    ssl_context = create_default_ssl_context(certs, ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(certs.cert, certs.key)
    return ssl_context


class Auth:
    @abc.abstractmethod
    def marshal(self) -> bytes:
        pass


class PlainAuth(Auth):
    def __init__(self, connector: "AMQPConnector"):
        self.value = (
            b"\x00" + (connector.url.user or 'guest').encode() +
            b"\x00" + (connector.url.password or 'guest').encode()
        )

    def marshal(self) -> bytes:
        return self.value


class AuthMechanism(Enum):
    PLAIN = PlainAuth


def task(func: T) -> T:
    @wraps(func)
    async def wrap(self: "AMQPConnector", *args, **kwargs):
        # noinspection PyCallingNonCallable
        return await self.create_task(func(self, *args, **kwargs))

    return wrap


ChannelRType = typing.Tuple[int, spec.Channel.OpenOk]
CallbackCoro = typing.Coroutine[DeliveredMessage, None, typing.Any]
ConsumerCallback = typing.Callable[[], CallbackCoro]
ReturnCallback = typing.Callable[[], CallbackCoro]
ArgumentsType = typing.Dict[str, typing.Union[str, int, bool]]


class Base:
    def __init__(self, loop):
        self.loop = loop
        self.tasks = set()

    def _cancel_tasks(self):
        tasks = []

        # noinspection PyShadowingNames
        async def awaiter(task):
            return await task

        # noinspection PyShadowingNames
        for task in self.tasks:
            if task.done():
                continue

            task.cancel()
            tasks.append(awaiter(task))

        return self.loop.create_task(awaiter(asyncio.gather(
            *tasks, return_exceptions=True, loop=self.loop
        )))

    def create_task(self, coro) -> asyncio.Task:
        # noinspection PyShadowingNames
        task = self.loop.create_task(coro)  # type: asyncio.Task
        self.tasks.add(task)
        task.add_done_callback(self.tasks.remove)
        return task


class AMQPConnector(Base):
    HEADER_LENGTH = 8
    FRAME_BUFFER = 10
    CHANNEL_MAX = 2047
    FRAME_MAX = 131072
    HEARTBEAT = 60

    def __init__(self, url: URL, *, loop: asyncio.get_event_loop() = None):
        super().__init__(loop or asyncio.get_event_loop())

        self.loop = loop or asyncio.get_event_loop()
        self.url = url
        self.vhost = self.url.path.strip("/") or "/"
        self.reader = None  # type: asyncio.StreamReader
        self.writer = None  # type: asyncio.StreamWriter
        self.ssl_certs = SSLCerts(
            ca=self.url.query.get('keyfile'),
            key=self.url.query.get('keyfile'),
            cert=self.url.query.get('certfile')
        )

        self.started = False
        self.lock = asyncio.Lock(loop=self.loop)
        self.channels = {}  # type: typing.Dict[int, Channel]
        self.channel_lock = asyncio.Lock(loop=self.loop)

        self.reader_task = None  # type: asyncio.Task
        self.server_properties = None

        self.channel_max = None  # type: int
        self.frame_max = None  # type: int
        self.heartbeat = None  # type: int

        self.last_channel = 0
        self.tasks = set()
        self.consumers = dict()
        self.readers = dict()

    def _get_ssl_context(self):
        if self.ssl_certs.key:
            return self.loop.run_in_executor(
                None, create_client_ssl_context, self.ssl_certs
            )

        return self.loop.run_in_executor(
            None, create_default_ssl_context, self.ssl_certs,
        )

    @staticmethod
    def _client_capabilities():
        return {
            'platform': PLATFORM,
            'version': __version__,
            'product': PRODUCT,
            'capabilities': {
                'authentication_failure_close': True,
                'basic.nack': True,
                'connection.blocked': False,
                'consumer_cancel_notify': True,
                'publisher_confirms': True
            },
            'information': 'See https://aio-pika.readthedocs.io/',
        }

    def _credentials(self, start_frame: spec.Connection.Start):
        for mechanism in start_frame.mechanisms.decode().split():
            with suppress(KeyError):
                return AuthMechanism[mechanism]
        raise NotImplementedError('Not found supported auth mechanisms')

    @task
    async def connect(self):
        ssl_context = None

        if self.url.scheme == 'amqps':
            ssl_context = await self._get_ssl_context()

        self.reader, self.writer = await asyncio.open_connection(
            host=self.url.host,
            port=self.url.port,
            loop=self.loop,
            ssl=ssl_context,
        )

        protocol_header = ProtocolHeader()
        self.writer.write(protocol_header.marshal())

        _, _, frame = await self._receive_frame()  # type: spec.Connection.Start
        credentials = self._credentials(frame)

        self.server_properties = frame.server_properties
        self.writer.write(
            pamqp.frame.marshal(
                spec.Connection.StartOk(
                    client_properties=self._client_capabilities(),
                    mechanism=credentials.name,
                    response=credentials.value(self).marshal()
                ), 0
            )
        )

        _, _, frame = await self._receive_frame()  # type: spec.Connection.Tune

        self.heartbeat = frame.heartbeat
        self.frame_max = frame.frame_max
        self.channel_max = frame.channel_max

        self.writer.write(
            pamqp.frame.marshal(
                spec.Connection.TuneOk(
                    channel_max=self.channel_max,
                    frame_max=self.frame_max,
                    heartbeat=self.heartbeat
                ),
                0
            )
        )

        frame = spec.Connection.Open(virtual_host=self.vhost)
        self.writer.write(pamqp.frame.marshal(frame, 0))
        _, _, result = await self._receive_frame()
        if result.name not in frame.valid_responses:
            raise spec.AMQPInternalError(frame, frame)

        # noinspection PyAsyncCall
        self.create_task(self._reader())

        return True

    async def _receive_frame(self) -> typing.Tuple[int, int, spec.Frame]:
        async with self.lock:
            frame_header = await self.reader.readexactly(1)

            if frame_header == b'\0x00':
                raise spec.AMQPFrameError(await self.reader.read())

            frame_header += await self.reader.readexactly(6)

            if not self.started and frame_header.startswith(b'AMQP'):
                raise spec.AMQPSyntaxError
            else:
                self.started = True

            # noinspection PyProtectedMember
            frame_type, _, frame_length = pamqp.frame._frame_parts(
                frame_header
            )

            frame_payload = await self.reader.readexactly(
                frame_length + 1
            )

            return pamqp.frame.unmarshal(frame_header + frame_payload)

    async def _reader(self):
        while self.writer:
            weight, channel, parsed_frame = await self._receive_frame()

            if channel == 0:
                if isinstance(parsed_frame, Heartbeat):
                    self.writer.write(pamqp.frame.marshal(Heartbeat(), channel))
                    continue

                log.error('Unexpected frame %r', parsed_frame)
                continue

            queue = self.channels[channel].frames
            await queue.put((65535 // weight, parsed_frame))

    def close(self):
        if not self.writer:
            return

        writer = self.writer
        self.reader = None
        self.writer = None

        # noinspection PyShadowingNames
        writer.close()

        results = [self._cancel_tasks()]
        for channel in tuple(self.channels.values()):  # type: Channel
            results.append(channel._cancel_tasks())

        return asyncio.gather(*results, return_exceptions=True)

    @property
    def server_capabilities(self) -> ArgumentsType:
        return self.server_properties['server_capabilities']

    @property
    def basic_nack(self) -> bool:
        return self.server_capabilities.get('basic.nack', False)

    @property
    def consumer_cancel_notify(self) -> bool:
        return self.server_capabilities.get('consumer_cancel_notify', False)

    @property
    def exchange_exchange_bindings(self) -> bool:
        return self.server_capabilities.get('exchange_exchange_bindings', False)

    @property
    def publisher_confirms(self):
        return self.server_capabilities.get('publisher_confirms', False)

    async def channel(self, channel_number: int = None) -> "Channel":
        async with self.channel_lock:
            if channel_number is None:
                self.last_channel += 1
                channel_number = self.last_channel
            else:
                self.last_channel = channel_number

            channel = Channel(
                self, channel_number, frame_buffer=self.FRAME_BUFFER
            )

            self.channels[channel_number] = channel

            try:
                await channel.open()
            except Exception:
                self.channels.pop(channel_number, None)
                raise

            channel.closing.add_done_callback(
                partial(lambda *_: self.channels.pop(channel_number, None))
            )

            return channel


class Channel(Base):
    def __init__(self, connector: AMQPConnector, number, frame_buffer=None):
        super().__init__(connector.loop)

        self.lock = asyncio.Lock(loop=connector.loop)
        self.frames = asyncio.PriorityQueue(
            maxsize=frame_buffer, loop=self.loop
        )
        self.rpc_frames = asyncio.Queue(maxsize=frame_buffer, loop=self.loop)
        self.writer = connector.writer
        self.consumers = {}
        self.number = number
        self.return_callbacks = set()
        self.return_callbacks_lock = asyncio.Lock(loop=self.loop)
        self.closing = self.create_task(self._reader())

    async def _get_frame(self) -> spec.Frame:
        weight, frame = await self.frames.get()
        return frame

    def handle_exception(self, frame):
        if isinstance(frame, spec.Channel.Close):
            self._cancel_tasks()
            raise ChannelClosed(frame.reply_code, frame.reply_text)

        raise InvalidFrameError(frame)

    @task
    async def rpc(self, frame: spec.Frame) -> typing.Optional[spec.Frame]:
        async with self.lock:
            self.writer.write(
                pamqp.frame.marshal(frame, self.number)
            )

            if frame.synchronous or getattr(frame, 'nowait', False):
                result = await self.rpc_frames.get()

                if result.name not in frame.valid_responses:
                    self.handle_exception(result)

                return result

    async def open(self):
        frame = await self.rpc(spec.Channel.Open())

        if frame is None:
            raise spec.AMQPFrameError(frame)

    async def _read_content(self, frame):
        content_header = await self._get_frame()    # type: ContentHeader

        body = BytesIO()
        content = await self._get_frame()       # type: ContentBody
        while (isinstance(content, ContentBody) and
               body.tell() < content_header.body_size):
            body.write(content.value)

            if body.tell() < content_header.body_size:
                content = await self._get_frame()

        # noinspection PyTypeChecker
        return DeliveredMessage(
            delivery=frame,
            header=content_header,
            body=body.getvalue()
        )

    async def _reader(self):
        while True:
            frame = await self._get_frame()

            if isinstance(frame, spec.Basic.Deliver):
                message = await self._read_content(frame)

                consumer = self.consumers.get(frame.consumer_tag)
                if consumer is not None:
                    with suppress(Exception):
                        self.create_task(consumer(message))

                continue
            elif isinstance(frame, spec.Basic.Return):
                message = await self._read_content(frame)
                async with self.return_callbacks_lock:
                    for func in self.return_callbacks:
                        with suppress(Exception):
                            self.create_task(func(message))
                continue
            elif isinstance(frame, spec.Basic.Cancel):
                self.consumers.pop(frame.consumer_tag, None)
                continue

            await self.rpc_frames.put(frame)

    async def add_return_callback(self, callback: ReturnCallback):
        async with self.return_callbacks_lock:
            self.return_callbacks.add(callback)

    async def remove_return_callback(self, callback: ReturnCallback):
        async with self.return_callbacks_lock:
            self.return_callbacks.remove(callback)

    async def close(self) -> spec.Channel.CloseOk:
        result = await self.rpc(spec.Channel.Close(
            reply_code=spec.REPLY_SUCCESS,
        )
        )  # type: spec.Channel.CloseOk

        await self._cancel_tasks()

        return result

    def basic_ack(self, delivery_tag, multiple=False):
        return self.rpc(spec.Basic.Ack(
            delivery_tag=delivery_tag,
            multiple=multiple,
        ))

    def basic_get(self, queue: str = '', no_ack: bool = False):
        return self.rpc(spec.Basic.Get(
            queue=queue, no_ack=no_ack,
        ))

    async def basic_cancel(self, consumer_tag, *,
                           nowait=False) -> spec.Basic.CancelOk:
        # noinspection PyTypeChecker
        return await self.rpc(spec.Basic.Cancel(
            consumer_tag=consumer_tag,
            nowait=nowait,
        ))

    async def basic_consume(self, queue: str,
                            consumer_callback: ConsumerCallback, *,

                            no_ack: bool = False, exclusive: bool = False,
                            arguments: ArgumentsType = None,
                            consumer_tag: str = None) -> spec.Basic.ConsumeOk:

        consumer_tag = consumer_tag or 'ctag%i.%s' % (
            self.number, hexlify(os.urandom(16)).decode()
        )

        self.consumers[consumer_tag] = consumer_callback

        # noinspection PyTypeChecker
        return await self.rpc(spec.Basic.Consume(
            queue=queue, no_ack=no_ack,
            exclusive=exclusive, consumer_tag=consumer_tag,
            arguments=arguments
        ))

    def basic_nack(self, delivery_tag: str = None,
                   multiple: str = False, requeue: str = True):
        # noinspection PyTypeChecker
        return self.rpc(spec.Basic.Nack(
            delivery_tag=delivery_tag, multiple=multiple, requeue=requeue
        ))

    async def basic_publish(
        self, body: bytes, *, exchange: str = '', routing_key: str = '',
        properties: spec.Basic.Properties = None,
        mandatory: bool = False, immediate: bool = False
    ):

        frame = spec.Basic.Publish(
            exchange=exchange, routing_key=routing_key,
            mandatory=mandatory, immediate=immediate
        )

        async with self.lock:
            self.writer.write(pamqp.frame.marshal(frame, self.number))

            content = ContentBody(body)
            content_header = ContentHeader(
                properties=properties or spec.Basic.Properties(delivery_mode=1),
                body_size=len(content)
            )

            # noinspection PyTypeChecker
            self.writer.write(
                pamqp.frame.marshal(content_header, self.number)
            )

            # noinspection PyTypeChecker
            self.writer.write(
                pamqp.frame.marshal(content, self.number)
            )

    async def basic_qos(self, *, prefetch_size: int = None,
                        prefetch_count: int = None) -> spec.Basic.QosOk:
        # noinspection PyTypeChecker
        return await self.rpc(spec.Basic.Qos(
            prefetch_count=prefetch_count or 0,
            prefetch_size=prefetch_size or 0,
        ))

    async def basic_reject(self, delivery_tag, *,
                           requeue=True) -> spec.Basic.Reject:
        # noinspection PyTypeChecker
        return await self.rpc(spec.Basic.Reject(
            delivery_tag=delivery_tag,
            requeue=requeue,
        ))

    async def basic_recover(self, *, nowait: bool = False,
                            requeue=False) -> spec.Basic.RecoverOk:

        if nowait:
            frame = spec.Basic.RecoverAsync(requeue=requeue)
        else:
            frame = spec.Basic.Recover(requeue=requeue)
        # noinspection PyTypeChecker
        return self.rpc(frame)

    async def exchange_declare(self, *, exchange: str = None,
                               exchange_type='direct', passive=False,
                               durable=False, auto_delete=False,
                               internal=False, nowait=False,
                               arguments=None) -> spec.Exchange.DeclareOk:
        # noinspection PyTypeChecker
        return self.rpc(spec.Exchange.Declare(
            exchange=exchange, exchange_type=exchange_type,
            passive=passive, durable=durable, auto_delete=auto_delete,
            internal=internal, nowait=nowait, arguments=arguments
        ))

    def exchange_delete(self, exchange: str = None, *,
                        if_unused: bool = False,
                        nowait: bool = False) -> spec.Exchange.DeleteOk:
        # noinspection PyTypeChecker
        return self.rpc(spec.Exchange.Delete(
            exchange=exchange, nowait=nowait, if_unused=if_unused,
        ))

    async def exchange_unbind(self, destination: str = None,
                              source: str = None,
                              routing_key: str = '', *,
                              nowait: bool = False,
                              arguments: dict = None) -> spec.Exchange.UnbindOk:
        # noinspection PyTypeChecker
        return await self.rpc(spec.Exchange.Unbind(
            destination=destination, source=source, routing_key=routing_key,
            nowait=nowait, arguments=arguments
        ))

    async def flow(self, active: bool) -> spec.Channel.FlowOk:
        # noinspection PyTypeChecker
        return await self.rpc(spec.Channel.Flow(active=active))

    async def queue_bind(self, queue: str,
                         exchange: str,
                         routing_key: str = '',
                         nowait: bool = False,
                         arguments: dict = None) -> spec.Queue.BindOk:
        # noinspection PyTypeChecker
        return await self.rpc(spec.Queue.Bind(
            queue=queue, exchange=exchange, routing_key=routing_key,
            nowait=nowait, arguments=arguments
        ))

    async def queue_declare(self, queue: str = '', *,
                            passive: bool = False,
                            durable: bool = False,
                            exclusive: bool = False,
                            auto_delete: bool = False,
                            nowait: bool = False,
                            arguments: dict = None) -> spec.Queue.DeclareOk:
        # noinspection PyTypeChecker
        return await self.rpc(spec.Queue.Declare(
            queue=queue, passive=passive, durable=durable, exclusive=exclusive,
            auto_delete=auto_delete, nowait=nowait, arguments=arguments,
        ))

    async def queue_delete(self, queue: str = '',
                           if_unused: bool = False, if_empty: bool = False,
                           nowait=False) -> spec.Queue.DeleteOk:
        # noinspection PyTypeChecker
        return await self.rpc(spec.Queue.Delete(
            queue=queue, if_unused=if_unused, if_empty=if_empty,
            nowait=nowait
        ))

    async def queue_purge(self, queue: str = '',
                          nowait: bool = False) -> spec.Queue.PurgeOk:
        # noinspection PyTypeChecker
        return await self.rpc(spec.Queue.Purge(
            queue=queue, nowait=nowait
        ))

    async def queue_unbind(self, queue: str = '',
                           exchange: str = None, routing_key: str = None,
                           arguments: dict = None) -> spec.Queue.UnbindOk:
        # noinspection PyTypeChecker
        return await self.rpc(spec.Queue.Unbind(
            routing_key=routing_key, arguments=arguments,
            queue=queue, exchange=exchange,
        ))

    async def tx_commit(self) -> spec.Tx.CommitOk:
        # noinspection PyTypeChecker
        return await self.rpc(spec.Tx.Commit())

    async def tx_rollback(self) -> spec.Tx.RollbackOk:
        # noinspection PyTypeChecker
        return await self.rpc(spec.Tx.Rollback())

    async def tx_select(self) -> spec.Tx.SelectOk:
        # noinspection PyTypeChecker
        return await self.rpc(spec.Tx.Select())

    async def confirm_delivery(self, nowait=False):
        return await self.rpc(spec.Confirm.Select(nowait=nowait))
