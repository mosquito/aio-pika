import abc
import asyncio
import logging
import platform
import ssl
import uuid
from collections import defaultdict
from contextlib import suppress
from enum import Enum
from functools import partial, wraps
from io import BytesIO

import pamqp.frame
import typing
from pamqp import ProtocolHeader, ContentHeader
from pamqp import specification as spec
from pamqp.body import ContentBody

from yarl import URL

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


class AMQPConnectorBase:
    HEADER_LENGTH = 8
    FRAME_BUFFER = 10
    CHANNEL_MAX = 2047
    FRAME_MAX = 131072
    HEARTBEAT = 60

    def __init__(self, url: URL, *, loop: asyncio.get_event_loop() = None):

        self.loop = loop or asyncio.get_event_loop()
        self.url = url
        self.vhost = self.url.path.strip("/") or "/"
        self.reader = None      # type: asyncio.StreamReader
        self.writer = None      # type: asyncio.StreamWriter
        self.ssl_certs = SSLCerts(
            ca=self.url.query.get('keyfile'),
            key=self.url.query.get('keyfile'),
            cert=self.url.query.get('certfile')
        )

        self.frames = defaultdict(
            partial(
                asyncio.PriorityQueue,
                maxsize=self.FRAME_BUFFER,
                loop=self.loop
            )
        )

        self.locks = defaultdict(
            partial(asyncio.Lock, loop=self.loop)
        )

        self.reader_task = None     # type: asyncio.Task
        self.server_properties = None

        self.channel_max = None     # type: int
        self.frame_max = None       # type: int
        self.heartbeat = None       # type: int
        self.last_channel = 0
        self.tasks = set()
        self.consumers = dict()
        self.readers = dict()
        self.return_callbacks = set()
        self.return_callbacks_lock = asyncio.Lock(loop=self.loop)

    async def add_return_callback(self, callback: ReturnCallback):
        async with self.return_callbacks_lock:
            self.return_callbacks.add(callback)

    async def remove_return_callback(self, callback: ReturnCallback):
        async with self.return_callbacks_lock:
            self.return_callbacks.remove(callback)

    def create_task(self, coro) -> asyncio.Task:
        # noinspection PyShadowingNames
        task = self.loop.create_task(coro)     # type: asyncio.Task
        self.tasks.add(task)
        task.add_done_callback(self.tasks.remove)
        return task

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

        # noinspection PyAsyncCall
        self.create_task(self._reader())

        protocol_header = ProtocolHeader()
        self.writer.write(protocol_header.marshal())

        frame = await self._get_frame(0)     # type: spec.Connection.Start
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

        frame = await self._get_frame(0)     # type: spec.Connection.Tune

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

        await self.rpc(0, spec.Connection.Open(virtual_host=self.vhost))
        return True

    async def _get_frame(self, channel: int) -> spec.Frame:
        weight, received_frame = await self.frames[channel].get()
        return received_frame

    # noinspection PyAsyncCall,PyTypeChecker
    async def _reader(self):
        connection_started = False

        async def read_content(parsed_frame) -> DeliveredMessage:
            weight, channel, content_header = await receive_frame()

            body = BytesIO()
            weight, channel, content = await receive_frame()
            while (isinstance(content, ContentBody) and
                   body.tell() < content_header.body_size):
                body.write(content.value)

                if body.tell() < content_header.body_size:
                    weight, channel, content = await receive_frame()

            # noinspection PyTypeChecker
            return DeliveredMessage(
                delivery=parsed_frame,
                header=content_header,
                body=body.getvalue()
            )

        async def receive_frame() -> typing.Tuple[int, int, spec.Frame]:
            nonlocal connection_started

            frame_header = await self.reader.readexactly(1)

            if frame_header == b'\0x00':
                raise spec.AMQPFrameError(await self.reader.read())

            frame_header += await self.reader.readexactly(6)

            if not connection_started and frame_header.startswith(b'AMQP'):
                raise spec.AMQPSyntaxError
            else:
                connection_started = True

            # noinspection PyProtectedMember
            frame_type, _, frame_length = pamqp.frame._frame_parts(
                frame_header
            )

            frame_payload = await self.reader.readexactly(
                frame_length + 1
            )

            return pamqp.frame.unmarshal(frame_header + frame_payload)

        while True:
            weight, channel, parsed_frame = await receive_frame()

            if isinstance(parsed_frame, spec.Basic.Deliver):
                message = await read_content(parsed_frame)

                consumer = self.consumers.get(parsed_frame.consumer_tag)
                if consumer is not None:
                    with suppress(Exception):
                        self.create_task(consumer(message))

                continue
            elif isinstance(parsed_frame, spec.Basic.Return):
                message = await read_content(parsed_frame)
                async with self.return_callbacks_lock:
                    for func in self.return_callbacks:
                        with suppress(Exception):
                            self.create_task(func(message))
                continue

            await self.frames[channel].put((weight, parsed_frame))

    @task
    async def rpc(self, channel, frame_value: spec.Frame) -> spec.Frame:
        self.writer.write(pamqp.frame.marshal(frame_value, channel))

        if frame_value.synchronous or getattr(frame_value, 'nowait', False):
            async with self.locks[channel]:
                frame = await self._get_frame(channel)

                if frame.name not in frame_value.valid_responses:
                    raise spec.AMQPInternalError(frame_value, frame)

            return frame

    def close(self) -> asyncio.Future:
        if not self.writer:
            # noinspection PyShadowingNames
            task = asyncio.Future()
            task.set_result(None)
            return task

        writer = self.writer
        self.reader = None
        self.writer = None

        # noinspection PyShadowingNames
        async def closer():
            writer.close()

            tasks = []
            for task in self.tasks:
                if task.done():
                    continue
                task.cancel()
                tasks.append(task)

            if tasks:
                await asyncio.wait(
                    tasks, return_when=asyncio.ALL_COMPLETED, loop=self.loop
                )

        return self.loop.create_task(closer())

    @property
    def server_capabilities(self) -> ArgumentsType:
        return self.server_properties['server_capabilities']

    @property
    def server_basic_nack(self) -> bool:
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


class AMQPConnector(AMQPConnectorBase):
    async def channel_open(self, channel: int=None) -> ChannelRType:

        if channel is None:
            self.last_channel += 1
            channel = self.last_channel

        frame = await self.rpc(
            channel, spec.Channel.Open()
        )

        if frame is None:
            raise spec.AMQPFrameError(frame)

        # noinspection PyTypeChecker
        return channel, frame

    async def channel_close(self, channel) -> spec.Channel.CloseOk:
        result = await self.rpc(
            channel, spec.Channel.Close(
                reply_code=spec.REPLY_SUCCESS,
            )
        )   # type: spec.Channel.CloseOk

        self.locks.pop(channel, None)
        self.frames.pop(channel, None)

        return result

    def basic_ack(self, channel: int, delivery_tag, multiple=False):
        return self.rpc(channel, spec.Basic.Ack(
            delivery_tag=delivery_tag,
            multiple=multiple,
        ))

    def basic_get(self, channel: int, queue: str = '', no_ack: bool = False):
        return self.rpc(channel, spec.Basic.Get(
            queue=queue, no_ack=no_ack,
        ))

    async def basic_cancel(self, channel: int, consumer_tag, *,
                           nowait=False) -> spec.Basic.CancelOk:
        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Basic.Cancel(
            consumer_tag=consumer_tag,
            nowait=nowait,
        ))

    async def basic_consume(self, channel: int, queue: str,
                            consumer_callback: ConsumerCallback, *,

                            no_ack: bool = False, exclusive: bool = False,
                            arguments: ArgumentsType = None,
                            consumer_tag: str = None) -> spec.Basic.ConsumeOk:

        consumer_tag = consumer_tag or 'ctag%i.%s' % (
            channel, uuid.uuid4().hex
        )

        self.consumers[consumer_tag] = consumer_callback

        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Basic.Consume(
            queue=queue, no_ack=no_ack,
            exclusive=exclusive, consumer_tag=consumer_tag,
            arguments=arguments
        ))

    def basic_nack(self, channel: int, delivery_tag: str = None,
                   multiple: str = False, requeue: str = True):
        # noinspection PyTypeChecker
        return self.rpc(channel, spec.Basic.Nack(
            delivery_tag=delivery_tag, multiple=multiple, requeue=requeue
        ))

    async def basic_publish(
        self, channel: int, body: bytes,
        *, exchange: str='', routing_key: str='',
        properties: spec.Basic.Properties = None,
        mandatory: bool = False, immediate: bool = False
    ):

        frame = spec.Basic.Publish(
            exchange=exchange, routing_key=routing_key,
            mandatory=mandatory, immediate=immediate
        )

        async with self.locks[channel]:
            self.writer.write(pamqp.frame.marshal(frame, channel))

            content = ContentBody(body)
            content_header = ContentHeader(
                properties=properties or spec.Basic.Properties(delivery_mode=1),
                body_size=len(content)
            )

            # noinspection PyTypeChecker
            self.writer.write(pamqp.frame.marshal(content_header, channel))

            # noinspection PyTypeChecker
            self.writer.write(pamqp.frame.marshal(content, channel))

    async def basic_qos(self, channel: int, *, prefetch_size: int = None,
                        prefetch_count: int = None) -> spec.Basic.QosOk:
        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Basic.Qos(
            prefetch_count=prefetch_count or 0,
            prefetch_size=prefetch_size or 0,
        ))

    async def basic_reject(self, channel: int, delivery_tag, *,
                           requeue=True) -> spec.Basic.Reject:
        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Basic.Reject(
            delivery_tag=delivery_tag,
            requeue=requeue,
        ))

    async def basic_recover(self, channel: int, *, nowait: bool = False,
                            requeue=False) -> spec.Basic.RecoverOk:

        if nowait:
            frame = spec.Basic.RecoverAsync(requeue=requeue)
        else:
            frame = spec.Basic.Recover(requeue=requeue)
        # noinspection PyTypeChecker
        return self.rpc(channel, frame)

    async def exchange_declare(self, channel: int, *, exchange: str = None,
                               exchange_type='direct', passive=False,
                               durable=False, auto_delete=False,
                               internal=False, nowait=False,
                               arguments=None) -> spec.Exchange.DeclareOk:
        # noinspection PyTypeChecker
        return self.rpc(channel, spec.Exchange.Declare(
            exchange=exchange, exchange_type=exchange_type,
            passive=passive, durable=durable, auto_delete=auto_delete,
            internal=internal, nowait=nowait, arguments=arguments
        ))

    def exchange_delete(self, channel: int,
                        exchange: str = None, *,
                        if_unused: bool = False,
                        nowait: bool = False) -> spec.Exchange.DeleteOk:
        # noinspection PyTypeChecker
        return self.rpc(channel, spec.Exchange.Delete(
            exchange=exchange, nowait=nowait, if_unused=if_unused,
        ))

    async def exchange_unbind(self, channel: int,
                              destination: str = None,
                              source: str = None,
                              routing_key: str = '', *,
                              nowait: bool = False,
                              arguments: dict = None) -> spec.Exchange.UnbindOk:
        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Exchange.Unbind(
            destination=destination, source=source, routing_key=routing_key,
            nowait=nowait, arguments=arguments
        ))

    async def flow(self, channel: int, active: bool) -> spec.Channel.FlowOk:
        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Channel.Flow(active=active))

    async def queue_bind(self, channel: int,
                         queue: str = None,
                         exchange: str = None,
                         routing_key: str = '',
                         nowait: bool = False,
                         arguments: dict = None) -> spec.Queue.BindOk:
        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Queue.Bind(
            queue=queue, exchange=exchange, routing_key=routing_key,
            nowait=nowait, arguments=arguments
        ))

    async def queue_declare(self, channel: int, queue: str = '', *,
                            passive: bool = False,
                            durable: bool = False,
                            exclusive: bool = False,
                            auto_delete: bool = False,
                            nowait: bool = False,
                            arguments: dict = None) -> spec.Queue.DeclareOk:
        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Queue.Declare(
            queue=queue, passive=passive, durable=durable, exclusive=exclusive,
            auto_delete=auto_delete, nowait=nowait, arguments=arguments,
        ))

    async def queue_delete(self, channel: int,  queue: str = '',
                           if_unused: bool = False, if_empty: bool = False,
                           nowait=False) -> spec.Queue.DeleteOk:
        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Queue.Delete(
            queue=queue, if_unused=if_unused, if_empty=if_empty,
            nowait=nowait
        ))

    async def queue_purge(self, channel: int, queue: str = '',
                          nowait: bool = False) -> spec.Queue.PurgeOk:
        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Queue.Purge(
            queue=queue, nowait=nowait
        ))

    async def queue_unbind(self, channel: int, queue: str = '',
                           exchange: str = None, routing_key: str = None,
                           arguments: dict = None) -> spec.Queue.UnbindOk:
        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Queue.Unbind(
            routing_key=routing_key, arguments=arguments,
            queue=queue, exchange=exchange,
        ))

    async def tx_commit(self, channel: int) -> spec.Tx.CommitOk:
        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Tx.Commit())

    async def tx_rollback(self, channel: int) -> spec.Tx.RollbackOk:
        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Tx.Rollback())

    async def tx_select(self, channel: int) -> spec.Tx.SelectOk:
        # noinspection PyTypeChecker
        return await self.rpc(channel, spec.Tx.Select())
