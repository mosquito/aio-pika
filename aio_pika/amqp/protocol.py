from enum import unique, IntEnum
from io import BytesIO

from .codec import (
    encode_long_string,
    decode_long_string,
    decode_short_string,
    decode_table,
    encode_short_string,
    encode_table,
    unpack_from, pack_into,
)
from .base import AsyncMethod, SyncMethod, Class, Properties

__doc__ = (
    "Base classes that are extended by low "
    "level AMQP frames and higher level AMQP classes and methods."
)


MappingProxyType = type(object.__dict__)
PROTOCOL_VERSION = (0, 9, 1)
PORT = 5672


@unique
class StatusCode(IntEnum):
    ACCESS_REFUSED = 403
    CHANNEL_ERROR = 504
    COMMAND_INVALID = 503
    CONNECTION_FORCED = 320
    CONTENT_TOO_LARGE = 311
    FRAME_BODY = 3
    FRAME_END = 206
    FRAME_END_SIZE = 1
    FRAME_ERROR = 501
    FRAME_HEADER = 2
    FRAME_HEADER_SIZE = 7
    FRAME_HEARTBEAT = 8
    FRAME_MAX_SIZE = 131072
    FRAME_METHOD = 1
    FRAME_MIN_SIZE = 4096
    INTERNAL_ERROR = 541
    INVALID_PATH = 402
    NOT_ALLOWED = 530
    NOT_FOUND = 404
    NOT_IMPLEMENTED = 540
    NO_CONSUMERS = 313
    NO_ROUTE = 312
    PRECONDITION_FAILED = 406
    REPLY_SUCCESS = 200
    RESOURCE_ERROR = 506
    RESOURCE_LOCKED = 405
    SYNTAX_ERROR = 502
    UNEXPECTED_FRAME = 505


class Connection(Class):
    INDEX = 0x000A  # 10
    NAME = 'Connection'

    class Start(SyncMethod):
        INDEX = 0x000A000A  # 10, 10; 655370
        NAME = 'Connection.Start'

        __slots__ = ('version_major', 'version_minor',
                     'server_properties', 'mechanisms', 'locales')

        def __init__(self,
                     version_major: int=PROTOCOL_VERSION[0],
                     version_minor: int=PROTOCOL_VERSION[1],
                     server_properties: dict=None,
                     mechanisms: str='PLAIN',
                     locales: str='en_US'):

            self.version_major = version_major
            self.version_minor = version_minor
            self.server_properties = server_properties or {}
            self.mechanisms = mechanisms
            self.locales = locales

        def decode(self, buffer: BytesIO):
            self.version_major, self.version_minor = unpack_from('BB', buffer)
            self.server_properties = decode_table(buffer)
            self.mechanisms = decode_long_string(buffer)
            self.locales = decode_long_string(buffer)
            return self

        def encode(self, buffer: BytesIO=None):
            buffer = buffer or BytesIO()
            pack_into('BB', buffer, self.version_major, self.version_minor)
            encode_table(buffer, self.server_properties)
            encode_long_string(buffer, self.mechanisms)
            encode_long_string(buffer, self.locales)
            return buffer

    class StartOk(AsyncMethod):
        INDEX = 0x000A000B  # 10, 11; 655371
        NAME = 'Connection.StartOk'

        __slots__ = 'client_properties', 'mechanism', 'response', 'locale',

        def __init__(self, client_properties: dict = None,
                     mechanism: str = 'PLAIN',
                     response: str = None,
                     locale: str = 'en_US'):
            self.client_properties = client_properties or {}
            self.mechanism = mechanism
            self.response = response
            self.locale = locale

        def decode(self, buffer: BytesIO):
            self.client_properties = decode_table(buffer)
            self.mechanism = decode_short_string(buffer)
            self.response = decode_long_string(buffer, encoding=None)
            self.locale = decode_short_string(buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()
            encode_table(buffer, self.client_properties)
            encode_short_string(buffer, self.mechanism)
            encode_long_string(buffer, self.response)
            encode_short_string(buffer, self.locale)
            return buffer

    class Secure(SyncMethod):
        INDEX = 0x000A0014  # 10, 20; 655380
        NAME = 'Connection.Secure'

        __slots__ = 'challenge',

        def __init__(self, challenge=None):
            self.challenge = challenge

        def decode(self, buffer: BytesIO):
            self.challenge = decode_long_string(buffer, encoding=None)
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            encode_long_string(self.challenge)
            return buffer

    class SecureOk(AsyncMethod):
        INDEX = 0x000A0015  # 10, 21; 655381
        NAME = 'Connection.SecureOk'

        __slots__ = 'response',

        def __init__(self, response=None):
            self.response = response

        def decode(self, buffer: BytesIO):
            self.response = decode_long_string(buffer, encoding=None)
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            encode_long_string(buffer, self.response)
            return buffer

    class TuneBase:
        __slots__ = 'channel_max', 'frame_max', 'heartbeat',

        def __init__(self, channel_max=0, frame_max=0, heartbeat=0):
            self.channel_max = channel_max
            self.frame_max = frame_max
            self.heartbeat = heartbeat

        def decode(self, buffer: BytesIO):
            self.channel_max, self.frame_max, self.heartbeat = unpack_from(
                '>HIH', buffer
            )
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            pack_into('>HIH', buffer,
                      self.channel_max, self.frame_max, self.heartbeat)
            return buffer

    class Tune(SyncMethod, TuneBase):
        INDEX = 0x000A001E  # 10, 30; 655390
        NAME = 'Connection.Tune'

    class TuneOk(AsyncMethod, TuneBase):
        INDEX = 0x000A001F  # 10, 31; 655391
        NAME = 'Connection.TuneOk'

    class Open(SyncMethod):
        INDEX = 0x000A0028  # 10, 40; 655400
        NAME = 'Connection.Open'

        __slots__ = 'virtual_host', 'capabilities', 'insist',

        def __init__(self, virtual_host='/', capabilities='', insist=False):
            self.virtual_host = virtual_host
            self.capabilities = capabilities
            self.insist = insist

        def decode(self, buffer: BytesIO):
            self.virtual_host = decode_short_string(buffer)
            self.capabilities = decode_short_string(buffer)
            self.insist = (unpack_from('B', buffer)[0] & (1 << 0)) != 0
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            encode_short_string(buffer, self.virtual_host)
            encode_short_string(buffer, self.capabilities)

            bit_buffer = 0
            if self.insist:
                bit_buffer |= 1 << 0

            pack_into('B', buffer, bit_buffer)
            return buffer

    class OpenOk(AsyncMethod):
        INDEX = 0x000A0029  # 10, 41; 655401
        NAME = 'Connection.OpenOk'

        __slots__ = 'known_hosts',

        def __init__(self, known_hosts=''):
            self.known_hosts = known_hosts

        def decode(self, buffer: BytesIO):
            self.known_hosts = decode_short_string(buffer)
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            encode_short_string(buffer, self.known_hosts)
            return buffer

    class Close(SyncMethod):
        INDEX = 0x000A0032  # 10, 50; 655410
        NAME = 'Connection.Close'

        __slots__ = 'reply_code', 'reply_text', 'class_id', 'method_id',

        def __init__(self, reply_code=None, reply_text='',
                     class_id=None, method_id=None):
            self.reply_code = reply_code
            self.reply_text = reply_text
            self.class_id = class_id
            self.method_id = method_id

        def decode(self, buffer: BytesIO):
            self.reply_code, = unpack_from('>H', buffer)
            self.reply_text = decode_short_string(buffer)
            self.class_id, self.method_id = unpack_from('>HH', buffer)
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            pack_into('>H', buffer, self.reply_code)
            encode_short_string(buffer, self.reply_text)
            pack_into('>HH', buffer, self.class_id, self.method_id)
            return buffer

    class CloseOk(AsyncMethod):
        INDEX = 0x000A0033  # 10, 51; 655411
        NAME = 'Connection.CloseOk'

    class Blocked(AsyncMethod):
        INDEX = 0x000A003C  # 10, 60; 655420
        NAME = 'Connection.Blocked'

        __slots__ = 'reason',

        def __init__(self, reason=''):
            self.reason = reason

        def decode(self, buffer: BytesIO):
            self.reason = decode_short_string(buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()
            encode_short_string(buffer, self.reason)
            return buffer

    class Unblocked(AsyncMethod):
        INDEX = 0x000A003D  # 10, 61; 655421
        NAME = 'Connection.Unblocked'


class Channel(Class):
    INDEX = 0x0014  # 20
    NAME = 'Channel'

    class Open(SyncMethod):
        INDEX = 0x0014000A  # 20, 10; 1310730
        NAME = 'Channel.Open'

        __slots__ = 'out_of_band',
        __arguments__ = __slots__

        def __init__(self, out_of_band=''):
            self.out_of_band = out_of_band

        def decode(self, buffer: BytesIO):
            self.out_of_band = decode_short_string(buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()
            encode_short_string(buffer, self.out_of_band)
            return buffer

    class OpenOk(AsyncMethod):
        INDEX = 0x0014000B  # 20, 11; 1310731
        NAME = 'Channel.OpenOk'

        __slots__ = 'channel_id',

        def __init__(self, channel_id=''):
            self.channel_id = channel_id

        def decode(self, buffer: BytesIO):
            self.channel_id = decode_long_string(buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()
            encode_long_string(self.channel_id)
            return buffer

    class Flow(SyncMethod):
        INDEX = 0x00140014  # 20, 20; 1310740
        NAME = 'Channel.Flow'

        __slots__ = 'active',

        def __init__(self, active=None):
            self.active = active

        def decode(self, buffer: BytesIO):
            bit_buffer, = unpack_from('B', buffer)
            self.active = (bit_buffer & (1 << 0)) != 0
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()

            bit_buffer = 0
            if self.active:
                bit_buffer |= 1 << 0

            pack_into('B', buffer, bit_buffer)
            return buffer

    class FlowOk(AsyncMethod):

        INDEX = 0x00140015  # 20, 21; 1310741
        NAME = 'Channel.FlowOk'

        __slots__ = 'active',

        def __init__(self, active=None):
            self.active = active

        def decode(self, buffer: BytesIO):
            bit_buffer, = unpack_from('B', buffer)
            self.active = (bit_buffer & (1 << 0)) != 0
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()
            bit_buffer = 0
            if self.active:
                bit_buffer |= 1 << 0
            pack_into('B', buffer, bit_buffer)
            return buffer

    class Close(SyncMethod):
        INDEX = 0x00140028  # 20, 40; 1310760
        NAME = 'Channel.Close'

        __slots__ = 'reply_code', 'reply_text', 'class_id', 'method_id'

        def __init__(self, reply_code=None, reply_text='',
                     class_id=None, method_id=None):

            self.reply_code = reply_code
            self.reply_text = reply_text
            self.class_id = class_id
            self.method_id = method_id

        def decode(self, buffer: BytesIO):
            self.reply_code, = unpack_from('>H', buffer)
            self.reply_text = decode_short_string(buffer)
            self.class_id, self.method_id = unpack_from('>HH', buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()
            pack_into('>H', buffer, self.reply_code)
            encode_short_string(buffer, self.reply_text)
            pack_into('>HH', buffer, self.class_id, self.method_id)
            return buffer

    class CloseOk(AsyncMethod):
        INDEX = 0x00140029  # 20, 41; 1310761
        NAME = 'Channel.CloseOk'


class Access(Class):
    INDEX = 0x001E  # 30
    NAME = 'Access'

    class Request(SyncMethod):
        INDEX = 0x001E000A  # 30, 10; 1966090
        NAME = 'Access.Request'

        __slots__ = 'realm', 'exclusive', 'passive', 'active', 'write', 'read',

        def __init__(self, realm='/data', exclusive=False, passive=True,
                     active=True, write=True, read=True):
            self.realm = realm
            self.exclusive = exclusive
            self.passive = passive
            self.active = active
            self.write = write
            self.read = read

        def decode(self, buffer: BytesIO):
            self.realm = decode_short_string(buffer)
            bit_buffer, = unpack_from('>B', buffer)
            self.exclusive = (bit_buffer & (1 << 0)) != 0
            self.passive = (bit_buffer & (1 << 1)) != 0
            self.active = (bit_buffer & (1 << 2)) != 0
            self.write = (bit_buffer & (1 << 3)) != 0
            self.read = (bit_buffer & (1 << 4)) != 0
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()

            encode_short_string(buffer, self.realm)

            bit_buffer = 0

            if self.exclusive:
                bit_buffer |= 1 << 0

            if self.passive:
                bit_buffer |= 1 << 1

            if self.active:
                bit_buffer |= 1 << 2

            if self.write:
                bit_buffer |= 1 << 3

            if self.read:
                bit_buffer |= 1 << 4

            pack_into('B', buffer, bit_buffer)
            return buffer

    class RequestOk(AsyncMethod):
        INDEX = 0x001E000B  # 30, 11; 1966091
        NAME = 'Access.RequestOk'

        __slots__ = 'ticket',

        def __init__(self, ticket=1):
            self.ticket = ticket

        def decode(self, buffer: BytesIO):
            self.ticket, = unpack_from('>H', buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()
            pack_into('>H', buffer, self.ticket)
            return buffer


class Exchange(Class):
    INDEX = 0x0028  # 40
    NAME = 'Exchange'

    class Declare(SyncMethod):

        INDEX = 0x0028000A  # 40, 10; 2621450
        NAME = 'Exchange.Declare'

        __slots__ = (
            'ticket', 'exchange', 'type', 'passive', 'durable', 'auto_delete',
            'internal', 'nowait', 'arguments',
        )

        def __init__(self, ticket=0, exchange=None, type='direct',
                     passive=False, durable=False, auto_delete=False,
                     internal=False, nowait=False, arguments=None):
            self.ticket = ticket
            self.exchange = exchange
            self.type = type
            self.passive = passive
            self.durable = durable
            self.auto_delete = auto_delete
            self.internal = internal
            self.nowait = nowait
            self.arguments = arguments or {}

        def decode(self, buffer: BytesIO):
            self.ticket, = unpack_from('>H', buffer)
            self.exchange = decode_short_string(buffer)
            self.type = decode_short_string(buffer)
            bit_buffer, = unpack_from('B', buffer)
            self.passive = (bit_buffer & (1 << 0)) != 0
            self.durable = (bit_buffer & (1 << 1)) != 0
            self.auto_delete = (bit_buffer & (1 << 2)) != 0
            self.internal = (bit_buffer & (1 << 3)) != 0
            self.nowait = (bit_buffer & (1 << 4)) != 0
            self.arguments = decode_table(buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()

            pack_into('>H', buffer, self.ticket)
            encode_short_string(buffer, self.exchange)
            encode_short_string(buffer, self.type)

            bit_buffer = 0
            if self.passive:
                bit_buffer |= 1 << 0
            if self.durable:
                bit_buffer |= 1 << 1
            if self.auto_delete:
                bit_buffer |= 1 << 2
            if self.internal:
                bit_buffer |= 1 << 3
            if self.nowait:
                bit_buffer |= 1 << 4

            pack_into('B', buffer, bit_buffer)
            encode_table(buffer, self.arguments)
            return buffer

    class DeclareOk(AsyncMethod):
        INDEX = 0x0028000B  # 40, 11; 2621451
        NAME = 'Exchange.DeclareOk'

    class Delete(SyncMethod):
        INDEX = 0x00280014  # 40, 20; 2621460
        NAME = 'Exchange.Delete'

        __slots__ = 'ticket', 'exchange', 'if_unused', 'nowait',

        def __init__(self, ticket=0, exchange=None,
                     if_unused=False, nowait=False):
            self.ticket = ticket
            self.exchange = exchange
            self.if_unused = if_unused
            self.nowait = nowait

        def decode(self, buffer: BytesIO):
            self.ticket, = unpack_from('>H', buffer)
            self.exchange = decode_short_string(buffer)
            bit_buffer, = unpack_from('B', buffer)
            self.if_unused = (bit_buffer & (1 << 0)) != 0
            self.nowait = (bit_buffer & (1 << 1)) != 0
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()

            pack_into('>H', buffer, self.ticket)
            encode_short_string(buffer, self.exchange)

            bit_buffer = 0
            if self.if_unused:
                bit_buffer |= 1 << 0
            if self.nowait:
                bit_buffer |= 1 << 1

            pack_into('B', buffer, bit_buffer)
            return buffer

    class DeleteOk(AsyncMethod):
        INDEX = 0x00280015  # 40, 21; 2621461
        NAME = 'Exchange.DeleteOk'

    class Bind(SyncMethod):
        INDEX = 0x0028001E  # 40, 30; 2621470
        NAME = 'Exchange.Bind'

        __slots__ = (
            'ticket', 'destination', 'source', 'routing_key',
            'nowait', 'arguments'
        )

        def __init__(self, ticket=0, destination=None, source=None,
                     routing_key='', nowait=False, arguments=None):
            self.ticket = ticket
            self.destination = destination
            self.source = source
            self.routing_key = routing_key
            self.nowait = nowait
            self.arguments = arguments or {}

        def decode(self, buffer: BytesIO):
            self.ticket, = unpack_from('>H', buffer)
            self.destination = decode_short_string(buffer)
            self.source = decode_short_string(buffer)
            self.routing_key = decode_short_string(buffer)
            self.nowait = (unpack_from('B', buffer)[0] & (1 << 0)) != 0
            self.arguments = decode_table(buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()

            pack_into('>H', buffer, self.ticket)
            encode_short_string(buffer, self.destination)
            encode_short_string(buffer, self.source)
            encode_short_string(buffer, self.routing_key)

            bit_buffer = 0
            if self.nowait:
                bit_buffer |= 1 << 0

            pack_into('B', buffer, bit_buffer)
            encode_table(buffer, self.arguments)
            return buffer

    class BindOk(AsyncMethod):
        INDEX = 0x0028001F  # 40, 31; 2621471
        NAME = 'Exchange.BindOk'

    class Unbind(SyncMethod):

        INDEX = 0x00280028  # 40, 40; 2621480
        NAME = 'Exchange.Unbind'

        __slots__ = (
            'ticket', 'destination', 'source', 'routing_key',
            'nowait', 'arguments'
        )

        def __init__(self, ticket=0, destination=None, source=None,
                     routing_key='', nowait=False, arguments=None):
            self.ticket = ticket
            self.destination = destination
            self.source = source
            self.routing_key = routing_key
            self.nowait = nowait
            self.arguments = arguments or {}

        def decode(self, buffer: BytesIO):
            self.ticket, = unpack_from('>H', buffer)
            self.destination = decode_short_string(buffer)
            self.source = decode_short_string(buffer)
            self.routing_key = decode_short_string(buffer)
            self.nowait = (unpack_from('B', buffer)[0] & (1 << 0)) != 0
            self.arguments = decode_table(buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()

            pack_into('>H', buffer, self.ticket)
            encode_short_string(buffer, self.destination)
            encode_short_string(buffer, self.source)
            encode_short_string(buffer, self.routing_key)

            bit_buffer = 0
            if self.nowait:
                bit_buffer |= 1 << 0

            pack_into('B', buffer, bit_buffer)
            encode_table(buffer, self.arguments)
            return buffer

    class UnbindOk(AsyncMethod):
        INDEX = 0x00280033  # 40, 51; 2621491
        NAME = 'Exchange.UnbindOk'


class Queue(Class):
    INDEX = 0x0032  # 50
    NAME = 'Queue'

    class Declare(SyncMethod):

        INDEX = 0x0032000A  # 50, 10; 3276810
        NAME = 'Queue.Declare'

        __slots__ = (
            'ticket', 'queue', 'passive', 'durable', 'exclusive',
            'auto_delete', 'nowait', 'arguments'
        )

        def __init__(self, ticket=0, queue='', passive=False, durable=False,
                     exclusive=False, auto_delete=False, nowait=False,
                     arguments=None):
            self.ticket = ticket
            self.queue = queue
            self.passive = passive
            self.durable = durable
            self.exclusive = exclusive
            self.auto_delete = auto_delete
            self.nowait = nowait
            self.arguments = arguments or {}

        def decode(self, buffer: BytesIO):
            self.ticket, = unpack_from('>H', buffer)
            self.queue = decode_short_string(buffer)
            bit_buffer, = unpack_from('B', buffer)
            self.passive = (bit_buffer & (1 << 0)) != 0
            self.durable = (bit_buffer & (1 << 1)) != 0
            self.exclusive = (bit_buffer & (1 << 2)) != 0
            self.auto_delete = (bit_buffer & (1 << 3)) != 0
            self.nowait = (bit_buffer & (1 << 4)) != 0
            self.arguments = decode_table(buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()
            pack_into('>H', buffer, self.ticket)
            encode_short_string(buffer, self.queue)

            bit_buffer = 0
            if self.passive:
                bit_buffer |= 1 << 0
            if self.durable:
                bit_buffer |= 1 << 1
            if self.exclusive:
                bit_buffer |= 1 << 2
            if self.auto_delete:
                bit_buffer |= 1 << 3
            if self.nowait:
                bit_buffer |= 1 << 4

            pack_into('B', buffer, bit_buffer)
            encode_table(buffer, self.arguments)
            return buffer

    class DeclareOk(AsyncMethod):

        INDEX = 0x0032000B  # 50, 11; 3276811
        NAME = 'Queue.DeclareOk'

        __slots__ = 'queue', 'message_count', 'consumer_count'

        def __init__(self, queue=None, message_count=None, consumer_count=None):
            self.queue = queue
            self.message_count = message_count
            self.consumer_count = consumer_count

        def decode(self, buffer: BytesIO):
            self.queue = decode_short_string(buffer)
            self.message_count, self.consumer_count = unpack_from('>II', buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()
            encode_short_string(buffer, self.queue)
            pack_into('>II', buffer, self.message_count, self.consumer_count)
            return buffer

    class Bind(SyncMethod):

        INDEX = 0x00320014  # 50, 20; 3276820
        NAME = 'Queue.Bind'

        __slots__ = (
            'ticket', 'queue', 'exchange',
            'routing_key', 'nowait', 'arguments'
        )

        def __init__(self, ticket=0, queue='', exchange=None,
                     routing_key='', nowait=False, arguments=None):
            self.ticket = ticket
            self.queue = queue
            self.exchange = exchange
            self.routing_key = routing_key
            self.nowait = nowait
            self.arguments = arguments or {}

        def decode(self, buffer: BytesIO):
            self.ticket, = unpack_from('>H', buffer)
            self.queue = decode_short_string(buffer)
            self.exchange = decode_short_string(buffer)
            self.routing_key = decode_short_string(buffer)
            self.nowait = (unpack_from('B', buffer)[0] & (1 << 0)) != 0
            self.arguments = decode_table(buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()

            pack_into('>H', buffer, self.ticket)
            encode_short_string(buffer, self.queue)
            encode_short_string(buffer, self.exchange)
            encode_short_string(buffer, self.routing_key)

            bit_buffer = 0
            if self.nowait:
                bit_buffer |= 1 << 0

            pack_into('B', buffer, bit_buffer)
            encode_table(buffer, self.arguments)
            return buffer

    class BindOk(AsyncMethod):
        INDEX = 0x00320015  # 50, 21; 3276821
        NAME = 'Queue.BindOk'

    class Purge(SyncMethod):
        INDEX = 0x0032001E  # 50, 30; 3276830
        NAME = 'Queue.Purge'

        __slots__ = 'ticket', 'queue', 'nowait'

        def __init__(self, ticket=0, queue='', nowait=False):
            self.ticket = ticket
            self.queue = queue
            self.nowait = nowait

        def decode(self, buffer: BytesIO):
            self.ticket, = unpack_from('>H', buffer)
            self.queue = decode_short_string(buffer)
            self.nowait = (unpack_from('B', buffer)[0] & (1 << 0)) != 0
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()

            pack_into('>H', buffer, self.ticket)
            encode_short_string(buffer, self.queue)

            bit_buffer = 0
            if self.nowait:
                bit_buffer |= 1 << 0

            pack_into('B', buffer, bit_buffer)
            return buffer

    class PurgeOk(AsyncMethod):

        INDEX = 0x0032001F  # 50, 31; 3276831
        NAME = 'Queue.PurgeOk'

        __slots__ = 'message_count',

        def __init__(self, message_count=None):
            self.message_count = message_count

        @property
        def synchronous(self):
            return False

        def decode(self, buffer: BytesIO):
            self.message_count, = unpack_from('>I', buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()
            pack_into('>I', buffer, self.message_count)
            return buffer

    class Delete(SyncMethod):

        INDEX = 0x00320028  # 50, 40; 3276840
        NAME = 'Queue.Delete'

        __slots__ = 'ticket', 'queue', 'if_unused', 'if_empty', 'nowait'

        def __init__(self, ticket=0, queue='', if_unused=False,
                     if_empty=False, nowait=False):
            self.ticket = ticket
            self.queue = queue
            self.if_unused = if_unused
            self.if_empty = if_empty
            self.nowait = nowait

        def decode(self, buffer: BytesIO):
            self.ticket, = unpack_from('>H', buffer)
            self.queue = decode_short_string(buffer)
            bit_buffer = unpack_from('B', buffer)[0]
            self.if_unused = (bit_buffer & (1 << 0)) != 0
            self.if_empty = (bit_buffer & (1 << 1)) != 0
            self.nowait = (bit_buffer & (1 << 2)) != 0
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()

            pack_into('>H', buffer, self.ticket)
            encode_short_string(buffer, self.queue)

            bit_buffer = 0

            if self.if_unused:
                bit_buffer |= 1 << 0

            if self.if_empty:
                bit_buffer |= 1 << 1

            if self.nowait:
                bit_buffer |= 1 << 2

            pack_into('B', buffer, bit_buffer)
            return buffer

    class DeleteOk(AsyncMethod):
        INDEX = 0x00320029  # 50, 41; 3276841
        NAME = 'Queue.DeleteOk'

        __slots__ = 'message_count',

        def __init__(self, message_count=None):
            self.message_count = message_count

        def decode(self, buffer: BytesIO):
            self.message_count, = unpack_from('>I', buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()
            pack_into('>I', buffer, self.message_count)
            return buffer

    class Unbind(SyncMethod):
        INDEX = 0x00320032  # 50, 50; 3276850
        NAME = 'Queue.Unbind'

        __slots__ = 'ticket', 'queue', 'exchange', 'routing_key', 'arguments',

        def __init__(self, ticket=0, queue='', exchange=None,
                     routing_key='', arguments=None):
            self.ticket = ticket
            self.queue = queue
            self.exchange = exchange
            self.routing_key = routing_key
            self.arguments = arguments or {}

        def decode(self, buffer: BytesIO):
            self.ticket, = unpack_from('>H', buffer)
            self.queue = decode_short_string(buffer)
            self.exchange = decode_short_string(buffer)
            self.routing_key = decode_short_string(buffer)
            self.arguments = decode_table(buffer)
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()

            pack_into('>H', buffer, self.ticket)
            encode_short_string(buffer, self.queue)
            encode_short_string(buffer, self.exchange)
            encode_short_string(buffer, self.routing_key)
            encode_table(buffer, self.arguments)
            return buffer

    class UnbindOk(AsyncMethod):
        INDEX = 0x00320033  # 50, 51; 3276851
        NAME = 'Queue.UnbindOk'


class Basic(Class):
    INDEX = 0x003C  # 60
    NAME = 'Basic'

    class Qos(SyncMethod):

        INDEX = 0x003C000A  # 60, 10; 3932170
        NAME = 'Basic.Qos'

        __slots__ = 'prefetch_size', 'prefetch_count', 'is_global'

        def __init__(self,
                     prefetch_size: int = 0,
                     prefetch_count: int = 0,
                     is_global: bool = False):
            self.prefetch_size = prefetch_size
            self.prefetch_count = prefetch_count
            self.is_global = is_global

        def decode(self, buffer: BytesIO):
            self.prefetch_size, self.prefetch_count = unpack_from(
                '>IH', buffer
            )[0]
            self.is_global = (unpack_from('B', buffer)[0] & (1 << 0)) != 0
            return self

        def encode(self, buffer=None):
            buffer = buffer or BytesIO()
            pack_into('>IH', buffer, self.prefetch_size, self.prefetch_count)

            bit_buffer = 0
            if self.is_global:
                bit_buffer |= 1 << 0

            pack_into('B', buffer, bit_buffer)
            return buffer

    class QosOk(AsyncMethod):
        INDEX = 0x003C000B  # 60, 11; 3932171
        NAME = 'Basic.QosOk'

    class Consume(SyncMethod):

        INDEX = 0x003C0014  # 60, 20; 3932180
        NAME = 'Basic.Consume'

        __slots__ = (
            'ticket', 'queue', 'consumer_tag', 'no_local',
            'no_ack', 'exclusive', 'nowait', 'arguments'
        )

        def __init__(self, ticket=0, queue='', consumer_tag='',
                     no_local=False, no_ack=False, exclusive=False,
                     nowait=False, arguments=None):
            self.ticket = ticket
            self.queue = queue
            self.consumer_tag = consumer_tag
            self.no_local = no_local
            self.no_ack = no_ack
            self.exclusive = exclusive
            self.nowait = nowait
            self.arguments = arguments or {}

        def decode(self, buffer: BytesIO):
            self.ticket = unpack_from('>H', buffer)[0]
            self.queue = decode_short_string(buffer)
            self.consumer_tag = decode_short_string(buffer)

            bit_buffer = unpack_from('B', buffer)[0]
            self.no_local = (bit_buffer & (1 << 0)) != 0
            self.no_ack = (bit_buffer & (1 << 1)) != 0
            self.exclusive = (bit_buffer & (1 << 2)) != 0
            self.nowait = (bit_buffer & (1 << 3)) != 0
            self.arguments = decode_table(buffer)
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            pack_into('>H', buffer, self.ticket)
            encode_short_string(buffer, self.queue)
            encode_short_string(buffer, self.consumer_tag)

            bit_buffer = 0
            if self.no_local:
                bit_buffer |= 1 << 0
            if self.no_ack:
                bit_buffer |= 1 << 1
            if self.exclusive:
                bit_buffer |= 1 << 2
            if self.nowait:
                bit_buffer |= 1 << 3

            pack_into('B', buffer, bit_buffer)
            encode_table(buffer, self.arguments)
            return buffer

    class ConsumeOk(AsyncMethod):

        INDEX = 0x003C0015  # 60, 21; 3932181
        NAME = 'Basic.ConsumeOk'

        __slots__ = 'consumer_tag',

        def __init__(self, consumer_tag=None):
            self.consumer_tag = consumer_tag

        def decode(self, buffer: BytesIO):
            self.consumer_tag = decode_short_string(buffer)
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            encode_short_string(buffer, self.consumer_tag)
            return buffer

    class Cancel(SyncMethod):
        INDEX = 0x003C001E  # 60, 30; 3932190
        NAME = 'Basic.Cancel'

        __slots__ = 'consumer_tag', 'nowait',

        def __init__(self, consumer_tag=None, nowait=False):
            self.consumer_tag = consumer_tag
            self.nowait = nowait

        def decode(self, buffer: BytesIO):
            self.consumer_tag = decode_short_string(buffer)
            self.nowait = (unpack_from('B', buffer)[0] & (1 << 0)) != 0
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            encode_short_string(buffer, self.consumer_tag)

            bit_buffer = 0
            if self.nowait:
                bit_buffer |= 1 << 0
            pack_into('B', buffer, bit_buffer)

            return buffer

    class CancelOk(AsyncMethod):

        INDEX = 0x003C001F  # 60, 31; 3932191
        NAME = 'Basic.CancelOk'

        __slots__ = 'consumer_tag',

        def __init__(self, consumer_tag=None):
            self.consumer_tag = consumer_tag

        def decode(self, buffer: BytesIO):
            self.consumer_tag = decode_short_string(buffer)
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            encode_short_string(buffer, self.consumer_tag)
            return buffer

    class Publish(AsyncMethod):

        INDEX = 0x003C0028  # 60, 40; 3932200
        NAME = 'Basic.Publish'

        __slots__ = (
            'ticket', 'exchange', 'routing_key', 'mandatory', 'immediate'
        )

        def __init__(self, ticket=0, exchange='', routing_key='',
                     mandatory=False, immediate=False):
            self.ticket = ticket
            self.exchange = exchange
            self.routing_key = routing_key
            self.mandatory = mandatory
            self.immediate = immediate

        def decode(self, buffer: BytesIO):
            self.ticket = unpack_from('>H', buffer)[0]
            self.exchange = decode_short_string(buffer)
            self.routing_key = decode_short_string(buffer)

            bit_buffer = unpack_from('B', buffer)[0]
            self.mandatory = (bit_buffer & (1 << 0)) != 0
            self.immediate = (bit_buffer & (1 << 1)) != 0
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            pack_into('>H', buffer, self.ticket)
            encode_short_string(buffer, self.exchange)
            encode_short_string(buffer, self.routing_key)

            bit_buffer = 0
            if self.mandatory:
                bit_buffer |= 1 << 0

            if self.immediate:
                bit_buffer |= 1 << 1

            pack_into('B', buffer, bit_buffer)
            return buffer

    class Return(AsyncMethod):

        INDEX = 0x003C0032  # 60, 50; 3932210
        NAME = 'Basic.Return'

        __slots__ = 'reply_code', 'reply_text', 'exchange', 'routing_key'

        def __init__(self, reply_code=None, reply_text='',
                     exchange=None, routing_key=None):
            self.reply_code = reply_code
            self.reply_text = reply_text
            self.exchange = exchange
            self.routing_key = routing_key

        def decode(self, buffer: BytesIO):
            self.reply_code = unpack_from('>H', buffer)[0]
            self.reply_text= decode_short_string(buffer)
            self.exchange= decode_short_string(buffer)
            self.routing_key= decode_short_string(buffer)
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            pack_into('>H', buffer, self.reply_code)
            encode_short_string(buffer, self.reply_text)
            encode_short_string(buffer, self.exchange)
            encode_short_string(buffer, self.routing_key)
            return buffer

    class Deliver(AsyncMethod):

        INDEX = 0x003C003C  # 60, 60; 3932220
        NAME = 'Basic.Deliver'

        __slots__ = 'consumer_tag', 'redelivered', 'exchnage', 'routing_key',

        def __init__(self, consumer_tag=None, delivery_tag=None,
                     redelivered=False, exchange=None, routing_key=None):
            self.consumer_tag = consumer_tag
            self.delivery_tag = delivery_tag
            self.redelivered = redelivered
            self.exchange = exchange
            self.routing_key = routing_key

        @property
        def synchronous(self):
            return False

        def decode(self, buffer: BytesIO):
            self.consumer_tag = decode_short_string(buffer)
            self.delivery_tag, bit_buffer = unpack_from('>QB', buffer)
            self.redelivered = (bit_buffer & (1 << 0)) != 0
            self.exchange= decode_short_string(buffer)
            self.routing_key= decode_short_string(buffer)
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            encode_short_string(buffer, self.consumer_tag)
            pack_into('>Q', buffer, self.delivery_tag)

            bit_buffer = 0

            if self.redelivered:
                bit_buffer |= 1 << 0

            pack_into('B', buffer, bit_buffer)

            encode_short_string(buffer, self.exchange)
            encode_short_string(buffer, self.routing_key)
            return buffer

    class Get(SyncMethod):
        INDEX = 0x003C0046  # 60, 70; 3932230
        NAME = 'Basic.Get'

        __slots__ = 'ticket', 'queue', 'no_ack',

        def __init__(self, ticket=0, queue='', no_ack=False):
            self.ticket = ticket
            self.queue = queue
            self.no_ack = no_ack

        def decode(self, buffer: BytesIO):
            self.ticket = unpack_from('>H', buffer)[0]
            self.queue= decode_short_string(buffer)
            self.no_ack = (unpack_from('B', buffer)[0] & (1 << 0)) != 0
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            pack_into('>H', buffer, self.ticket)
            encode_short_string(buffer, self.queue)

            bit_buffer = 0

            if self.no_ack:
                bit_buffer |= 1 << 0

            pack_into('B', buffer, bit_buffer)
            return buffer

    class GetOk(AsyncMethod):

        INDEX = 0x003C0047  # 60, 71; 3932231
        NAME = 'Basic.GetOk'

        __slots__ = (
            'delivery_tag', 'redelivered', 'exchange',
            'routing_key', 'message_count',
        )

        def __init__(self, delivery_tag=None, redelivered=False,
                     exchange=None, routing_key=None, message_count=None):
            self.delivery_tag = delivery_tag
            self.redelivered = redelivered
            self.exchange = exchange
            self.routing_key = routing_key
            self.message_count = message_count

        @property
        def synchronous(self):
            return False

        def decode(self, buffer: BytesIO):
            self.delivery_tag, bit_buffer = unpack_from('>QB', buffer)
            self.redelivered = (bit_buffer & (1 << 0)) != 0
            self.exchange= decode_short_string(buffer)
            self.routing_key= decode_short_string(buffer)
            self.message_count, = unpack_from('>I', buffer)
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            pack_into('>Q', buffer, self.delivery_tag)
            bit_buffer = 0

            if self.redelivered:
                bit_buffer |= 1 << 0
            pack_into('B', buffer, bit_buffer)
            encode_short_string(buffer, self.exchange)
            encode_short_string(buffer, self.routing_key)
            pack_into('>I', buffer, self.message_count)
            return buffer

    class GetEmpty(AsyncMethod):

        INDEX = 0x003C0048  # 60, 72; 3932232
        NAME = 'Basic.GetEmpty'

        __slots__ = 'cluster_id',

        def __init__(self, cluster_id=''):
            self.cluster_id = cluster_id

        def decode(self, buffer: BytesIO):
            self.cluster_id = decode_short_string(buffer)
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            encode_short_string(buffer, self.cluster_id)
            return buffer

    class Ack(AsyncMethod):

        INDEX = 0x003C0050  # 60, 80; 3932240
        NAME = 'Basic.Ack'

        __slots__ = 'delivery_tag', 'multiple',

        def __init__(self, delivery_tag=0, multiple=False):
            self.delivery_tag = delivery_tag
            self.multiple = multiple

        def decode(self, buffer: BytesIO):
            self.delivery_tag, bit_buffer = unpack_from('>QB', buffer)
            self.multiple = (bit_buffer & (1 << 0)) != 0
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()

            bit_buffer = 0
            if self.multiple:
                bit_buffer |= 1 << 0

            pack_into('>QB', buffer, self.delivery_tag, bit_buffer)
            return buffer

    class Reject(AsyncMethod):

        INDEX = 0x003C005A  # 60, 90; 3932250
        NAME = 'Basic.Reject'

        __slots__ = 'delivery_tag', 'requeue',

        def __init__(self, delivery_tag=None, requeue=True):
            self.delivery_tag = delivery_tag
            self.requeue = requeue

        def decode(self, buffer: BytesIO):
            self.delivery_tag, bit_buffer = unpack_from('>QB', buffer)
            self.requeue = (bit_buffer & (1 << 0)) != 0
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            bit_buffer = 0
            if self.requeue:
                bit_buffer |= 1 << 0

            pack_into('>QB', buffer, self.delivery_tag, bit_buffer)
            return buffer

    class RecoverAsync(AsyncMethod):

        INDEX = 0x003C0064  # 60, 100; 3932260
        NAME = 'Basic.RecoverAsync'

        __slots__ = 'requeue',

        def __init__(self, requeue=False):
            self.requeue = requeue

        def decode(self, buffer: BytesIO):
            self.requeue = (unpack_from('B', buffer)[0] & (1 << 0)) != 0
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()

            bit_buffer = 0
            if self.requeue:
                bit_buffer |= 1 << 0

            pack_into('B', buffer, bit_buffer)
            return buffer

    class Recover(SyncMethod):

        INDEX = 0x003C006E  # 60, 110; 3932270
        NAME = 'Basic.Recover'

        __slots__ = 'requeue',

        def __init__(self, requeue=False):
            self.requeue = requeue

        def decode(self, buffer: BytesIO):
            self.requeue = (unpack_from('B', buffer)[0] & (1 << 0)) != 0
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()

            bit_buffer = 0
            if self.requeue:
                bit_buffer |= 1 << 0

            pack_into('B', buffer, bit_buffer)
            return buffer

    class RecoverOk(AsyncMethod):

        INDEX = 0x003C006F  # 60, 111; 3932271
        NAME = 'Basic.RecoverOk'

    class Nack(AsyncMethod):

        INDEX = 0x003C0078  # 60, 120; 3932280
        NAME = 'Basic.Nack'

        __slots__ = 'delivery_tag', 'multiple', 'requeue',

        def __init__(self, delivery_tag=0, multiple=False, requeue=True):
            self.delivery_tag = delivery_tag
            self.multiple = multiple
            self.requeue = requeue

        def decode(self, buffer: BytesIO):
            self.delivery_tag, bit_buffer = unpack_from('>QB', buffer)
            self.multiple = (bit_buffer & (1 << 0)) != 0
            self.requeue = (bit_buffer & (1 << 1)) != 0
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()

            bit_buffer = 0
            if self.multiple:
                bit_buffer |= 1 << 0
            if self.requeue:
                bit_buffer |= 1 << 1

            pack_into('>QB', buffer, self.delivery_tag, bit_buffer)
            return buffer


class Tx(Class):
    INDEX = 0x005A  # 90
    NAME = 'Tx'

    class Select(SyncMethod):
        INDEX = 0x005A000A  # 90, 10; 5898250
        NAME = 'Tx.Select'

    class SelectOk(AsyncMethod):
        INDEX = 0x005A000B  # 90, 11; 5898251
        NAME = 'Tx.SelectOk'

    class Commit(SyncMethod):
        INDEX = 0x005A0014  # 90, 20; 5898260
        NAME = 'Tx.Commit'

    class CommitOk(AsyncMethod):
        INDEX = 0x005A0015  # 90, 21; 5898261
        NAME = 'Tx.CommitOk'

    class Rollback(SyncMethod):
        INDEX = 0x005A001E  # 90, 30; 5898270
        NAME = 'Tx.Rollback'

    class RollbackOk(AsyncMethod):
        INDEX = 0x005A001F  # 90, 31; 5898271
        NAME = 'Tx.RollbackOk'


class Confirm(Class):
    INDEX = 0x0055  # 85
    NAME = 'Confirm'

    class Select(SyncMethod):
        INDEX = 0x0055000A  # 85, 10; 5570570
        NAME = 'Confirm.Select'

        __slots__ = 'nowait',

        def __init__(self, nowait=False):
            self.nowait = nowait

        def decode(self, buffer: BytesIO):
            self.nowait = (unpack_from('B', buffer)[0] & (1 << 0)) != 0
            return self

        def encode(self, buffer: BytesIO = None):
            buffer = buffer or BytesIO()
            bit_buffer = 0

            if self.nowait:
                bit_buffer |= 1 << 0

            pack_into('B', buffer, bit_buffer)
            return buffer

    class SelectOk(AsyncMethod):
        INDEX = 0x0055000B  # 85, 11; 5570571
        NAME = 'Confirm.SelectOk'


class BasicProperties(Properties):
    CLASS = Basic
    INDEX = 0x003C  # 60
    NAME = 'BasicProperties'

    FLAG_CONTENT_TYPE = (1 << 15)
    FLAG_CONTENT_ENCODING = (1 << 14)
    FLAG_HEADERS = (1 << 13)
    FLAG_DELIVERY_MODE = (1 << 12)
    FLAG_PRIORITY = (1 << 11)
    FLAG_CORRELATION_ID = (1 << 10)
    FLAG_REPLY_TO = (1 << 9)
    FLAG_EXPIRATION = (1 << 8)
    FLAG_MESSAGE_ID = (1 << 7)
    FLAG_TIMESTAMP = (1 << 6)
    FLAG_TYPE = (1 << 5)
    FLAG_USER_ID = (1 << 4)
    FLAG_APP_ID = (1 << 3)
    FLAG_CLUSTER_ID = (1 << 2)

    __slots__ = (
        "content_type", "content_encoding", "headers",
        "delivery_mode", "priority", "correlation_id",
        "reply_to", "expiration", "message_id", "timestamp",
        "type", "user_id", "app_id", "cluster_id"
    )

    def __init__(self, content_type=None, content_encoding=None,
                 headers=None, delivery_mode=None, priority=None,
                 correlation_id=None, reply_to=None, expiration=None,
                 message_id=None, timestamp=None, type=None, user_id=None,
                 app_id=None, cluster_id=None):

        self.content_type = content_type
        self.content_encoding = content_encoding
        self.headers = headers
        self.delivery_mode = delivery_mode
        self.priority = priority
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        self.expiration = expiration
        self.message_id = message_id
        self.timestamp = timestamp
        self.type = type
        self.user_id = user_id
        self.app_id = app_id
        self.cluster_id = cluster_id

    def decode(self, buffer: BytesIO):
        flags = 0
        flag_word_index = 0

        while True:
            partial_flags = unpack_from('>H', buffer)[0]
            flags |= partial_flags << (flag_word_index * 16)

            if not (partial_flags & 1):
                break

            flag_word_index += 1

        if flags & BasicProperties.FLAG_CONTENT_TYPE:
            self.content_type = decode_short_string(buffer)
        else:
            self.content_type = None

        if flags & BasicProperties.FLAG_CONTENT_ENCODING:
            self.content_encoding = decode_short_string(buffer)
        else:
            self.content_encoding = None

        if flags & BasicProperties.FLAG_HEADERS:
            (self.headers, offset) = decode_table(buffer)
        else:
            self.headers = None

        if flags & BasicProperties.FLAG_DELIVERY_MODE:
            self.delivery_mode = unpack_from('B', buffer)[0]
        else:
            self.delivery_mode = None

        if flags & BasicProperties.FLAG_PRIORITY:
            self.priority = unpack_from('B', buffer)[0]
        else:
            self.priority = None

        if flags & BasicProperties.FLAG_CORRELATION_ID:
            self.correlation_id= decode_short_string(buffer)
        else:
            self.correlation_id = None

        if flags & BasicProperties.FLAG_REPLY_TO:
            self.reply_to = decode_short_string(buffer)
        else:
            self.reply_to = None

        if flags & BasicProperties.FLAG_EXPIRATION:
            self.expiration = decode_short_string(buffer)
        else:
            self.expiration = None

        if flags & BasicProperties.FLAG_MESSAGE_ID:
            self.message_id = decode_short_string(buffer)
        else:
            self.message_id = None

        if flags & BasicProperties.FLAG_TIMESTAMP:
            self.timestamp = unpack_from('>Q', buffer)[0]
        else:
            self.timestamp = None

        if flags & BasicProperties.FLAG_TYPE:
            self.type = decode_short_string(buffer)
        else:
            self.type = None

        if flags & BasicProperties.FLAG_USER_ID:
            self.user_id = decode_short_string(buffer)
        else:
            self.user_id = None

        if flags & BasicProperties.FLAG_APP_ID:
            self.app_id = decode_short_string(buffer)
        else:
            self.app_id = None

        if flags & BasicProperties.FLAG_CLUSTER_ID:
            self.cluster_id = decode_short_string(buffer)
        else:
            self.cluster_id = None

        return self

    def encode(self, buffer: BytesIO = None):
        buffer = buffer or BytesIO()
        flag_buffer = BytesIO()
        value_buffer = BytesIO()

        flags = 0

        if self.content_type is not None:
            flags |= BasicProperties.FLAG_CONTENT_TYPE
            encode_short_string(value_buffer, self.content_type)

        if self.content_encoding is not None:
            flags |= BasicProperties.FLAG_CONTENT_ENCODING
            encode_short_string(value_buffer, self.content_encoding)

        if self.headers is not None:
            flags |= BasicProperties.FLAG_HEADERS
            encode_table(value_buffer, self.headers)

        if self.delivery_mode is not None:
            flags |= BasicProperties.FLAG_DELIVERY_MODE
            pack_into('B', value_buffer, self.delivery_mode)

        if self.priority is not None:
            flags |= BasicProperties.FLAG_PRIORITY
            pack_into('B', value_buffer, self.priority)

        if self.correlation_id is not None:
            flags |= BasicProperties.FLAG_CORRELATION_ID
            encode_short_string(value_buffer, self.correlation_id)

        if self.reply_to is not None:
            flags |= BasicProperties.FLAG_REPLY_TO
            encode_short_string(value_buffer, self.reply_to)

        if self.expiration is not None:
            flags |= BasicProperties.FLAG_EXPIRATION
            encode_short_string(value_buffer, self.expiration)

        if self.message_id is not None:
            flags |= BasicProperties.FLAG_MESSAGE_ID
            encode_short_string(value_buffer, self.message_id)

        if self.timestamp is not None:
            flags |= BasicProperties.FLAG_TIMESTAMP
            pack_into('>Q', value_buffer, self.timestamp)

        if self.type is not None:
            flags |= BasicProperties.FLAG_TYPE
            encode_short_string(value_buffer, self.type)

        if self.user_id is not None:
            flags |= BasicProperties.FLAG_USER_ID
            encode_short_string(value_buffer, self.user_id)

        if self.app_id is not None:
            flags |= BasicProperties.FLAG_APP_ID
            encode_short_string(value_buffer, self.app_id)

        if self.cluster_id is not None:
            flags |= BasicProperties.FLAG_CLUSTER_ID
            encode_short_string(value_buffer, self.cluster_id)

        while True:
            remainder = flags >> 16
            partial_flags = flags & 0xFFFE

            if remainder != 0:
                partial_flags |= 1

            pack_into('>H', flag_buffer, partial_flags)
            flags = remainder

            if not flags:
                break

        buffer.write(flag_buffer.getvalue())
        buffer.write(value_buffer.getvalue())
        return buffer


methods = MappingProxyType({
    Connection.Start.INDEX: Connection.Start,
    Connection.StartOk.INDEX: Connection.StartOk,
    Connection.Secure.INDEX: Connection.Secure,
    Connection.SecureOk.INDEX: Connection.SecureOk,
    Connection.Tune.INDEX: Connection.Tune,
    Connection.TuneOk.INDEX: Connection.TuneOk,
    Connection.Open.INDEX: Connection.Open,
    Connection.OpenOk.INDEX: Connection.OpenOk,
    Connection.Close.INDEX: Connection.Close,
    Connection.CloseOk.INDEX: Connection.CloseOk,
    Connection.Blocked.INDEX: Connection.Blocked,
    Connection.Unblocked.INDEX: Connection.Unblocked,
    Channel.Open.INDEX: Channel.Open,
    Channel.OpenOk.INDEX: Channel.OpenOk,
    Channel.Flow.INDEX: Channel.Flow,
    Channel.FlowOk.INDEX: Channel.FlowOk,
    Channel.Close.INDEX: Channel.Close,
    Channel.CloseOk.INDEX: Channel.CloseOk,
    Access.Request.INDEX: Access.Request,
    Access.RequestOk.INDEX: Access.RequestOk,
    Exchange.Declare.INDEX: Exchange.Declare,
    Exchange.DeclareOk.INDEX: Exchange.DeclareOk,
    Exchange.Delete.INDEX: Exchange.Delete,
    Exchange.DeleteOk.INDEX: Exchange.DeleteOk,
    Exchange.Bind.INDEX: Exchange.Bind,
    Exchange.BindOk.INDEX: Exchange.BindOk,
    Exchange.Unbind.INDEX: Exchange.Unbind,
    Exchange.UnbindOk.INDEX: Exchange.UnbindOk,
    Queue.Declare.INDEX: Queue.Declare,
    Queue.DeclareOk.INDEX: Queue.DeclareOk,
    Queue.Bind.INDEX: Queue.Bind,
    Queue.BindOk.INDEX: Queue.BindOk,
    Queue.Purge.INDEX: Queue.Purge,
    Queue.PurgeOk.INDEX: Queue.PurgeOk,
    Queue.Delete.INDEX: Queue.Delete,
    Queue.DeleteOk.INDEX: Queue.DeleteOk,
    Queue.Unbind.INDEX: Queue.Unbind,
    Queue.UnbindOk.INDEX: Queue.UnbindOk,
    Basic.Qos.INDEX: Basic.Qos,
    Basic.QosOk.INDEX: Basic.QosOk,
    Basic.Consume.INDEX: Basic.Consume,
    Basic.ConsumeOk.INDEX: Basic.ConsumeOk,
    Basic.Cancel.INDEX: Basic.Cancel,
    Basic.CancelOk.INDEX: Basic.CancelOk,
    Basic.Publish.INDEX: Basic.Publish,
    Basic.Return.INDEX: Basic.Return,
    Basic.Deliver.INDEX: Basic.Deliver,
    Basic.Get.INDEX: Basic.Get,
    Basic.GetOk.INDEX: Basic.GetOk,
    Basic.GetEmpty.INDEX: Basic.GetEmpty,
    Basic.Ack.INDEX: Basic.Ack,
    Basic.Reject.INDEX: Basic.Reject,
    Basic.RecoverAsync.INDEX: Basic.RecoverAsync,
    Basic.Recover.INDEX: Basic.Recover,
    Basic.RecoverOk.INDEX: Basic.RecoverOk,
    Basic.Nack.INDEX: Basic.Nack,
    Tx.Select.INDEX: Tx.Select,
    Tx.SelectOk.INDEX: Tx.SelectOk,
    Tx.Commit.INDEX: Tx.Commit,
    Tx.CommitOk.INDEX: Tx.CommitOk,
    Tx.Rollback.INDEX: Tx.Rollback,
    Tx.RollbackOk.INDEX: Tx.RollbackOk,
    Confirm.Select.INDEX: Confirm.Select,
    Confirm.SelectOk.INDEX: Confirm.SelectOk
})

props = MappingProxyType({
    BasicProperties.INDEX: BasicProperties
})

_methods_with_content = frozenset({
    Basic.Publish.INDEX,
    Basic.Return.INDEX,
    Basic.Deliver.INDEX,
    Basic.GetOk.INDEX,
})


def has_content(method_number):
    return method_number in _methods_with_content
