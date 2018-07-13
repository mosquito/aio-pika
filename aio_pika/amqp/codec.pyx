from cpython.bytes cimport PyBytes_Check, PyBytes_GET_SIZE
from cpython.unicode cimport PyUnicode_AsUTF8String, PyUnicode_Check
from cpython.object cimport PyObject_IsInstance

import struct
import decimal
from io import BytesIO
from datetime import datetime


__doc__ = """AMQP Table Encoding/Decoding"""


cdef class long(int):
    """
    A marker class that signifies that the integer value should be
    serialized as `l` instead of `I`
    """

    def __repr__(self):
        return str(self) + 'L'


cpdef unpack_from(fmt: str, buffer: BytesIO):
    data = buffer.read(struct.calcsize(fmt))
    return struct.unpack(fmt, data)


def pack_into(fmt, buffer: BytesIO, *data):
    position = buffer.tell()
    buffer.write(struct.pack(fmt, *data))
    return buffer.tell() - position


cpdef as_bytes(value):
    if PyBytes_Check(value):
        return value
    elif PyUnicode_Check(value):
        return PyUnicode_AsUTF8String(value)


cpdef encode_short_string(buffer: BytesIO, value):
    cdef bytes encoded_value = as_bytes(value)
    cdef int length = PyBytes_GET_SIZE(encoded_value)
    cdef int size = length

    if length > 255:
        raise ValueError(encoded_value)

    size += pack_into('B', buffer, length)
    buffer.write(encoded_value)
    return size


cpdef encode_long_string(buffer: BytesIO, value):
    cdef bytes encoded_value = as_bytes(value)
    cdef int length = PyBytes_GET_SIZE(encoded_value)
    cdef int size = length

    size += pack_into('>I', buffer, length)
    buffer.write(encoded_value)
    return size


cpdef encode_table(buffer: BytesIO, dict table):
    table = table or {}
    length_index = buffer.tell()
    buffer.write(b'\x00\x00\x00\x00')

    tablesize = 0

    for key, value in table.items():
        tablesize += encode_short_string(buffer, key)
        tablesize += encode_value(buffer, value)

    position = buffer.tell()
    buffer.seek(length_index)
    buffer.write(struct.pack('>I', tablesize))
    buffer.seek(position)

    return buffer.tell() - length_index


cpdef int encode_bool(buffer: BytesIO, value: bool):
    return pack_into('>B', buffer, value.real)


cpdef int encode_int(buffer: BytesIO, int value):
    return pack_into('>i', buffer, value)


cpdef int encode_long(buffer: BytesIO, long value):
    return pack_into('>q', buffer, value)


cpdef int encode_decimal(buffer: BytesIO, value: decimal):
    value = value.normalize()

    if value.as_tuple().exponent < 0:
        decimals = -value.as_tuple().exponent
        raw = int(value * (decimal.Decimal(10) ** decimals))
        return pack_into('>Bi', buffer, decimals, raw)
    else:
        # per spec, the "decimals" octet is unsigned (!)
        return pack_into('>Bi', buffer, 0, int(value))


cpdef int encode_datetime(buffer, value: datetime):
    return pack_into('>Q', buffer, int(value.timestamp()))


cpdef encode_value(buffer, value):
    cdef bint is_str = PyUnicode_Check(value)
    cdef bint is_bytes = PyBytes_Check(value)

    # encode string
    if is_str or is_bytes:
        if is_str:
            value = as_bytes(value)

        size = PyBytes_GET_SIZE(value)
        size += pack_into('>c', buffer, b'S')
        size += encode_long_string(buffer, value)
        return size

    # Encode bool
    elif PyObject_IsInstance(value, bool):
        return pack_into('>c', buffer, b't') + encode_bool(buffer, value)

    # Encode dict
    elif PyObject_IsInstance(value, dict):
        return pack_into('>c', buffer, b'F') + encode_table(buffer, value)

    # Encode long
    elif PyObject_IsInstance(value, long):
        return pack_into('>c', buffer, b'l') + encode_long(buffer, value)

    # Encode int
    elif PyObject_IsInstance(value, int):
        return pack_into('>c', buffer, b'I') + encode_int(buffer, value)

    # Encode decimal
    elif PyObject_IsInstance(value, decimal.Decimal):
        return pack_into('>c', buffer, b'D') + encode_decimal(buffer, value)

    # Encode datetime
    elif PyObject_IsInstance(value, datetime):
        return pack_into('>c', buffer, b'T') + encode_datetime(buffer, value)

    # Encode Tuple or List
    elif PyObject_IsInstance(value, list) or PyObject_IsInstance(value, tuple):
        buff = BytesIO()

        for v in value:
            encode_value(buff, v)

        size = buff.tell()
        size += pack_into('>cI', buffer, b'A', size)

        buffer.write(buff.getvalue())
        return size

    # Encode None
    elif value is None:
        return pack_into('>c', b'V')

    raise ValueError(value)


cpdef decode_table(buffer):
    """Decode the AMQP table passed in from the encoded value returning the
    decoded result and the number of bytes read plus the offset.

    :param str encoded: The binary encoded data to decode
    :param int offset: The starting byte offset
    :rtype: tuple

    """
    cdef dict result = {}
    cdef int limit = buffer.tell() + unpack_from('>I', buffer)[0]

    while buffer.tell() < limit:
        key = decode_short_string(buffer)
        value = decode_value(buffer)
        result[key] = value

    return result


cpdef decode_short_string(buffer):
    length, = unpack_from('B', buffer)
    value = buffer.read(length)
    return value.decode('utf-8')


cpdef decode_long_string(buffer: BytesIO, encoding='utf-8'):
    length, = unpack_from('>I', buffer)
    data = buffer.read(length)

    if encoding:
        return data.decode(encoding)
    return data


def decode_value(buffer):
    """Decode the value passed in returning the decoded value and the number
    of bytes read in addition to the starting offset.

    :param str encoded: The binary encoded data to decode
    :param int offset: The starting byte offset
    :rtype: tuple

    """
    # slice to get bytes in Python 3 and str in Python 2
    kind = buffer.read(1)

    # Bool
    if kind == b't':
        value, = unpack_from('>B', buffer)
        return bool(value)

    # Short-Short Int
    elif kind == b'b':
        value, = unpack_from('>B', buffer)
        return value

    # Short-Short Unsigned Int
    elif kind == b'B':
        value, = unpack_from('>b', buffer)
        return value

    # Short Int
    elif kind == b'U':
        value, = unpack_from('>h', buffer)
        return value

    # Short Unsigned Int
    elif kind == b'u':
        value, = unpack_from('>H', buffer)
        return value

    # Long Int
    elif kind == b'I':
        value, = unpack_from('>i', buffer)
        return value

    # Long Unsigned Int
    elif kind == b'i':
        value, = unpack_from('>I', buffer)
        return value

    # Long-Long Int
    elif kind == b'L':
        value, = unpack_from('>q', buffer)
        return long(value)

    # Long-Long Unsigned Int
    elif kind == b'l':
        value, = unpack_from('>Q', buffer)
        return long(value)

    # Float
    elif kind == b'f':
        value, = unpack_from('>f', buffer)
        return value

    # Double
    elif kind == b'd':
        value, = unpack_from('>d', buffer)
        return value

    # Decimal
    elif kind == b'D':
        decimals, raw = unpack_from('>Bi', buffer)
        return decimal.Decimal(raw) * (decimal.Decimal(10) ** -decimals)

    # Short String
    elif kind == b's':
        return decode_short_string(buffer)

    # Long String
    elif kind == b'S':
        return decode_long_string(buffer)

    # Field Array
    elif kind == b'A':
        end = buffer.tell() + unpack_from('>I', buffer)[0]
        value = []

        while buffer.tell() < end:
            value.append(decode_value(buffer))

        return value

    # Timestamp
    elif kind == b'T':
        return datetime.utcfromtimestamp(unpack_from('>Q', buffer)[0])

    # Field Table
    elif kind == b'F':
        return decode_table(buffer)

    # Null / Void
    elif kind == b'V':
        return

    raise ValueError(kind)
