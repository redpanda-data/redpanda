import logging
from enum import Enum
import struct
import collections
from io import BufferedReader, BytesIO

SERDE_ENVELOPE_FORMAT = "<BBI"
SERDE_ENVELOPE_SIZE = struct.calcsize(SERDE_ENVELOPE_FORMAT)

SerdeEnvelope = collections.namedtuple('SerdeEnvelope',
                                       ('version', 'compat_version', 'size'))

logger = logging.getLogger('reader')


class Endianness(Enum):
    BIG_ENDIAN = 0
    LITTLE_ENDIAN = 1


class Reader:
    def __init__(self, stream, endianness=Endianness.LITTLE_ENDIAN):
        # BytesIO provides .getBuffer(), BufferedReader peek()
        self.stream = BufferedReader(stream)
        self.endianness = endianness

    @staticmethod
    def _decode_zig_zag(v):
        return (v >> 1) ^ (~(v & 1) + 1)

    def read_varint(self):
        shift = 0
        result = 0
        while True:
            i = ord(self.stream.read(1))
            if i & 128:
                result |= ((i & 0x7f) << shift)
            else:
                result |= i << shift
                break
            shift += 7

        return Reader._decode_zig_zag(result)

    def with_endianness(self, str):
        ch = '<' if self.endianness == Endianness.LITTLE_ENDIAN else '>'
        return f"{ch}{str}"

    def read_int8(self):
        return struct.unpack(self.with_endianness('b'), self.stream.read(1))[0]

    def read_uint8(self):
        return struct.unpack(self.with_endianness('B'), self.stream.read(1))[0]

    def read_int16(self):
        return struct.unpack(self.with_endianness('h'), self.stream.read(2))[0]

    def read_uint16(self):
        return struct.unpack(self.with_endianness('H'), self.stream.read(2))[0]

    def read_int32(self):
        return struct.unpack(self.with_endianness('i'), self.stream.read(4))[0]

    def read_uint32(self):
        return struct.unpack(self.with_endianness('I'), self.stream.read(4))[0]

    def read_int64(self):
        return struct.unpack(self.with_endianness('q'), self.stream.read(8))[0]

    def read_uint64(self):
        return struct.unpack(self.with_endianness('Q'), self.stream.read(8))[0]

    def read_serde_enum(self):
        return self.read_int32()

    def read_iobuf(self):
        len = self.read_int32()
        return self.stream.read(len)

    def read_bool(self):
        return self.read_int8() == 1

    def read_string(self):
        len = self.read_int32()
        return self.stream.read(len).decode('utf-8')

    def read_kafka_string(self):
        len = self.read_int16()
        return self.stream.read(len).decode('utf-8')

    def read_kafka_bytes(self):
        len = self.read_int32()
        return self.stream.read(len)

    def read_optional(self, type_read):
        present = self.read_int8()
        if present == 0:
            return None
        return type_read(self)

    def read_kafka_optional_string(self):
        len = self.read_int16()
        if len == -1:
            return None
        return self.stream.read(len).decode('utf-8')

    def read_vector(self, type_read):
        sz = self.read_int32()
        ret = []
        for i in range(0, sz):
            ret.append(type_read(self))
        return ret

    def read_envelope(self, type_read=None, max_version=0):
        header = self.read_bytes(SERDE_ENVELOPE_SIZE)
        envelope = SerdeEnvelope(*struct.unpack(SERDE_ENVELOPE_FORMAT, header))
        if type_read is not None:
            if envelope.version <= max_version:
                return {
                    'envelope': envelope
                } | type_read(self, envelope.version)
            else:
                logger.error(
                    f"can't decode {envelope.version=}, check {type_read=} or {max_version=}"
                )
                return {
                    'error': {
                        'max_supported_version': max_version,
                        'envelope': envelope
                    }
                }
        return envelope

    def read_serde_vector(self, type_read):
        sz = self.read_uint32()
        ret = []
        for i in range(0, sz):
            ret.append(type_read(self))
        return ret

    def read_tristate(self, type_read):
        state = self.read_int8()
        t = {}
        if state == -1:
            t['state'] = 'disabled'
        elif state == 0:
            t['state'] = 'empty'
        else:
            t['value'] = type_read(self)
        return t

    def read_bytes(self, length):
        return self.stream.read(length)

    def peek(self, length):
        return self.stream.peek(length)

    def read_uuid(self):
        return ''.join([
            f'{self.read_uint8():02x}' + ('-' if k in [3, 5, 7, 9] else '')
            for k in range(16)
        ])

    def peek_int8(self):
        # peek returns the whole memory buffer, slice is needed to conform to struct format string
        return struct.unpack('<b', self.stream.peek(1)[:1])[0]

    def skip(self, length):
        self.stream.read(length)

    def remaining(self):
        return len(self.stream.raw.getbuffer()) - self.stream.tell()

    def read_serde_map(self, k_reader, v_reader):
        ret = {}
        for _ in range(self.read_uint32()):
            key = k_reader(self)
            val = v_reader(self)
            ret[key] = val
        return ret
