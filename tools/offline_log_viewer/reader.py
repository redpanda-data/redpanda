import logging
from enum import Enum
import struct
import collections
from io import BufferedReader, BytesIO

SERDE_ENVELOPE_FORMAT = "<BBI"
SERDE_CHECKSUM_ENVELOPE_FORMAT = "<BBIi"
SERDE_ENVELOPE_SIZE = struct.calcsize(SERDE_ENVELOPE_FORMAT)
SERDE_CHECKSUM_ENVELOPE_SIZE = struct.calcsize(SERDE_CHECKSUM_ENVELOPE_FORMAT)

SerdeEnvelope = collections.namedtuple('SerdeEnvelope',
                                       ('version', 'compat_version', 'size'))
SerdeChecksumEnvelope = collections.namedtuple(
    'SerdeChecksumEnvelope', ('version', 'compat_version', 'size', 'checksum'))

logger = logging.getLogger('reader')


class Endianness(Enum):
    BIG_ENDIAN = 0
    LITTLE_ENDIAN = 1


class Reader:
    def __init__(self,
                 stream,
                 endianness=Endianness.LITTLE_ENDIAN,
                 include_envelope=False):
        # BytesIO provides .getBuffer(), BufferedReader peek()
        self.stream = BufferedReader(stream)
        self.endianness = endianness
        self.include_envelope = include_envelope

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

    def read_map(self, k_reader, v_reader):
        ret = {}
        for _ in range(self.read_uint32()):
            key = k_reader(self)
            val = v_reader(self)
            ret[key] = val
        return ret

    def read_envelope(self, type_read=None, reader_version=0):
        header = self.read_bytes(SERDE_ENVELOPE_SIZE)
        envelope = SerdeEnvelope(*struct.unpack(SERDE_ENVELOPE_FORMAT, header))
        return self.read_envelope_inner(envelope, type_read, reader_version)

    def read_checksum_envelope(self, type_read=None, reader_version=0):
        header = self.read_bytes(SERDE_CHECKSUM_ENVELOPE_SIZE)
        envelope = SerdeChecksumEnvelope(
            *struct.unpack(SERDE_CHECKSUM_ENVELOPE_FORMAT, header))
        return self.read_envelope_inner(envelope, type_read, reader_version)

    def read_envelope_inner(self, envelope, type_read, reader_version):
        bytes_left_in_envelope = envelope.size
        envelope_start_pos = self.stream.tell()

        if type_read is not None:
            # This is technical debt. We are currently assuming that all
            # type_read methods were written to handle the original envelope
            # declarations. If their compat_version is greater than 0, it
            # means that they do contain breaking changes.
            #
            # We should extend the interface to allow reading different compat
            # versions of the envelope in some way.
            if envelope.compat_version == 0:
                if envelope.version > reader_version:
                    logger.warning(
                        f"Reading {type_read=} envelope with version {envelope.version},"
                        f" but max compat version is {reader_version}."
                        f" The output is likely to be incomplete.")

                v = type_read(self, envelope.version)

                # Skip the rest of the envelope.
                bytes_left_in_envelope -= self.stream.tell(
                ) - envelope_start_pos
                self.stream.read(bytes_left_in_envelope)

                if not self.include_envelope:
                    return v
                else:
                    return {'envelope': envelope} | v
            else:
                # Skip the the envelope.
                self.stream.read(bytes_left_in_envelope)
                logger.error(
                    f"can't decode {envelope.version=}, check {envelope.compat_version=} > {reader_version=} for {type_read=}"
                )
                return {
                    'error': {
                        'reader_version': reader_version,
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
