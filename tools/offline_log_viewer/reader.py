import struct
import collections
from io import BufferedReader, BytesIO

SERDE_ENVELOPE_FORMAT = "<BBI"
SERDE_ENVELOPE_SIZE = struct.calcsize(SERDE_ENVELOPE_FORMAT)

SerdeEnvelope = collections.namedtuple('SerdeEnvelope',
                                       ('version', 'compat_version', 'size'))


class Reader:
    def __init__(self, stream: BytesIO):
        # BytesIO provides .getBuffer(), BufferedReader peek()
        self.stream = BufferedReader(stream)

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

    def read_int8(self):
        return struct.unpack("<b", self.stream.read(1))[0]

    def read_uint8(self):
        return struct.unpack("<B", self.stream.read(1))[0]

    def read_int16(self):
        return struct.unpack("<h", self.stream.read(2))[0]

    def read_uint16(self):
        return struct.unpack("<H", self.stream.read(2))[0]

    def read_int32(self):
        return struct.unpack("<i", self.stream.read(4))[0]

    def read_uint32(self):
        return struct.unpack("<I", self.stream.read(4))[0]

    def read_int64(self):
        return struct.unpack("<q", self.stream.read(8))[0]

    def read_uint64(self):
        return struct.unpack("<Q", self.stream.read(8))[0]

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

    def read_optional(self, type_read):
        present = self.read_int8()
        if present == 0:
            return None
        return type_read(self)

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
            ret[k_reader(self)] = v_reader(self)
        return ret
