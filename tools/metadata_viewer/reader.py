import struct
import collections

SERDE_ENVELOPE_FORMAT = "<BBI"
SERDE_ENVELOPE_SIZE = struct.calcsize(SERDE_ENVELOPE_FORMAT)

SerdeEnvelope = collections.namedtuple('SerdeEnvelope',
                                       ('version', 'compat_version', 'size'))


class Reader:
    def __init__(self, stream):
        self.stream = stream

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

    def read_envelope(self):
        data = self.read_bytes(SERDE_ENVELOPE_SIZE)
        return SerdeEnvelope(*struct.unpack(SERDE_ENVELOPE_FORMAT, data))

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

    def skip(self, length):
        self.stream.read(length)
