from io import BytesIO
import struct


class Writer:
    def __init__(self, stream):
        self.stream = stream

    @staticmethod
    def _encode_zig_zag(v):
        return (v << 1) ^ (v >> 63)

    @staticmethod
    def vint_length(v):
        n = Writer._encode_zig_zag(v)
        length = 1
        while n >= 0x80:
            length += 1
            n >>= 7
        return length

    def write_vint(self, v):
        n = Writer._encode_zig_zag(v)
        data = BytesIO(b'')

        while n >= 0x80:
            data.write(struct.pack("<B", (n & 0x7f) | 0x80))
            n >>= 7
        data.write(struct.pack("<B", n))

        self.stream.write(data.getbuffer())

    def write_int8(self, v):
        bytes = struct.pack("<b", v)
        self.stream.write(bytes)

    def write_int32(self, v):
        bytes = struct.pack("<i", v)
        self.stream.write(bytes)

    def write_int64(self, v):
        bytes = struct.pack("<q", v)
        self.stream.write(bytes)

    def write_bytes(self, v):
        self.stream.write(v)

    def write_nullopt(self):
        self.write_int8(0)

    def write_optional(self, fun):
        self.write_int8(1)
        fun(self)
