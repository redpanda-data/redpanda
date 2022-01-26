import collections
import os

import struct
import crc32c
import glob
import re
import logging
from io import BytesIO
from reader import Reader
from writer import Writer

logger = logging.getLogger('rp')

# https://docs.python.org/3.8/library/struct.html#format-strings
#
# redpanda header prefix:
#   - little endian encoded
#   - batch size, base offset, type crc
#
# note that the crc that is stored is the crc reported by kafka which happens to
# be computed over the big endian encoding of the same data. thus to verify the
# crc we need to rebuild part of the header in big endian before adding to crc.
HDR_FMT_RP_PREFIX_NO_CRC = "iqbI"
HDR_FMT_RP_PREFIX = "<I" + HDR_FMT_RP_PREFIX_NO_CRC

# below the crc redpanda and kafka have the same layout
#   - little endian encoded
#   - attributes ... record_count
HDR_FMT_CRC = "hiqqqhii"

HDR_FMT_RP = HDR_FMT_RP_PREFIX + HDR_FMT_CRC
HEADER_SIZE = struct.calcsize(HDR_FMT_RP)

Header = collections.namedtuple(
    'Header', ('header_crc', 'batch_size', 'base_offset', 'type', 'crc',
               'attrs', 'delta', 'first_ts', 'max_ts', 'producer_id',
               'producer_epoch', 'base_seq', 'record_count'))

SEGMENT_NAME_PATTERN = re.compile(
    "(?P<base_offset>\d+)-(?P<term>\d+)-v(?P<version>\d)\.log")


class CorruptBatchError(Exception):
    def __init__(self, batch):
        self.batch = batch


class Record:
    def __init__(self, length, attrs, timestamp_delta, offset_delta, key,
                 value, headers):
        self.length = length
        self.attrs = attrs
        self.timestamp_delta = timestamp_delta
        self.offset_delta = offset_delta
        self.key = key
        self.value = value
        self.headers = headers


class RecordHeader:
    def __init__(self, key, value):
        self.key = key
        self.value = value


class RecordIter:
    def __init__(self, record_count, records_data):
        self.data_stream = BytesIO(records_data)
        self.rdr = Reader(self.data_stream)
        self.record_count = record_count

    def _parse_header(self):
        k_sz = self.rdr.read_varint()
        key = self.rdr.read_bytes(k_sz)
        v_sz = self.rdr.read_varint()
        value = self.rdr.read_bytes(v_sz)
        return RecordHeader(key, value)

    def __next__(self):
        if self.record_count == 0:
            raise StopIteration()

        self.record_count -= 1
        len = self.rdr.read_varint()
        attrs = self.rdr.read_int8()
        timestamp_delta = self.rdr.read_varint()
        offset_delta = self.rdr.read_varint()
        key_length = self.rdr.read_varint()
        if key_length > 0:
            key = self.rdr.read_bytes(key_length)
        else:
            key = None
        value_length = self.rdr.read_varint()
        if value_length > 0:
            value = self.rdr.read_bytes(value_length)
        else:
            value = None
        hdr_size = self.rdr.read_varint()
        headers = []
        for i in range(0, hdr_size):
            headers.append(self._parse_header())

        return Record(len, attrs, timestamp_delta, offset_delta, key, value,
                      headers)


class WritableRecord:
    def __init__(self, key, value, headers, offset_delta, ts_delta):
        self.attrs = 0
        self.key = key
        self.value = value
        self.headers = headers
        self.offset_delta = offset_delta
        self.ts_delta = ts_delta

    def size(self):
        # attributes
        sz = 1
        sz += Writer.vint_length(self.ts_delta)
        sz += Writer.vint_length(self.offset_delta)
        if self.key:
            sz += Writer.vint_length(len(self.key))
            sz += len(self.key)
        else:
            sz += Writer.vint_length(0)

        if self.value:
            sz += Writer.vint_length(len(self.value))
            sz += len(self.value)
        else:
            sz += Writer.vint_length(0)

        if self.headers:
            sz += Writer.vint_length(len(self.headers))
            for h in self.headers:
                sz += Writer.vint_length(len(h.key))
                sz += len(h.key)
                sz += Writer.vint_length(len(h.value))
                sz += len(h.value)
        else:
            sz += Writer.vint_length(0)

        return sz

    def write_into(self, stream):
        w = Writer(stream)
        w.write_vint(self.size())
        w.write_int8(self.attrs)
        w.write_vint(self.ts_delta)
        w.write_vint(self.offset_delta)
        if self.key:
            w.write_vint(len(self.key))
            w.write_bytes(self.key)
        else:
            w.write_vint(0)

        if self.value:
            w.write_vint(len(self.value))
            w.write_bytes(self.value)
        else:
            w.write_vint(0)

        w.write_vint(len(self.headers))
        for h in self.headers:
            if h.key:
                w.write_vint(len(h.key))
                w.write_bytes(h.key)
            if h.value:
                w.write_vint(len(h.value))
                w.write_bytes(h.value)


class WritableBatch:
    def __init__(self,
                 batch_type,
                 base_offset,
                 records,
                 attrs,
                 first_ts=0,
                 producer_id=-1,
                 producer_epoch=-1,
                 base_seq=-1):
        self.batch_type = batch_type
        self.base_offset = base_offset
        self.records = records
        self.attrs = attrs
        self.first_ts = first_ts
        self.producer_id = producer_id
        self.producer_epoch = producer_epoch
        self.base_seq = base_seq

    def build_header(self, records_buffer):

        h = {}
        h['batch_size'] = len(records_buffer) + HEADER_SIZE
        h['base_offset'] = self.base_offset
        h['type'] = self.batch_type
        h['attrs'] = self.attrs
        h['first_ts'] = self.first_ts
        h['delta'] = self.records[-1].offset_delta
        h['max_ts'] = self.records[-1].ts_delta + self.first_ts
        h['producer_id'] = self.producer_id
        h['producer_epoch'] = self.producer_epoch
        h['base_seq'] = self.base_seq
        h['record_count'] = len(self.records)
        h['crc'] = self.crc(h, records_buffer)

        h['header_crc'] = self.crc_header(h)
        return h

    def crc_header(self, header):
        bytes = struct.pack("<" + HDR_FMT_RP_PREFIX_NO_CRC + HDR_FMT_CRC,
                            header['batch_size'], header['base_offset'],
                            header['type'], header['crc'], header['attrs'],
                            header['delta'], header['first_ts'],
                            header['max_ts'], header['producer_id'],
                            header['producer_epoch'], header['base_seq'],
                            header['record_count'])
        return crc32c.crc32c(bytes)

    def crc(self, header, records):
        header_crc_bytes = struct.pack(">" + HDR_FMT_CRC, header['attrs'],
                                       header['delta'], header['first_ts'],
                                       header['max_ts'], header['producer_id'],
                                       header['producer_epoch'],
                                       header['base_seq'],
                                       header['record_count'])
        crc = crc32c.crc32c(header_crc_bytes)
        return crc32c.crc32c(records, crc)

    def encode(self):
        records_stream = BytesIO(b"")
        for r in self.records:
            r.write_into(records_stream)
        encoded_records = records_stream.getbuffer()
        hdr = self.build_header(encoded_records)
        out = BytesIO(b"")
        out.write(struct.pack(HDR_FMT_RP, *Header(**hdr)))
        out.write(encoded_records)
        return out.getbuffer()


class Batch:
    def __init__(self, index, header, records):
        self.index = index
        self.header = header
        self.term = None
        self.records = records

        header_crc_bytes = struct.pack(
            "<" + HDR_FMT_RP_PREFIX_NO_CRC + HDR_FMT_CRC, *self.header[1:])
        header_crc = crc32c.crc32c(header_crc_bytes)
        if self.header.header_crc != header_crc:
            raise CorruptBatchError(self)
        crc = crc32c.crc32c(self._crc_header_be_bytes())
        crc = crc32c.crc32c(records, crc)
        if self.header.crc != crc:
            raise CorruptBatchError(self)

    def last_offset(self):
        return self.header.base_offset + self.header.record_count - 1

    def _crc_header_be_bytes(self):
        # encode header back to big-endian for crc calculation
        return struct.pack(">" + HDR_FMT_CRC, *self.header[5:])

    @staticmethod
    def from_stream(f, index):
        data = f.read(HEADER_SIZE)
        if len(data) == HEADER_SIZE:
            header = Header(*struct.unpack(HDR_FMT_RP, data))
            # it appears that we may have hit a truncation point if all of the
            # fields in the header are zeros
            if all(map(lambda v: v == 0, header)):
                return
            records_size = header.batch_size - HEADER_SIZE
            data = f.read(records_size)
            assert len(data) == records_size
            return Batch(index, header, data)
        assert len(data) == 0

    def __iter__(self):
        return RecordIter(self.header.record_count, self.records)


class BatchIterator:
    def __init__(self, path):
        self.file = open(path, "rb")
        self.idx = 0

    def __next__(self):
        b = Batch.from_stream(self.file, self.idx)
        if not b:
            raise StopIteration()
        self.idx += 1
        return b

    def __del__(self):
        self.file.close()


class Segment:
    def __init__(self, path):
        self.path = path
        m = SEGMENT_NAME_PATTERN.match(os.path.basename(self.path))
        self.term = int(m['term'])
        self.base_offset = int(m['base_offset'])

    # we must consume a segment up to last valid batch to establish
    # last valid offset redpanda might have been force stopped so
    # there may be fallocated space at the end of segment file
    def _last_valid_offset(self):
        ret = 0
        with open(self.path, 'rb') as f:
            while True:
                b = Batch.from_stream(f, 0)
                if b:
                    ret = f.tell()
                else:
                    return ret

    def append(self, batch):

        last_valid_offset = self._last_valid_offset()
        with open(self.path, 'ab') as f:
            f.truncate(last_valid_offset)
            f.write(batch.encode())

    def __iter__(self):
        return BatchIterator(self.path)


class Ntp:
    def __init__(self, base_dir, namespace, topic, partition, ntp_id):
        self.base_dir = base_dir
        self.nspace = namespace
        self.topic = topic
        self.partition = partition
        self.ntp_id = ntp_id
        self.path = os.path.join(self.base_dir, self.nspace, self.topic,
                                 f"{self.partition}_{self.ntp_id}")
        pattern = os.path.join(self.path, "*.log")
        self.segments = glob.iglob(pattern)

        def _base_offset(segment_path):
            m = SEGMENT_NAME_PATTERN.match(os.path.basename(segment_path))
            return int(m['base_offset'])

        self.segments = sorted(self.segments, key=_base_offset)

    def __str__(self):
        return "{0.nspace}/{0.topic}/{0.partition}_{0.ntp_id}".format(self)


class Store:
    def __init__(self, base_dir):
        self.base_dir = os.path.abspath(base_dir)
        self.ntps = []
        self.__search()

    def __search(self):
        dirs = os.walk(self.base_dir)
        for ntpd in (p[0] for p in dirs if not p[1]):
            if 'cloud_storage_cache' in ntpd:
                continue
            head, part_ntp_id = os.path.split(ntpd)
            [part, ntp_id] = part_ntp_id.split("_")
            head, topic = os.path.split(head)
            head, nspace = os.path.split(head)
            assert head == self.base_dir
            ntp = Ntp(self.base_dir, nspace, topic, int(part), int(ntp_id))
            self.ntps.append(ntp)
