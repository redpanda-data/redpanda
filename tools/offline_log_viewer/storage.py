import collections
from enum import Enum
import os
import re
from os.path import join

import struct
import crc32c
import glob
import re
import logging
from io import BytesIO
from reader import Reader

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

    def kv_dict(self):
        key = None if self.key == None else self.key.hex()
        val = None if self.value == None else self.value.hex()
        return {"k": key, "v": val}


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


class BatchType(Enum):
    """Keep this in sync with model/record_batch_types.h"""
    raft_data = 1
    raft_configuration = 2
    controller = 3
    kvstore = 4
    checkpoint = 5
    topic_management_cmd = 6
    ghost_batch = 7
    id_allocator = 8
    tx_prepare = 9
    tx_fence = 10
    tm_update = 11
    user_management_cmd = 12
    acl_management_cmd = 13
    group_prepare_tx = 14
    group_commit_tx = 15
    group_abort_tx = 16
    node_management_cmd = 17
    data_policy_management_cmd = 18
    archival_metadata = 19
    cluster_config_cmd = 20
    feature_update = 21
    cluster_bootstrap_cmd = 22
    unknown = -1

    @classmethod
    def _missing_(e, value):
        return e.unknown


class Batch:
    class CompressionType(Enum):
        none = 0
        gzip = 1
        snappy = 2
        lz4 = 3
        zstd = 4
        unknown = -1

        @classmethod
        def _missing_(e, value):
            return e.unknown

    compression_mask = 0x7
    ts_type_mask = 0x8
    transactional_mask = 0x10
    control_mask = 0x20

    def __init__(self, index, header, records):
        self.index = index
        self.header = header
        self.term = None
        self.records = records
        self.type = BatchType(header[3])

        header_crc_bytes = struct.pack(
            "<" + HDR_FMT_RP_PREFIX_NO_CRC + HDR_FMT_CRC, *self.header[1:])
        header_crc = crc32c.crc32c(header_crc_bytes)
        if self.header.header_crc != header_crc:
            raise CorruptBatchError(self)
        crc = crc32c.crc32c(self._crc_header_be_bytes())
        crc = crc32c.crc32c(records, crc)
        if self.header.crc != crc:
            raise CorruptBatchError(self)

    def header_dict(self):
        header = self.header._asdict()
        attrs = header['attrs']
        header["type_name"] = self.type.name
        header['expanded_attrs'] = {
            'compression':
            Batch.CompressionType(attrs & Batch.compression_mask).name,
            'transactional':
            attrs & Batch.transactional_mask == Batch.transactional_mask,
            'control_batch': attrs & Batch.control_mask == Batch.control_mask,
            'timestamp_type': attrs & Batch.ts_type_mask == Batch.ts_type_mask
        }
        return header

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
            if len(data) < records_size:
                # Short read, probably end of a partially written log.
                logger.info(
                    "Stopping batch parse on short read (this is normal if the log was not from a clean shutdown)"
                )
                return None
            assert len(data) == records_size
            return Batch(index, header, data)

        if len(data) < HEADER_SIZE:
            # Short read, probably log being actively written or unclean shutdown
            return None

    def __len__(self):
        return self.header.record_count

    def __iter__(self):
        return RecordIter(self.header.record_count, self.records)


class BatchIterator:
    def __init__(self, path):
        self.path = path
        self.file = open(path, "rb")
        self.idx = 0

    def __next__(self):
        b = Batch.from_stream(self.file, self.idx)
        if not b:
            fsize = os.stat(self.path).st_size
            if fsize != self.file.tell():
                logger.warn(
                    f"Incomplete read of {self.path}: {self.file.tell()}/{fsize}"
                )
            raise StopIteration()
        self.idx += 1
        return b

    def __del__(self):
        self.file.close()


class Segment:
    def __init__(self, path):
        self.path = path

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


def listdirs(path):
    return [x for x in os.listdir(path) if os.path.isdir(join(path, x))]


class Store:
    def __init__(self, base_dir):
        self.base_dir = os.path.abspath(base_dir)
        self.ntps = []
        self.__search()

    def __search(self):
        for nspace in listdirs(self.base_dir):
            if nspace == "cloud_storage_cache":
                continue
            for topic in listdirs(join(self.base_dir, nspace)):
                for part_ntp_id in listdirs(join(self.base_dir, nspace,
                                                 topic)):
                    assert re.match("^\\d+_\\d+$", part_ntp_id)
                    [part, ntp_id] = part_ntp_id.split("_")
                    ntp = Ntp(self.base_dir, nspace, topic, int(part),
                              int(ntp_id))
                    self.ntps.append(ntp)
