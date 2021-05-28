#!/usr/bin/env python3
import os
import sys
import collections
import glob
import struct
import crc32c
import logging
import zstd
import six

logger = logging.getLogger('storage')
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

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


class CorruptBatchError(Exception):
    def __init__(self, batch):
        self.batch = batch


def read_varlong(buffer, pos):
    """
    Decodes zig-zag encoded signed integers up to 64 bits long as per utils/vint.h
    from `buffer` starting at `pos`.

    Returns (value, next @pos)
    """
    bits = 64
    signbit = 1 << (bits - 1)
    mask = (1 << bits) - 1
    result = 0
    shift = 0
    while 1:
        b = six.indexbytes(buffer, pos)
        result |= ((b & 0x7f) << shift)
        pos += 1
        if not (b & 0x80):
            result &= mask
            result = (result ^ signbit) - signbit
            result = int(result)
            result = (result >> 1) ^ -(result & 1)
            return (result, pos)
        shift += 7
        if shift >= 64:
            raise _DecodeError('Too many bytes when decoding varint.')

class Record:
    def __init__(self, size, record_attr, timestamp_delta, offset_delta, key, headers, value):
        self.size = size
        self.record_attr = record_attr
        self.timestamp_delta = timestamp_delta
        self.offset_delta = offset_delta
        self.key = key
        self.headers = headers
        self.value = value 

    def __str__(self):
        return "{}:{}".format(self.key.decode("utf-8"), self.value.decode("utf-8"))


    @staticmethod
    def record_from_bytes(b):
        (size, v) = read_varlong(b, 0)
        
        record_attr = b[v]
        v = v + 1

        (timestamp_delta, v) = read_varlong(b, v)
        (offset_delta, v) = read_varlong(b, v)

        (key_length, v) = read_varlong(b, v)
        key = b[v:v+key_length]
        v = v + key_length

        (value_length, v) = read_varlong(b, v)
        value = b[v:v+value_length]
        v = v + value_length

        (header_count, v) = read_varlong(b, v)
        headers = []
        for i in range(0, header_count):
            (header_length, v) = read_varlong(b, v)
            header = b[v:v+header_length]
            v = v + header_length
            headers.append(header)

        return Record(size, record_attr, timestamp_delta, offset_delta, key, headers, value)

    @staticmethod
    def from_bytes(b, count):
        records = []
        for i in range(0, count):
            record = Record.record_from_bytes(b)
            b = b[:record.size]
            records.append(record)
        return records


class Batch:
    def __init__(self, index, header, records):
        self.index = index
        self.header = header
        self.records = records

    def verify_crc(self):
        header_crc_bytes = struct.pack(
            "<" + HDR_FMT_RP_PREFIX_NO_CRC + HDR_FMT_CRC, *self.header[1:])
        header_crc = crc32c.crc32c(header_crc_bytes)
        if self.header.header_crc != header_crc:
            raise CorruptBatchError(self)

        crc = crc32c.crc32c(self._crc_header_be_bytes())
        crc = crc32c.crc32c(self.records, crc)
        if self.header.crc != crc:
            raise CorruptBatchError(self)

    def last_offset(self):
        return self.header.base_offset + self.header.delta

    def _crc_header_be_bytes(self):
        # encode header back to big-endian for crc calculation
        return struct.pack(">" + HDR_FMT_CRC, *self.header[5:])

    def parse_records(self):
        batch = self.records
        try:
            batch = zstd.decompress(self.records)
            return Record.from_bytes(batch, self.header.record_count)
        except zstd.Error as e:
            logger.warn(f"zstd decoding failure, header: {self.header}", e)
        except Exception as e:
            logger.warn(f"Unable to decode batch, header: {self.header}", e)
        return []

    @staticmethod
    def from_file(f, index):
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


class Segment:
    def __init__(self, path, dump=False):
        self.path = path
        self.prev_batch = None
        self.__read_batches(dump)

    def __handle_batch(self, batch):
        if self.prev_batch:
            expected = self.prev_batch.last_offset() + 1
            if batch.header.base_offset != expected:
                logger.info("Encountered hole (expected for compacted topics)")
                logger.info(f"Previous header: {self.prev_batch.header}")
                logger.info(f"Current  header: {batch.header}")


    def __read_batches(self, dump=False):
        index = 1
        with open(self.path, "rb") as f:
            while True:
                batch = Batch.from_file(f, index)
                if not batch:
                    break

                self.__handle_batch(batch)
                self.prev_batch = batch
                batch.verify_crc()

                if dump:
                    for record in batch.parse_records():
                        print(record)
                index += 1

    def dump(self):
        if self.batches:
            next_offset = self.batches[0].header.base_offset
        index = 0
        for batch in self.batches:
            if index < 3 or index == (len(self.batches) - 1):
                logger.info(batch.header.base_offset)
            if index == 3 and len(self.batches) > 4:
                logger.info("...")
            if batch.header.base_offset != next_offset:
                logger.info("hole discovered at offset {} expected {}".format(
                    batch.header.base_offset, next_offset))
                break
            next_offset = batch.last_offset() + 1
            index += 1


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
            head, part_ntp_id = os.path.split(ntpd)
            [part, ntp_id] = part_ntp_id.split("_")
            head, topic = os.path.split(head)
            head, nspace = os.path.split(head)
            assert head == self.base_dir
            ntp = Ntp(self.base_dir, nspace, topic, int(part), int(ntp_id))
            self.ntps.append(ntp)


def main():
    import argparse

    def generate_options():
        parser = argparse.ArgumentParser(description='Redpanda log analyzer')
        parser.add_argument('--dump',
                            type=str,
                            required=False,
                            help='Dump key:value for a topic as utf-8 to stdout')
        parser.add_argument('--path',
                            type=str,
                            required=True,
                            help='Path to the log desired to be analyzed')
        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()
    logger.info("%s" % options)
    if not os.path.exists(options.path):
        logger.error("Path doesn't exist %s" % options.path)
        sys.exit(1)
    store = Store(options.path)
    for ntp in store.ntps:
        dump = options.dump == ntp.topic
        for path in ntp.segments:
            try:
                s = Segment(path, dump=dump)
            except CorruptBatchError as e:
                logger.error(
                    "corruption detected in batch {} of segment: {}".format(
                        e.batch.index, path))
                logger.error("header of corrupt batch: {}".format(
                    e.batch.header))
                sys.exit(1)
            logger.info("successfully decoded segment: {}".format(path))


if __name__ == '__main__':
    main()
