#!/usr/bin/env python3
import os
import sys
import collections
import glob
import re
import struct
import crc32c
import logging

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


class CorruptBatchError(Exception):
    def __init__(self, batch):
        self.batch = batch


class Batch:
    def __init__(self, index, header, records):
        self.index = index
        self.header = header

        header_crc_bytes = struct.pack(
            "<" + HDR_FMT_RP_PREFIX_NO_CRC + HDR_FMT_CRC, *self.header[1:])
        header_crc = crc32c.crc32(header_crc_bytes)
        if self.header.header_crc != header_crc:
            raise CorruptBatchError(self)

        crc = crc32c.crc32(self._crc_header_be_bytes())
        crc = crc32c.crc32(records, crc)
        if self.header.crc != crc:
            raise CorruptBatchError(self)

    def last_offset(self):
        return self.header.base_offset + self.header.record_count - 1

    def _crc_header_be_bytes(self):
        # encode header back to big-endian for crc calculation
        return struct.pack(">" + HDR_FMT_CRC, *self.header[5:])

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
    def __init__(self, path):
        self.path = path
        self.__read_batches()

    def __read_batches(self):
        index = 1
        with open(self.path, "rb") as f:
            while True:
                batch = Batch.from_file(f, index)
                if not batch:
                    break
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
        parser.add_argument('--path',
                            type=str,
                            help='Path to the log desired to be analyzed')
        return parser

    parser = generate_options()
    options, program_options = parser.parse_known_args()
    logger.info("%s" % options)
    if not os.path.exists(options.path):
        logger.error("Path doesn't exist %s" % options.path)
        sys.exit(1)
    store = Store(options.path)
    for ntp in store.ntps:
        for path in ntp.segments:
            try:
                s = Segment(path)
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
