import os
import collections
import glob
import re
import shutil
import struct
import subprocess
import tempfile
import click
import crc32c
from absl import logging

# https://docs.python.org/3.8/library/struct.html#format-strings
#
# redpanda header prefix:
#   - little endian encoded
#   - batch size, base offset, type crc
#
# note that the crc that is stored is the crc reported by kafka which happens to
# be computed over the big endian encoding of the same data. thus to verify the
# crc we need to rebuild part of the header in big endian before adding to crc.
HDR_FMT_RP_PREFIX = "<iqbI"

# below the crc redpanda and kafka have the same layout
#   - little endian encoded
#   - attributes ... record_count
HDR_FMT_CRC = "hiqqqhii"

HDR_FMT_RP = HDR_FMT_RP_PREFIX + HDR_FMT_CRC
HEADER_SIZE = struct.calcsize(HDR_FMT_RP)

Header = collections.namedtuple(
    'Header',
    ('batch_size', 'base_offset', 'type', 'crc', 'attrs', 'delta', 'first_ts',
     'max_ts', 'producer_id', 'producer_epoch', 'base_seq', 'record_count'))


class Batch:
    def __init__(self, header, records):
        self.header = header
        crc = crc32c.crc32(self._crc_header_be_bytes())
        crc = crc32c.crc32(records, crc)
        assert self.header.crc == crc

    def last_offset(self):
        return self.header.base_offset + self.header.record_count - 1

    def _crc_header_be_bytes(self):
        # encode header back to big-endian for crc calculation
        return struct.pack(">" + HDR_FMT_CRC, *self.header[4:])

    @staticmethod
    def from_file(f):
        data = f.read(HEADER_SIZE)
        if len(data) == HEADER_SIZE:
            header = Header(*struct.unpack(HDR_FMT_RP, data))
            records_size = header.batch_size - HEADER_SIZE
            data = f.read(records_size)
            assert len(data) == records_size
            return Batch(header, data)
        assert len(data) == 0


class Segment:
    def __init__(self, path):
        self.path = path
        self.batches = []
        self.__read_batches()

    def __read_batches(self):
        print(self.path)
        with open(self.path, "rb") as f:
            while True:
                batch = Batch.from_file(f)
                if not batch:
                    break
                self.batches.append(batch)

    def dump(self):
        if self.batches:
            next_offset = self.batches[0].header.base_offset
        index = 0
        for batch in self.batches:
            if index < 3 or index == (len(self.batches) - 1):
                print(batch.header.base_offset)
            if index == 3 and len(self.batches) > 4:
                print("...")
            if batch.header.base_offset != next_offset:
                print("hole discovered at offset {} expected {}".format(
                    batch.header.base_offset, next_offset))
                break
            next_offset = batch.last_offset() + 1
            index += 1


class Ntp:
    def __init__(self, base_dir, namespace, topic, partition):
        self.base_dir = base_dir
        self.nspace = namespace
        self.topic = topic
        self.partition = partition
        self.path = os.path.join(self.base_dir, self.nspace, self.topic,
                                 str(self.partition))
        self.segments = []
        self.__segments()

    def __str__(self):
        return "{0.nspace}/{0.topic}/{0.partition}".format(self)

    def __segments(self):
        pattern = os.path.join(self.path, "*.log")
        paths = glob.iglob(pattern)
        self.segments = map(lambda p: Segment(p), paths)


class Store:
    def __init__(self, base_dir):
        self.base_dir = os.path.abspath(base_dir)
        self.ntps = []
        self.__search()

    def __search(self):
        dirs = os.walk(self.base_dir)
        for ntpd in (p[0] for p in dirs if not p[1]):
            head, part = os.path.split(ntpd)
            head, topic = os.path.split(head)
            head, nspace = os.path.split(head)
            assert head == self.base_dir
            ntp = Ntp(self.base_dir, nspace, topic, int(part))
            self.ntps.append(ntp)


@click.group(short_help='storage')
def storage():
    pass


@storage.command(short_help='Report storage summary')
@click.option("--path", help="Path to storage root")
def summary(path):
    store = Store(path)
    for ntp in store.ntps:
        for segment in ntp.segments:
            print(ntp)
