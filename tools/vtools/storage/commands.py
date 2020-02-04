import os
import glob
import re
import shutil
import struct
import subprocess
import tempfile
import click
from absl import logging


class Segment:
    # record batch header layout expressed in terms of a python struct format
    # string: {le-encoding, batch-size, base-offset, bathc-type, etc...}
    # https://docs.python.org/3.8/library/struct.html#format-strings
    HEADER_FMT = "<iqbihiqqqhii"

    def __init__(self, path):
        self.path = path
        self.base_offset = None
        self.type = None
        self.__parse()

    def __read_batch(self, f):
        size = struct.calcsize(self.HEADER_FMT)
        data = f.read(size)
        hdr = struct.unpack(self.HEADER_FMT, data)
        self.batch_size = hdr[0]
        self.base_offset = hdr[1]
        self.type = hdr[2]

    def __parse(self):
        with open(self.path, "rb") as f:
            self.__read_batch(f)


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
            print(ntp, "type", segment.type, "base offset",
                  segment.base_offset)
