#!/usr/bin/env python3
import os
import sys
import collections
import glob
import re
import struct
import crc32c
import logging
from io import BytesIO
import json
import datetime

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


class Record:
    def __init__(self, length, attrs, timestamp_delta, offset_delta, key,
                 value, offset):
        self.length = length
        self.attrs = attrs
        self.timestamp_delta = timestamp_delta
        self.offset_delta = offset_delta
        self.key = key
        self.value = value
        self.offset = offset


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


def read_broker_shard(reader):
    b = {}
    b['id'] = reader.read_int32()
    b['shard'] = reader.read_uint32()
    return b


def read_inc_update_op(reader):
    v = reader.read_int8()
    if v == 0:
        return 'none'
    elif v == 1:
        return 'set'
    elif v == 2:
        return 'remove'

    return 'unknown'


def read_property_update(reader, type_reader):
    u = {}
    u['value'] = type_reader(reader)
    u['type'] = read_inc_update_op(reader)
    return u


def decode_cleanup_policy(bitflags):
    D = 1
    C = 1 << 1
    compaction = (bitflags & C) == C
    deletion = (bitflags & D) == D
    if compaction and deletion:
        return "compact,delete"
    elif compaction:
        return "compact"
    elif deletion:
        return "deletion"

    return "none"


def read_incremental_properties_update(reader):
    u = {}
    u['compression'] = read_property_update(
        reader, lambda r: r.read_optional(lambda r: r.read_uint8()))
    u['cleanup_policy'] = read_property_update(
        reader, lambda r: r.read_optional(lambda r: r.read_int8()))
    u['compaction_strategy'] = read_property_update(
        reader, lambda r: r.read_optional(lambda r: r.read_int8()))
    u['ts_type'] = read_property_update(
        reader, lambda r: r.read_optional(lambda r: r.read_int8()))
    u['segment_size'] = read_property_update(
        reader, lambda r: r.read_optional(lambda r: r.read_uint64()))
    u['retention_bytes'] = read_property_update(
        reader, lambda r: r.read_tristate(lambda rdr: rdr.read_uint64()))
    u['retention_time'] = read_property_update(
        reader, lambda r: r.read_tristate(lambda rdr: rdr.read_uint64()))
    return u


def read_endpoint(r):
    ep = {}
    ep['name'] = r.read_string()
    ep['address'] = r.read_string()
    ep['port'] = r.read_uint16()
    return ep


def read_broker(rdr):
    br = {}
    br['id'] = rdr.read_int32()
    br['kafka_endpoints'] = rdr.read_vector(lambda r: read_endpoint(r))
    br['rpc_address'] = rdr.read_string()
    br['rpc_port'] = rdr.read_uint16()
    br['rack'] = rdr.read_optional(lambda r: r.read_string())
    br['cores'] = rdr.read_uint32()
    br['memory'] = rdr.read_uint32()
    br['disk'] = rdr.read_uint32()
    br['mount_paths'] = rdr.read_vector(lambda r: r.read_string())
    br['etc'] = rdr.read_vector(lambda r: (r.read_string(), r.read_string()))
    return br


def read_vnode(r):
    vn = {}
    vn['id'] = r.read_int32()
    vn['revision'] = r.read_int64()
    return vn


def read_group_nodes(r):
    ret = {}
    ret['voters'] = r.read_vector(read_vnode)
    ret['learners'] = r.read_vector(read_vnode)
    return ret


class RecordDecoder:
    def __init__(self, batch):
        self.batch = batch
        self.stream = BytesIO(self.batch.records)

    def decode_records(self):
        records = []
        for i in range(self.batch.header.record_count):
            records.append(self.decode_record())
        return records

    def decode_record(self):
        rdr = Reader(self.stream)
        len = rdr.read_varint()
        attrs = rdr.read_int8()
        timestamp_delta = rdr.read_varint()
        offset_delta = rdr.read_varint()
        key_length = rdr.read_varint()
        key = rdr.read_bytes(key_length)
        value_length = rdr.read_varint()
        value = rdr.read_bytes(value_length)
        return Record(len, attrs, timestamp_delta, offset_delta, key, value,
                      self.batch.header.base_offset + offset_delta)


def decode_user_cmd_type(tp):
    if tp == 5:
        return "create user"
    elif tp == 6:
        return "delete user"
    elif tp == 7:
        return "update user"
    return "unknown"


def decode_acls_cmd_type(tp):
    if tp == 8:
        return "create acls"
    elif tp == 9:
        return "delete acls"
    return "unknown"


def decode_acl_resource(r):
    if r == 0:
        return 'topic'
    elif r == 1:
        return 'group'
    elif r == 2:
        return 'cluster'
    elif r == 3:
        return 'transactional_id'

    return "unknown"


def decode_acl_pattern_type(p):
    if p == 0:
        return 'litteral'
    elif p == 1:
        return 'prefixed'

    return "unknown"


def decode_acl_permission(p):
    if p == 0:
        return 'deny'
    elif p == 1:
        return 'allow'
    return "unknown"


def decode_acl_principal_type(p):
    if p == 0:
        return 'user'

    return "unknown"


def decode_acl_operation(o):
    if o == 0:
        return 'all'
    elif o == 1:
        return "read"
    elif o == 2:
        return "write"
    elif o == 3:
        return "create"
    elif o == 4:
        return "remove"
    elif o == 5:
        return "alter"
    elif o == 6:
        return "describe"
    elif o == 7:
        return "cluster_action"
    elif o == 8:
        return "describe_configs"
    elif o == 9:
        return "alter_configs"
    elif o == 10:
        return "idempotent_write"

    return "unknown"


def read_acl_pattern(rdr):
    pattern = {}
    pattern['resource'] = decode_acl_resource(rdr.read_int8())
    pattern['name'] = rdr.read_string()
    pattern['type'] = decode_acl_pattern_type(rdr.read_int8())

    return pattern


def read_acl_entry(rdr):
    entry = {}
    entry['principal'] = {}
    entry['principal']['type'] = decode_acl_principal_type(rdr.read_int8())
    entry['principal']['name'] = rdr.read_string()
    entry['host'] = {}
    entry['host']['ipv4'] = rdr.read_bool()
    entry['host']['data'] = rdr.read_optional(lambda r: r.read_iobuf())
    entry['operation'] = decode_acl_operation(rdr.read_int8())
    entry['permission'] = decode_acl_permission(rdr.read_int8())

    return entry


def read_acl(rdr):
    acl = {}
    acl['pattern'] = read_acl_pattern(rdr)
    acl['entry'] = read_acl_entry(rdr)
    return acl


def obfuscate_secret(s):
    return f"{s[0:3]}..."


def read_broker_shard(rdr):
    bs = {}
    bs['node_id'] = rdr.read_int32()
    bs['shard'] = rdr.read_uint32()
    return bs


def read_partition_assignment(rdr):
    pas = {}
    pas['group_id'] = rdr.read_int64()
    pas['partition_id'] = rdr.read_int32()
    pas['replicas'] = rdr.read_vector(read_broker_shard)
    return pas


class ControllerRecordDecoder:
    def __init__(self, record, header):
        self.record = record
        self.header = header
        self.batch_type = header.type
        self.offset_delta = record.offset_delta
        self.v_stream = BytesIO(self.record.value)
        self.k_stream = BytesIO(self.record.key)

    def _decode_topic_command(self):
        rdr = Reader(self.v_stream)
        k_rdr = Reader(self.k_stream)
        cmd = {}
        cmd['type'] = rdr.read_int8()
        if cmd['type'] == 0:
            cmd['type_string'] = 'create_topic'
            cmd['namespace'] = rdr.read_string()
            cmd['topic'] = rdr.read_string()
            cmd['partitions'] = rdr.read_int32()
            cmd['replication_factor'] = rdr.read_int16()
            cmd['compression'] = rdr.read_optional(lambda r: r.read_int8())
            cmd['cleanup_policy_bitflags'] = rdr.read_optional(
                lambda r: decode_cleanup_policy(r.read_int8()))
            cmd['compaction_strategy'] = rdr.read_optional(
                lambda r: r.read_int8())
            cmd['timestamp_type'] = rdr.read_optional(lambda r: r.read_int8())
            cmd['segment_size'] = rdr.read_optional(lambda r: r.read_int64())
            cmd['retention_bytes'] = rdr.read_tristate(
                lambda r: r.read_int64())
            cmd['retention_duration'] = rdr.read_tristate(
                lambda r: r.read_int64())
            cmd['assignments'] = rdr.read_vector(read_partition_assignment)
        elif cmd['type'] == 1:
            cmd['type_string'] = 'delete_topic'
            cmd['namespace'] = rdr.read_string()
            cmd['topic'] = rdr.read_string()
        elif cmd['type'] == 2:
            cmd['type_string'] = 'update_partitions'
            cmd['namespace'] = k_rdr.read_string()
            cmd['topic'] = k_rdr.read_string()
            cmd['partition'] = k_rdr.read_int32()
            cmd['replicas'] = rdr.read_vector(lambda r: read_broker_shard(r))

        elif cmd['type'] == 3:
            cmd['type_string'] = 'finish_partitions_update'
            cmd['namespace'] = k_rdr.read_string()
            cmd['topic'] = k_rdr.read_string()
            cmd['partition'] = k_rdr.read_int32()
            cmd['replicas'] = rdr.read_vector(lambda r: read_broker_shard(r))
        elif cmd['type'] == 4:
            cmd['type_string'] = 'update_topic_properties'
            cmd['namespace'] = k_rdr.read_string()
            cmd['topic'] = k_rdr.read_string()
            cmd['update'] = read_incremental_properties_update(rdr)

        return cmd

    def _decode_config(self):
        rdr = Reader(self.v_stream)
        cfg = {}
        cfg['version'] = rdr.read_int8()
        cfg['brokers'] = rdr.read_vector(read_broker)
        cfg['current_config'] = read_group_nodes(rdr)
        cfg['prev_config'] = rdr.read_optional(read_group_nodes)
        cfg['revision'] = rdr.read_int64()

        return cfg

    def _decode_user_command(self):
        rdr = Reader(self.v_stream)
        k_rdr = Reader(self.k_stream)
        cmd = {}
        cmd['type'] = rdr.read_int8()
        cmd['str_type'] = decode_user_cmd_type(cmd['type'])

        if cmd['type'] == 5 or cmd['type'] == 7:
            cmd['user'] = k_rdr.read_string()
            cmd['cred'] = {}
            cmd['cred']['version'] = rdr.read_int8()
            cmd['cred']['salt'] = rdr.read_iobuf().hex()
            cmd['cred']['server_key'] = rdr.read_iobuf().hex()
            cmd['cred']['stored_key'] = rdr.read_iobuf().hex()
            # obfuscate secrets
            cmd['cred']['salt'] = obfuscate_secret(cmd['cred']['salt'])
            cmd['cred']['server_key'] = obfuscate_secret(
                cmd['cred']['server_key'])
            cmd['cred']['stored_key'] = obfuscate_secret(
                cmd['cred']['stored_key'])

        elif cmd['type'] == 6:
            cmd['user'] = k_rdr.read_string()

        return cmd

    def _decode_acl_command(self):
        rdr = Reader(self.v_stream)
        k_rdr = Reader(self.k_stream)
        cmd = {}
        cmd['type'] = rdr.read_int8()
        cmd['str_type'] = decode_acls_cmd_type(cmd['type'])
        if cmd['type'] == 8:
            cmd['version'] = k_rdr.read_int8()
            cmd['acls'] = k_rdr.read_vector(read_acl)
        elif cmd['type'] == 9:
            cmd['version'] = k_rdr.read_int8()

        return cmd

    def decode(self):
        ret = {}
        ret['type'] = self.type_str()
        ret['epoch'] = self.header.first_ts
        ret['offset'] = self.header.base_offset + self.offset_delta
        ret['ts'] = datetime.datetime.utcfromtimestamp(
            self.header.first_ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
        ret['data'] = None

        if self.batch_type == 2:
            ret['data'] = self._decode_config()
        if self.batch_type == 6:
            ret['data'] = self._decode_topic_command()
        if self.batch_type == 12:
            ret['data'] = self._decode_user_command()
        if self.batch_type == 13:
            ret['data'] = self._decode_acl_command()

        return ret

    def type_str(self):
        if self.batch_type == 1:
            return "data"
        if self.batch_type == 2:
            return "configuration"
        if self.batch_type == 3:
            return "old controller"
        if self.batch_type == 4:
            return "kv store"
        if self.batch_type == 5:
            return "checkpoint"
        if self.batch_type == 6:
            return "topic command"
        if self.batch_type == 12:
            return "user management command"
        if self.batch_type == 13:
            return "acl management command"

        return f"unknown {self.batch_type}"


class Segment:
    def __init__(self, path):
        self.path = path
        self.batches = []
        self.__read_batches()

    def __read_batches(self):
        index = 1
        with open(self.path, "rb") as f:
            while True:
                batch = Batch.from_file(f, index)
                if not batch:
                    break
                self.batches.append(batch)
                index += 1

    def dump(self):
        if self.batches:
            next_offset = self.batches[0].header.base_offset
        index = 0
        for batch in self.batches:
            d = RecordDecoder(batch)
            records = d.decode_records()
            for r in records:
                decoder = ControllerRecordDecoder(r, batch.header)
                print(json.dumps(decoder.decode(), indent=4))


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
        if ntp.nspace == "redpanda" and ntp.topic == "controller":
            for path in ntp.segments:
                s = Segment(path)
                s.dump()


if __name__ == '__main__':
    main()
