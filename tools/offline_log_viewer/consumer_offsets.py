import base64
from io import BytesIO
from model import *
from reader import Endianness, Reader
from storage import Segment
import datetime


def decode_key_type(v):
    if v == 0 or v == 1:
        return "offset_commit"
    elif v == 2:
        return "group_metadata"

    return "unknown"


def decode_member_proto(rdr):
    ret = {}
    ret['name'] = rdr.read_string()
    ret['metadata'] = rdr.read_iobuf().hex()
    return ret


def decode_member(rdr):
    ret = {}
    ret['v'] = rdr.read_int16()
    ret['member_id'] = rdr.read_kafka_string()
    ret['instance_id'] = rdr.read_kafka_optional_string()
    ret['client_id'] = rdr.read_kafka_string()
    ret['client_host'] = rdr.read_kafka_string()
    ret['rebalance_timeout'] = rdr.read_int32()
    ret['session_timeout'] = rdr.read_int32()
    ret['subscription'] = base64.b64encode(
        rdr.read_kafka_bytes()).decode('utf-8')
    ret['assignment'] = base64.b64encode(
        rdr.read_kafka_bytes()).decode('utf-8')

    return ret


def decode_metadata(rdr):
    ret = {}
    ret['version'] = rdr.read_int16()
    ret['protocol_type'] = rdr.read_kafka_string()
    ret['generation_id'] = rdr.read_int32()
    ret['protocol_name'] = rdr.read_kafka_optional_string()
    ret['leader'] = rdr.read_kafka_optional_string()
    ret['state_timestamp'] = rdr.read_int64()
    ret['member_state'] = rdr.read_vector(decode_member)
    return ret


def decode_key(key_rdr):
    ret = {}
    v = key_rdr.read_int16()
    ret['type'] = decode_key_type(v)
    ret['group_id'] = key_rdr.read_kafka_string()
    if ret['type'] == 'offset_commit':
        ret['topic'] = key_rdr.read_kafka_string()
        ret['partition'] = key_rdr.read_int32()

    return ret


def decode_offset_commit(v_rdr):
    ret = {}
    ret['version'] = v_rdr.read_int16()
    ret['committed_offset'] = v_rdr.read_int64()
    if ret['version'] >= 3:
        ret['leader_epoch'] = v_rdr.read_int32()

    ret['committed_metadata'] = v_rdr.read_kafka_string()
    ret['commit_timestamp'] = v_rdr.read_int64()
    if ret['version'] == 1:
        ret['expiry_timestamp'] = v_rdr.read_int64()

    return ret


def decode_record(hdr, r):
    v = {}
    v['epoch'] = hdr.first_ts
    v['offset'] = hdr.base_offset + r.offset_delta
    v['ts'] = datetime.datetime.utcfromtimestamp(
        hdr.first_ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    k_rdr = Reader(BytesIO(r.key), endianness=Endianness.BIG_ENDIAN)

    v['key'] = decode_key(k_rdr)

    if v['key']['type'] == "group_metadata":
        if r.value:
            rdr = Reader(BytesIO(r.value), endianness=Endianness.BIG_ENDIAN)
            v['value'] = decode_metadata(rdr)
        else:
            v['value'] = 'tombstone'
    elif v['key']['type'] == "offset_commit":
        if r.value:
            rdr = Reader(BytesIO(r.value), endianness=Endianness.BIG_ENDIAN)
            v['value'] = decode_offset_commit(rdr)
        else:
            v['value'] = 'tombstone'

    return v


class OffsetsLog:
    def __init__(self, ntp):
        self.ntp = ntp
        self.records = []

    def decode(self):
        paths = []
        for path in self.ntp.segments:
            paths.append(path)
        paths.sort()
        for path in paths:
            s = Segment(path)
            for b in s:
                if b.header.type != 1:
                    continue
                for r in b:
                    self.records.append(decode_record(b.header, r))
