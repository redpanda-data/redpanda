from io import BytesIO
from model import *
from reader import Reader
from storage import Segment
import datetime


def decode_key_type(kt):
    if kt == 0:
        return "group_metadata"
    elif kt == 1:
        return "offset_commit"
    elif kt == 2:
        return "noop"
    return "unknown"


def decode_member_proto(rdr):
    ret = {}
    ret['name'] = rdr.read_string()
    ret['metadata'] = rdr.read_iobuf().hex()
    return ret


def decode_member(rdr):
    ret = {}
    ret['member_id'] = rdr.read_string()
    ret['session_timeout'] = rdr.read_int64()
    ret['rebalance_timeout'] = rdr.read_int64()
    ret['instance_id'] = rdr.read_optional(lambda r: r.read_string())
    ret['protocol_type'] = rdr.read_string()
    ret['protocols'] = rdr.read_vector(decode_member_proto)
    ret['assignment'] = rdr.read_iobuf().hex()

    return ret


def decode_metadata(rdr):
    ret = {}
    ret['protocol_type'] = rdr.read_string()
    ret['generation_id'] = rdr.read_int32()
    ret['protocol_name'] = rdr.read_optional(lambda r: r.read_string())
    ret['leader'] = rdr.read_optional(lambda r: r.read_string())
    ret['state_timestamp'] = rdr.read_int32()
    ret['members'] = rdr.read_vector(decode_member)
    return ret


def decode_metadata_key(key_rdr):
    ret = {}
    key_buf = key_rdr.read_iobuf()
    rdr = Reader(BytesIO(key_buf))
    ret['id'] = rdr.read_string()
    return ret


def decode_offset_commit_key(key_rdr):
    ret = {}
    key_buf = key_rdr.read_iobuf()
    rdr = Reader(BytesIO(key_buf))
    ret['id'] = rdr.read_string()
    ret['topic'] = rdr.read_string()
    ret['partition'] = rdr.read_int32()
    return ret


def decode_offset_commit(v_rdr):
    ret = {}
    ret['committed_offset'] = v_rdr.read_int64()
    ret['leader_epoch'] = v_rdr.read_int32()
    ret['committed_metadata'] = v_rdr.read_optional(lambda r: r.read_string())
    return ret


def decode_record(hdr, r):
    v = {}
    v['epoch'] = hdr.first_ts
    v['offset'] = hdr.base_offset + r.offset_delta
    v['ts'] = datetime.datetime.utcfromtimestamp(
        hdr.first_ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    k_rdr = Reader(BytesIO(r.key))

    v['type'] = decode_key_type(k_rdr.read_int8())

    if v['type'] == "group_metadata":
        v["key"] = decode_metadata_key(k_rdr)
        if r.value:
            rdr = Reader(BytesIO(r.value))
            v['value'] = decode_metadata(rdr)
        else:
            v['value'] = 'tombstone'
    elif v['type'] == "offset_commit":
        v["key"] = decode_offset_commit_key(k_rdr)
        if r.value:
            rdr = Reader(BytesIO(r.value))
            v['value'] = decode_offset_commit(rdr)
        else:
            v['value'] = 'tombstone'

    return v


class GroupsLog:
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
