from io import BytesIO
import logging
import os
import struct
from model import *
from reader import Reader
from storage import BatchType, Header, Record, Segment
import collections
import datetime

logger = logging.getLogger('kvstore')


class SnapshotBatch:
    def __init__(self, header, records):
        self.header = header
        self.records = records
        self.type = BatchType(header[3])

    def __iter__(self):
        for r in self.records:
            yield r

    @staticmethod
    def from_stream(f):
        rdr = Reader(f)
        h_crc = rdr.read_uint32()
        h_sz = rdr.read_int32()
        h_bo = rdr.read_int64()
        h_tp = rdr.read_int8()
        h_batch_crc = rdr.read_int32()
        h_attrs = rdr.read_int16()
        h_lod = rdr.read_int32()
        first_ts = rdr.read_int64()
        last_ts = rdr.read_int64()
        producer_id = rdr.read_int64()
        producer_epoch = rdr.read_int16()
        base_seq = rdr.read_int32()
        record_cnt = rdr.read_int32()
        term = rdr.read_int64()
        compressed = rdr.read_int8()

        header = Header(h_crc, h_sz, h_bo, h_tp, h_batch_crc, h_attrs, h_lod,
                        first_ts, last_ts, producer_id, producer_epoch,
                        base_seq, record_cnt)

        records = []

        for i in range(0, header.record_count):
            sz = rdr.read_uint32()
            attr = rdr.read_int8()
            ts = rdr.read_int64()
            o_delta = rdr.read_int32()
            rdr.read_int32()
            key = rdr.read_iobuf()
            rdr.read_int32()
            v = rdr.read_iobuf()
            rdr.read_int32()
            records.append(Record(sz, attr, ts, o_delta, key, v, []))

        return SnapshotBatch(header, records)


class KvStoreRecordDecoder:
    def __init__(self, record, batch, value_is_optional_type):
        self.record = record
        self.header = batch.header
        self.batch_type = batch.type
        self.offset_delta = record.offset_delta
        self.v_stream = BytesIO(self.record.value)
        self.k_stream = BytesIO(self.record.key)
        self.value_is_optional_type = value_is_optional_type

    def _decode_ks(self, ks):
        if ks == 0:
            return "testing"
        elif ks == 1:
            return "consensus"
        elif ks == 2:
            return "storage"
        elif ks == 3:
            return "cluster"
        elif ks == 4:
            return "offset_translator"
        elif ks == 5:
            return "usage"
        return "unknown"

    def decode(self):

        assert self.batch_type == BatchType.kvstore
        ret = {}
        ret['epoch'] = self.header.first_ts
        ret['offset'] = self.header.base_offset + self.offset_delta
        ret['ts'] = datetime.datetime.utcfromtimestamp(
            self.header.first_ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

        k_rdr = Reader(self.k_stream)

        keyspace = k_rdr.read_int8()

        key_buf = k_rdr.stream.read()

        ret['key_space'] = self._decode_ks(keyspace)
        ret['key_buf'] = key_buf
        if self.value_is_optional_type:
            data_rdr = Reader(self.v_stream)
            data = data_rdr.read_optional(lambda r: r.read_iobuf())
        else:
            data = self.record.value
        if data:
            ret['data'] = data
        else:
            ret['data'] = None

        return ret


def decode_raft_metadata_type(k):
    if k == 0:
        return "voted_for"
    elif k == 1:
        return "config_map"
    elif k == 2:
        return "config_latest_known_offset"
    elif k == 3:
        return "last_applied_offset"
    elif k == 4:
        return "unique_local_id"
    elif k == 5:
        return "config_next_cfg_idx"
    return "unknown"


SNAP_HDR_FMT = "<IIbi"
SNAP_HDR_SIZE = struct.calcsize(SNAP_HDR_FMT)

SnapshotHeader = collections.namedtuple(
    'SnapshotHeader',
    ('header_crc', 'metadata_crc', 'version', 'metadata_size'))


class Snapshot():
    def __init__(self, snapshot_file_path):
        self.path = snapshot_file_path
        self.header = None
        self.meta = None
        self.data = None

    def read(self):
        with open(self.path, "rb") as f:
            data = f.read(SNAP_HDR_SIZE)
            if len(data) == SNAP_HDR_SIZE:
                self.header = SnapshotHeader(
                    *struct.unpack(SNAP_HDR_FMT, data))
            if self.header:
                self.meta = f.read(self.header.metadata_size)
                self.data = f.read()


class KvSnapshot:
    def __init__(self, path):
        logger.info(f"reading snapshot from {path}")
        self.snap = Snapshot(path)
        self.snap.read()
        self.last_offset = None
        self.data_batch = None

    def decode(self):
        self.last_offset = Reader(BytesIO(self.snap.meta)).read_int64()
        data_str = BytesIO(self.snap.data)
        rdr = Reader(data_str)
        data_sz = rdr.read_int32()
        self.data_batch = SnapshotBatch.from_stream(
            f=BytesIO(rdr.read_bytes(data_sz)))


def read_vnode(rdr):
    ret = {}
    ret['id'] = rdr.read_int32()
    ret['revision'] = rdr.read_int64()
    return ret


def read_configurations_map(rdr):
    ret = {}
    sz = rdr.read_uint64()
    for _ in range(0, sz):
        offset = rdr.read_int64()
        cfg = read_raft_config(rdr)
        ret[offset] = cfg

    return ret


def decode_raft_key(k):
    rdr = Reader(BytesIO(k))
    ret = {}
    ret['type'] = rdr.read_int8()
    ret['name'] = decode_raft_meta_key(ret['type'])
    ret['group'] = rdr.read_int64()
    return ret


def decode_offset_translator_key(k):
    rdr = Reader(BytesIO(k))
    ret = {}
    ret['type'] = rdr.read_int8()
    if ret['type'] == 0:
        ret['name'] = "offset_map"
    else:
        ret['name'] = 'highest_known_offset'

    ret['group'] = rdr.read_int64()
    return ret


def decode_storage_key_name(key_type):
    if key_type == 0:
        return "start offset"
    elif key_type == 1:
        return "clean segment"

    return "unknown"


def decode_storage_key(k):
    rdr = Reader(BytesIO(k))
    ret = {}
    ret['type'] = rdr.read_int8()
    ret['name'] = decode_storage_key_name(ret['type'])
    ret['ntp'] = read_ntp(rdr)
    return ret


def decode_key(ks, key):
    data = key
    if ks == "consensus":
        data = decode_raft_key(key)
    elif ks == "storage":
        data = decode_storage_key(key)
    elif ks == "offset_translator":
        data = decode_offset_translator_key(key)
    else:
        data = key.hex()
    return {'keyspace': ks, 'data': data}


def decode_raft_meta_key(type):
    if type == 0:
        return "voted for"
    elif type == 1:
        return "configuration map"
    elif type == 2:
        return "last known config offset"
    elif type == 3:
        return "last applied offset"
    elif type == 4:
        return "local id"
    elif type == 5:
        return "next cfg idx"
    return "unknown"


def decode_storage_value(type, v):
    rdr = Reader(BytesIO(v))
    ret = {}
    if type == 0:  # start offset
        return rdr.read_int64()
    return ret


def decode_offset_translator_value(type, v):
    rdr = Reader(BytesIO(v))
    ret = {}

    def read_peristed_batch(rdr):
        ret = {}
        ret['base_offset'] = rdr.read_int64()
        ret['length'] = rdr.read_int32()
        return ret

    if type == 1:
        ret['offset'] = rdr.read_int64()
    else:
        rdr.read_envelope()
        ret['start_delta'] = rdr.read_int64()
        ret['persisted_batches'] = rdr.read_serde_vector(read_peristed_batch)
    return ret


def decode_value(dk, v):
    if dk['keyspace'] == 'consensus':
        return decode_raft_value(dk['data']['type'], v)
    elif dk['keyspace'] == 'storage':
        return decode_storage_value(dk['data']['type'], v)
    elif dk['keyspace'] == 'offset_translator':
        return decode_offset_translator_value(dk['data']['type'], v)
    return v.hex()


def decode_raft_value(type, v):
    rdr = Reader(BytesIO(v))

    if type == 0:  # voted for
        ret = {}
        ret['vnode'] = read_vnode(rdr)
        ret['term'] = rdr.read_int64()
        return ret
    elif type == 1:  # config map
        return read_configurations_map(rdr)
    elif type == 2:  # config_latest_known_offset
        return rdr.read_int64()
    elif type == 3:  # last_applied_offset
        return rdr.read_int64()
    elif type == 4:  # unique_local_id
        return None
    elif type == 5:  # config_next_cfg_idx
        return rdr.read_int64()

    return None


class KvStore:
    def __init__(self, ntp):
        logger.info(f"building kvstore on path: {ntp.path}")
        self.ntp = ntp
        self.kv = {}

    def _apply(self, entry):
        key = (entry['key_space'], entry['key_buf'])
        logger.debug(f"applying {entry}")

        if entry['data'] is not None:
            self.kv[key] = entry['data']
        else:
            try:
                del self.kv[key]
            except KeyError:
                # Missing key, that's okay for a deletion
                pass

    def decode(self):
        snapshot_offset = None
        if os.path.exists(f"{self.ntp.path}/snapshot"):
            snap = KvSnapshot(f"{self.ntp.path}/snapshot")
            snap.decode()
            logger.info(f"snapshot last offset: {snap.last_offset}")
            snapshot_offset = snap.last_offset
            for r in snap.data_batch:
                d = KvStoreRecordDecoder(r,
                                         snap.data_batch,
                                         value_is_optional_type=False)
                self._apply(d.decode())
        else:
            logger.info(f"{self.ntp.path}/snapshot does not exist")

        for path in self.ntp.segments:
            s = Segment(path)
            for batch in s:
                for r in batch:
                    offset = batch.header.base_offset + r.offset_delta
                    if snapshot_offset is not None and offset <= snapshot_offset:
                        continue

                    d = KvStoreRecordDecoder(r,
                                             batch,
                                             value_is_optional_type=True)
                    self._apply(d.decode())

    def items(self):
        ret = []
        for k, v in self.kv.items():
            dk = decode_key(k[0], k[1])
            dv = decode_value(dk, v)
            ret.append({'key': dk, 'value': dv})
        return ret
