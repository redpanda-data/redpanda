from storage import Segment, BatchType
from enum import Enum, IntEnum
from io import BytesIO
from reader import Reader
import struct


class KafkaControlRecordType(Enum):
    """Valid control record types defined by Kafka, currently only
    used for transactional control records.
    Keep this in sync with model::control_record_type
    """
    tx_abort = 0
    tx_commit = 1
    unknown = -1


class ArchivalMetadataCommand(IntEnum):
    add_segment = 0
    cleanup_metadata = 3
    mark_clean = 4
    replace_manifest = 10


def read_cloud_storage_segment_meta(rdr):
    def spec(rdr, version):
        assert version >= 2, f"Version {version} < 2 unsupported"
        o = {}
        o["is_compacted"] = rdr.read_bool()
        o["size_bytes"] = rdr.read_int64()
        o["base_offset"] = rdr.read_int64()
        o["committed_offset"] = rdr.read_int64()
        o["base_timestamp"] = rdr.read_int64()
        o["max_timestamp"] = rdr.read_int64()
        o["delta_offset"] = rdr.read_int64()
        o["ntp_revision"] = rdr.read_int64()
        o["archiver_term"] = rdr.read_int64()
        o["segment_term"] = rdr.read_int64()
        o["delta_offset_end"] = rdr.read_int64()
        o["sname_format"] = rdr.read_int16()
        if version >= 3:
            o["metadata_size_hint"] = rdr.read_int64()
        else:
            o["metadata_size_hint"] = 0
        return o

    return rdr.read_envelope(spec, max_version=3)


def get_control_record_type(key):
    rdr = Reader(BytesIO(key))
    rdr.skip(2)  # skip the 16bit version.
    # Encoded as big endian
    type_rdr = Reader(BytesIO(struct.pack(">h", rdr.read_int16())))
    return KafkaControlRecordType(type_rdr.read_int16()).name


def decode_archival_metadata_command(kr, vr):
    key = kr.read_int8()
    if key == ArchivalMetadataCommand.add_segment:

        def spec(rdr, version):
            o = {}
            o['ntp_revision_deprecated'] = rdr.read_int64(),
            o['name'] = rdr.read_string(),
            o['meta'] = read_cloud_storage_segment_meta(rdr),
            if version >= 1:
                o['is_validated'] = rdr.read_bool(),
            else:
                o['is_validated'] = False
            return o

        return vr.read_envelope(spec, max_version=1)

    elif key == ArchivalMetadataCommand.mark_clean:
        return dict(offset=vr.read_int64())

    elif key == ArchivalMetadataCommand.cleanup_metadata:
        return dict()

    elif key == ArchivalMetadataCommand.replace_manifest:
        return dict(size=len(vr.read_iobuf()))


def decode_record(batch, header, record):
    attrs = header["expanded_attrs"]
    record_dict = record.kv_dict()

    is_txn = attrs["transactional"]
    is_ctrl = attrs["control_batch"]
    is_tx_ctrl = is_txn and is_ctrl
    if is_tx_ctrl:
        record_dict["type"] = get_control_record_type(record.key)

    kr = Reader(BytesIO(record.key))
    vr = Reader(BytesIO(record.value))

    decoded = None
    if batch.type == BatchType.archival_metadata:
        decoded = decode_archival_metadata_command(kr, vr)

    if decoded is not None:
        return decoded

    return record_dict


class KafkaLog:
    def __init__(self, ntp, headers_only):
        self.ntp = ntp
        self.headers_only = headers_only

    def __iter__(self):
        self.results = []
        for batch in self.batches():
            header = batch.header_dict()
            yield header
            if not self.headers_only:
                for record in batch:
                    yield decode_record(batch, header, record)

    def batches(self):
        for path in self.ntp.segments:
            s = Segment(path)
            for batch in s:
                yield batch
