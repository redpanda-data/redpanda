from storage import Segment
from enum import Enum
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


def decode_record(batch, header, record):
    attrs = header["expanded_attrs"]
    record_dict = record.kv_dict()

    is_txn = attrs["transactional"]
    is_ctrl = attrs["control_batch"]
    is_tx_ctrl = is_txn and is_ctrl
    if is_tx_ctrl:
        record_dict["type"] = self.get_control_record_type(record.key)

    return record_dict


class KafkaLog:
    def __init__(self, ntp, headers_only):
        self.ntp = ntp
        self.headers_only = headers_only

    def get_control_record_type(self, key):
        rdr = Reader(BytesIO(key))
        rdr.skip(2)  # skip the 16bit version.
        # Encoded as big endian
        type_rdr = Reader(BytesIO(struct.pack(">h", rdr.read_int16())))
        return KafkaControlRecordType(type_rdr.read_int16()).name

    def decode(self):
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
