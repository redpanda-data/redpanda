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
                records = []
                for record in batch:
                    attrs = header["expanded_attrs"]
                    is_tx_ctrl = attrs["transactional"] and attrs[
                        "control_batch"]
                    record_dict = record.kv_dict()
                    records.append(record_dict)
                    if is_tx_ctrl:
                        record_dict["type"] = self.get_control_record_type(
                            record.key)
                    yield record_dict

    def batches(self):
        for path in self.ntp.segments:
            s = Segment(path)
            for batch in s:
                yield batch
