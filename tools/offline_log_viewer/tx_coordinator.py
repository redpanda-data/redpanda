from storage import Segment
from enum import Enum
from io import BytesIO
from reader import Reader
import struct


class TxStatus(Enum):
    ongoing = 0
    preparing = 1
    prepared = 2
    aborting = 3
    killed = 4
    ready = 5
    tombstone = 6


class TxLog:
    def __init__(self, ntp):
        self.ntp = ntp

    def decode(self):
        for batch in self.batches():
            header = batch.header_dict()
            records = batch
            if header["type_name"] == "tm_update":
                assert len(records) == 1
            header["records"] = []
            for record in records:
                record_dict = record.kv_dict()
                if header["type_name"] == "tm_update":
                    rdr = Reader(BytesIO(record.key))
                    record_dict["key"] = {}
                    record_dict["key"]["raw"] = record_dict["k"]
                    del record_dict["k"]
                    record_dict["key"]["type"] = rdr.read_int8()
                    record_dict["key"]["pid.id"] = rdr.read_int64()
                    record_dict["key"]["tx.id"] = rdr.read_string()

                    rdr = Reader(BytesIO(record.value))
                    record_dict["value"] = {}
                    record_dict["value"]["raw"] = record_dict["v"]
                    del record_dict["v"]
                    record_dict["value"]["version"] = rdr.read_int8()
                    record_dict["value"]["tx.id"] = rdr.read_string()
                    record_dict["value"]["pid"] = {}
                    record_dict["value"]["pid"]["id"] = rdr.read_int64()
                    record_dict["value"]["pid"]["epoch"] = rdr.read_int16()
                    record_dict["value"]["tx_seq"] = rdr.read_int64()
                    record_dict["value"]["etag"] = rdr.read_int64()
                    record_dict["value"]["status"] = TxStatus(
                        rdr.read_int32()).name
                    record_dict["value"]["timeout_ms"] = rdr.read_int64()
                    record_dict["value"]["last_update_ts"] = rdr.read_int64()
                    record_dict["value"]["partitions"] = []
                    record_dict["value"]["groups"] = []
                    p_size = rdr.read_int32()
                    for i in range(p_size):
                        partition = {}
                        partition["ntp"] = {}
                        partition["ntp"]["ns"] = rdr.read_string()
                        partition["ntp"]["tp"] = {}
                        partition["ntp"]["tp"]["topic"] = rdr.read_string()
                        partition["ntp"]["tp"]["partition"] = rdr.read_int32()
                        partition["etag"] = rdr.read_int64()
                        record_dict["value"]["partitions"].append(partition)

                    g_size = rdr.read_int32()
                    for i in range(g_size):
                        group = {}
                        group["id"] = rdr.read_string()
                        group["etag"] = rdr.read_int64()
                        record_dict["value"]["groups"].append(group)
                elif header["type_name"] == "raft_configuration":
                    pass
                elif header["type_name"] == "checkpoint":
                    pass
                else:
                    raise Exception(header["type_name"])
                header["records"].append(record_dict)
                yield header

    def batches(self):
        for path in self.ntp.segments:
            s = Segment(path)
            for batch in s:
                yield batch
