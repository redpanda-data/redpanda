from io import BufferedReader, BytesIO
from model import *
from reader import Reader
from storage import Batch, Segment
from storage import BatchType
import datetime


def decode_topic_command(record):
    rdr = Reader(BufferedReader(BytesIO(record.value)))
    k_rdr = Reader(BytesIO(record.key))
    cmd = {}
    cmd['type'] = rdr.read_int8()
    if cmd['type'] == 0:
        cmd['type_string'] = 'create_topic'
        version = Reader(BytesIO(rdr.peek(4))).read_int32()
        if version < 0:
            assert version == -1
            rdr.skip(4)
        else:
            version = 0
        cmd['namespace'] = rdr.read_string()
        cmd['topic'] = rdr.read_string()
        cmd['partitions'] = rdr.read_int32()
        cmd['replication_factor'] = rdr.read_int16()
        cmd['compression'] = rdr.read_optional(lambda r: r.read_int8())
        cmd['cleanup_policy_bitflags'] = rdr.read_optional(
            lambda r: decode_cleanup_policy(r.read_int8()))
        cmd['compaction_strategy'] = rdr.read_optional(lambda r: r.read_int8())
        cmd['timestamp_type'] = rdr.read_optional(lambda r: r.read_int8())
        cmd['segment_size'] = rdr.read_optional(lambda r: r.read_int64())
        cmd['retention_bytes'] = rdr.read_tristate(lambda r: r.read_int64())
        cmd['retention_duration'] = rdr.read_tristate(lambda r: r.read_int64())
        if version == -1:
            cmd["recovery"] = rdr.read_optional(lambda r: r.read_bool())
            cmd["shadow_indexing"] = rdr.read_optional(lambda r: r.read_int8())
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
    elif cmd['type'] == 5:
        cmd['type_string'] = 'create_partitions'
        cmd['namespace'] = rdr.read_string()
        cmd['topic'] = rdr.read_string()
        cmd['new_total_partitions'] = rdr.read_int32()
        cmd['custom_assignments'] = rdr.read_vector(lambda r: r.read_int32())
        cmd['assignments'] = rdr.read_vector(read_partition_assignment)

    return cmd


def decode_config(record):
    rdr = Reader(BytesIO(record.value))
    return read_raft_config(rdr)


def decode_user_command(record):
    rdr = Reader(BytesIO(record.value))
    k_rdr = Reader(BytesIO(record.key))
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
        cmd['cred']['server_key'] = obfuscate_secret(cmd['cred']['server_key'])
        cmd['cred']['stored_key'] = obfuscate_secret(cmd['cred']['stored_key'])

    elif cmd['type'] == 6:
        cmd['user'] = k_rdr.read_string()

    return cmd


def decode_acl_command(record):
    rdr = Reader(BytesIO(record.value))
    k_rdr = Reader(BytesIO(record.key))
    cmd = {}
    cmd['type'] = rdr.read_int8()
    cmd['str_type'] = decode_acls_cmd_type(cmd['type'])
    if cmd['type'] == 8:
        cmd['version'] = k_rdr.read_int8()
        cmd['acls'] = k_rdr.read_vector(read_acl)
    elif cmd['type'] == 9:
        cmd['version'] = k_rdr.read_int8()

    return cmd


def read_config_kv(reader):

    k = reader.read_string()
    v = reader.read_string()
    return (k, v)


def decode_config_command(record):
    rdr = Reader(BytesIO(record.value))
    k_rdr = Reader(BytesIO(record.key))
    cmd = {}
    cmd['type'] = rdr.read_int8()
    if cmd['type'] == 0:
        cmd['type_name'] = 'config_delta'
        cmd['version'] = k_rdr.read_int64()
        cmd['cmd_version'] = rdr.read_int8()
        cmd['upsert'] = rdr.read_vector(read_config_kv)
        cmd['remove'] = rdr.read_vector(lambda r: r.read_string())
    elif cmd['type'] == 1:
        cmd['type_name'] = 'config_status'
        cmd['node_id'] = k_rdr.read_int32()
        cmd['cmd_version'] = rdr.read_int8()
        cmd['status_node_id'] = rdr.read_int32()
        cmd['cfg_version'] = rdr.read_int64()
        cmd['restart'] = rdr.read_bool()
        cmd['unknown'] = rdr.read_vector(lambda r: r.read_string())
        cmd['invalid'] = rdr.read_vector(lambda r: r.read_string())
    else:
        cmd['type_name'] = 'unknown'
    return cmd


def decode_feature_command(record):
    def decode_feature_update_action(r):
        action = {}
        action['v'] = r.read_int8()
        action['feature_name'] = r.read_string()
        action['action'] = r.read_int16()
        return action

    rdr = Reader(BytesIO(record.value))
    k_rdr = Reader(BytesIO(record.key))
    cmd = {}
    cmd['type'] = rdr.read_int8()
    if cmd['type'] == 0:
        cmd['type_name'] = 'feature_update'
        cmd['v'] = k_rdr.read_int8()
        cmd['cluster_version'] = k_rdr.read_int64()
        cmd['actions'] = k_rdr.read_vector(decode_feature_update_action)
    else:
        cmd['type_name'] = 'unknown'
    return cmd


def decode_record(batch, record):
    ret = {}
    header = batch.header
    ret['type'] = batch.type.name
    ret['epoch'] = header.first_ts
    ret['offset'] = header.base_offset + record.offset_delta
    ret['ts'] = datetime.datetime.utcfromtimestamp(
        header.first_ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    ret['data'] = None

    if batch.type == BatchType.raft_configuration:
        ret['data'] = decode_config(record)
    if batch.type == BatchType.topic_management_cmd:
        ret['data'] = decode_topic_command(record)
    if batch.type == BatchType.user_management_cmd:
        ret['data'] = decode_user_command(record)
    if batch.type == BatchType.acl_management_cmd:
        ret['data'] = decode_acl_command(record)
    if header.type == BatchType.cluster_config_cmd:
        ret['data'] = decode_config_command(record)
    if header.type == BatchType.feature_update:
        ret['data'] = decode_feature_command(record)
    return ret


class ControllerLog:
    def __init__(self, ntp):
        self.ntp = ntp
        self.records = []

    def decode(self):
        for path in self.ntp.segments:
            s = Segment(path)
            for b in s:
                for r in b:
                    self.records.append(decode_record(b, r))
