import logging
from io import BytesIO
from pathlib import Path
from model import *
from reader import Reader
from storage import Segment
from storage import BatchType
import datetime
import json

logger = logging.getLogger('controller')


def read_remote_topic_properties_serde(rdr: Reader):
    return rdr.read_envelope(
        lambda rdr, _: {
            "remote_revision": rdr.read_int64(),
            "remote_partition_count": rdr.read_int32(),
        })


def read_remote_label_serde(rdr: Reader):
    return rdr.read_envelope(lambda rdr, _: {"cluster_uuid": rdr.read_uuid()})


def read_topic_namespace(rdr: Reader):
    namespace = rdr.read_string()
    topic = rdr.read_string()
    return f"{namespace}/{topic}"


def read_leaders_preference(rdr: Reader):
    return rdr.read_envelope(
        lambda r, ver: {
            "type": r.read_serde_enum(),
            "racks": r.read_optional(lambda r: r.read_vector(Reader.read_string
                                                             )),
        })


def read_topic_properties_serde(rdr: Reader, version):
    topic_properties = {
        'compression': rdr.read_optional(Reader.read_serde_enum),
        'cleanup_policy_bitflags': rdr.read_optional(Reader.read_serde_enum),
        'compaction_strategy': rdr.read_optional(Reader.read_serde_enum),
        'timestamp_type': rdr.read_optional(Reader.read_serde_enum),
        'segment_size': rdr.read_optional(Reader.read_uint64),
        'retention_bytes': rdr.read_tristate(Reader.read_uint64),
        'retention_duration': rdr.read_tristate(Reader.read_uint64),
        'recovery': rdr.read_optional(Reader.read_bool),
        'shadow_indexing': rdr.read_optional(Reader.read_serde_enum),
    }

    # introduced for remote read replicas
    if version >= 1:
        topic_properties |= {
            'read_replica':
            rdr.read_optional(Reader.read_bool),
            'read_replica_bucket':
            rdr.read_optional(Reader.read_string),
            'remote_topic_properties':
            rdr.read_optional(read_remote_topic_properties_serde)
        }
    if version >= 2:
        topic_properties |= {
            'batch_max_bytes': rdr.read_optional(Reader.read_uint32),
        }

    if version >= 3:
        topic_properties |= {
            'retention_local_target_bytes':
            rdr.read_tristate(Reader.read_uint64),
            'retention_local_target_ms': rdr.read_tristate(Reader.read_uint64),
            'remote_delete': rdr.read_bool()
        }

    if version >= 4:
        topic_properties |= {
            'segment_ms': rdr.read_tristate(Reader.read_uint64)
        }
    if version >= 5:
        topic_properties |= {
            'record_key_schema_id_validation':
            rdr.read_optional(Reader.read_bool)
        }
        topic_properties |= {
            'record_key_schema_id_validation_compat':
            rdr.read_optional(Reader.read_bool)
        }
        topic_properties |= {
            'record_key_subject_name_strategy':
            rdr.read_optional(Reader.read_serde_enum)
        }
        topic_properties |= {
            'record_key_subject_name_strategy_compat':
            rdr.read_optional(Reader.read_serde_enum)
        }
        topic_properties |= {
            'record_value_schema_id_validation':
            rdr.read_optional(Reader.read_bool)
        }
        topic_properties |= {
            'record_value_schema_id_validation_compat':
            rdr.read_optional(Reader.read_bool)
        }
        topic_properties |= {
            'record_value_subject_name_strategy':
            rdr.read_optional(Reader.read_serde_enum)
        }
        topic_properties |= {
            'record_value_subject_name_strategy_compat':
            rdr.read_optional(Reader.read_serde_enum)
        }
    if version >= 6:
        topic_properties |= {
            'initial_retention_local_target_bytes':
            rdr.read_tristate(Reader.read_uint64),
            'initial_retention_local_target_ms':
            rdr.read_tristate(Reader.read_uint64)
        }
    if version >= 7:
        topic_properties |= {
            'mpx_virtual_cluster_id':
            rdr.read_optional(
                lambda rdr: rdr.read_serde_vector(Reader.read_uint8)),
        }

    if version >= 8:
        topic_properties |= {
            'write_caching': rdr.read_optional(Reader.read_serde_enum),
            'flush_ms': rdr.read_optional(Reader.read_int64),
            'flush_bytes': rdr.read_optional(Reader.read_int64)
        }
    if version >= 9:
        topic_properties |= {
            'remote_label': rdr.read_optional(read_remote_label_serde),
            'topic_namespace_override': rdr.read_optional(read_topic_namespace)
        }
    if version >= 10:
        topic_properties |= {
            'iceberg_enabled': rdr.read_bool(),
            'leaders_preference': rdr.read_optional(read_leaders_preference),
            'cloud_topic_enabled': rdr.read_bool(),
        }

    return topic_properties


def read_topic_config(rdr: Reader, version):
    decoded = {
        'namespace':
        rdr.read_string(),
        'topic':
        rdr.read_string(),
        'partitions':
        rdr.read_int32(),
        'replication_factor':
        rdr.read_int16(),
        'properties':
        rdr.read_envelope(read_topic_properties_serde, max_version=10),
    }
    if version < 1:
        # see https://github.com/redpanda-data/redpanda/pull/6613
        decoded['properties']['remote_delete'] = False
    decoded['is_migrated'] = rdr.read_bool() if version >= 2 else False

    return decoded


def read_topic_configuration_assignment_serde(rdr: Reader):
    return rdr.read_envelope(
        lambda rdr, _: {
            'cfg':
            rdr.read_envelope(read_topic_config, 2),
            'assignments':
            rdr.read_serde_vector(lambda r: r.read_envelope(
                lambda ir, _: {
                    'group':
                    ir.read_int64(),
                    'id':
                    ir.read_int32(),
                    'replicas':
                    ir.read_serde_vector(lambda iir: {
                        'node_id': iir.read_int32(),
                        'shard': iir.read_uint32(),
                    }),
                })),
        }, 1)


def read_inc_update_op_serde(rdr: Reader):
    v = rdr.read_serde_enum()
    if -1 < v < 3:
        return ['none', 'set', 'remove'][v]
    return 'error'


def read_property_update_serde(rdr: Reader, type_reader):
    return rdr.read_envelope(lambda rdr, _: {
        'value': type_reader(rdr),
        'op': read_inc_update_op_serde(rdr),
    })


def read_incremental_topic_update_serde(rdr: Reader):
    def incr_topic_upd(rdr: Reader, version):
        incr_obj = {
            'compression':
            read_property_update_serde(
                rdr, lambda r: r.read_optional(Reader.read_serde_enum)),
            'cleanup_policy_bitflags':
            read_property_update_serde(
                rdr, lambda r: r.read_optional(Reader.read_serde_enum)),
            'compaction_strategy':
            read_property_update_serde(
                rdr, lambda r: r.read_optional(Reader.read_serde_enum)),
            'timestamp_type':
            read_property_update_serde(
                rdr, lambda r: r.read_optional(Reader.read_serde_enum)),
            'segment_size':
            read_property_update_serde(
                rdr, lambda r: r.read_optional(Reader.read_uint64)),
            'retention_bytes':
            read_property_update_serde(
                rdr, lambda r: r.read_tristate(Reader.read_uint64)),
            'retention_duration':
            read_property_update_serde(
                rdr, lambda r: r.read_tristate(Reader.read_int64)),
            'shadow_indexing':
            read_property_update_serde(
                rdr, lambda r: r.read_optional(Reader.read_serde_enum)),
        }
        # there is no release that uses version 1
        if version >= 2:
            incr_obj |= {
                'batch_max_bytes':
                read_property_update_serde(
                    rdr, lambda r: r.read_optional(Reader.read_uint32)),
                'retention_local_target_bytes':
                read_property_update_serde(
                    rdr, lambda r: r.read_tristate(Reader.read_uint64)),
                'retention_local_target_ms':
                read_property_update_serde(
                    rdr, lambda r: r.read_tristate(Reader.read_uint64)),
                'remote_delete':
                read_property_update_serde(rdr, Reader.read_bool)
            }
        if version >= 3:
            incr_obj |= {
                'segment_ms':
                read_property_update_serde(
                    rdr, lambda r: r.read_tristate(Reader.read_uint64))
            }
        if version >= 4:
            incr_obj |= {
                'record_key_schema_id_validation':
                rdr.read_optional(Reader.read_bool)
            }
            incr_obj |= {
                'record_key_schema_id_validation_compat':
                rdr.read_optional(Reader.read_bool)
            }
            incr_obj |= {
                'record_key_subject_name_strategy':
                rdr.read_optional(Reader.read_serde_enum)
            }
            incr_obj |= {
                'record_key_subject_name_strategy_compat':
                rdr.read_optional(Reader.read_serde_enum)
            }
            incr_obj |= {
                'record_value_schema_id_validation':
                rdr.read_optional(Reader.read_bool)
            }
            incr_obj |= {
                'record_value_schema_id_validation_compat':
                rdr.read_optional(Reader.read_bool)
            }
            incr_obj |= {
                'record_value_subject_name_strategy':
                rdr.read_optional(Reader.read_serde_enum)
            }
            incr_obj |= {
                'record_value_subject_name_strategy_compat':
                rdr.read_optional(Reader.read_serde_enum)
            }
        if version >= 5:
            incr_obj |= {
                'initial_retention_local_target_bytes':
                read_property_update_serde(
                    rdr, lambda r: r.read_tristate(Reader.read_uint64)),
                'initial_retention_local_target_ms':
                read_property_update_serde(
                    rdr, lambda r: r.read_tristate(Reader.read_uint64))
            }
        if version >= 6:
            incr_obj |= {
                'write_caching': rdr.read_optional(Reader.read_serde_enum),
                'flush_ms': rdr.read_optional(Reader.read_int64),
                'flush_bytes': rdr.read_optional(Reader.read_int64)
            }
        if version >= 7:
            incr_obj |= {
                'iceberg_enabled': rdr.read_bool(),
                'leaders_preference':
                rdr.read_optional(read_leaders_preference),
            }

        return incr_obj

    return rdr.read_envelope(incr_topic_upd, max_version=7)


def read_create_partitions_serde(rdr: Reader):
    return rdr.read_envelope(
        lambda rdr, _: {
            'cfg':
            rdr.read_envelope(
                lambda rdr, _: {
                    'namespace':
                    rdr.read_string(),
                    'topic':
                    rdr.read_string(),
                    'new_total_partition_count':
                    rdr.read_int32(),
                    'custom_assignments':
                    rdr.read_serde_vector(lambda r: r.read_serde_vector(
                        Reader.read_int32)),
                }),
            'assignments':
            rdr.read_serde_vector(lambda r: r.read_envelope(
                lambda rdr, _: {
                    'group': rdr.read_int64(),
                    'id': rdr.read_int32(),
                    'replicas': rdr.read_serde_vector(read_broker_shard),
                })),
        })


def decode_topic_command_serde(k_rdr: Reader, rdr: Reader):
    cmd = {}
    cmd['type'] = rdr.read_int8()
    if cmd['type'] == 0:
        cmd['type_string'] = 'create_topic'
        cmd['key'] = {}
        cmd['key']['namespace'] = k_rdr.read_string()
        cmd['key']['topic'] = k_rdr.read_string()
        cmd |= read_topic_configuration_assignment_serde(rdr)
    elif cmd['type'] == 1:
        cmd['type_string'] = 'delete_topic'
        cmd['namespace'] = rdr.read_string()
        cmd['topic'] = rdr.read_string()
        k_rdr.read_string()
        k_rdr.read_string()
    elif cmd['type'] == 10:
        # This command replaces delete_topic in Redpanda 23.2
        cmd['type_string'] = 'topic_lifecycle_transition'
        cmd['namespace'] = k_rdr.read_string()
        cmd['topic'] = k_rdr.read_string()
        cmd['transition'] = rdr.read_envelope(
            lambda rdr, _v: {
                'nt':
                rdr.read_envelope(
                    lambda rdr, _v: {
                        'namespace': rdr.read_string(),
                        'topic': rdr.read_string(),
                        'initial_revision_id': rdr.read_int64()
                    }),
                'mode':
                rdr.read_serde_enum(),
            })
    elif cmd['type'] == 2:
        cmd['type_string'] = 'update_partitions'
        cmd['namespace'] = k_rdr.read_string()
        cmd['topic'] = k_rdr.read_string()
        cmd['partition'] = k_rdr.read_int32()
        cmd['replicas'] = rdr.read_serde_vector(read_broker_shard)
    elif cmd['type'] == 3:
        cmd['type_string'] = 'finish_partitions_update'
        cmd['namespace'] = k_rdr.read_string()
        cmd['topic'] = k_rdr.read_string()
        cmd['partition'] = k_rdr.read_int32()
        cmd['replicas'] = rdr.read_serde_vector(read_broker_shard)
    elif cmd['type'] == 4:
        cmd['type_string'] = 'update_topic_properties'
        cmd['namespace'] = k_rdr.read_string()
        cmd['topic'] = k_rdr.read_string()
        cmd['update'] = read_incremental_topic_update_serde(rdr)
    elif cmd['type'] == 5:
        # consume k_rdr to prevent error messages
        k_rdr.read_string()  # ns
        k_rdr.read_string()  # topic
        cmd['type_string'] = 'create_partitions'
        cmd |= read_create_partitions_serde(rdr)
    elif cmd['type'] == 6:
        cmd['type_string'] = 'create_non_replicable_topic'
        cmd['topic'] = k_rdr.read_envelope(
            lambda k_rdr, _: {
                'source': {
                    'namespace': k_rdr.read_string(),
                    'topic': k_rdr.read_string(),
                },
                'name': {
                    'namespace': k_rdr.read_string(),
                    'topic': k_rdr.read_string(),
                },
            })
    elif cmd['type'] == 7:
        cmd['type_string'] = 'cancel_moving_partition_replicas'
        cmd['namespace'] = k_rdr.read_string()
        cmd['topic'] = k_rdr.read_string()
        cmd['partition'] = k_rdr.read_int32()
        cmd |= rdr.read_envelope(lambda rdr, _: {'force': rdr.read_bool()})
    elif cmd['type'] == 11:
        cmd['type_string'] = 'force_partition_reconfiguration'
        cmd['namespace'] = k_rdr.read_string()
        cmd['topic'] = k_rdr.read_string()
        cmd['partition'] = k_rdr.read_int32()
        cmd |= rdr.read_envelope(
            lambda rdr, _:
            {'replicas': rdr.read_serde_vector(read_broker_shard)})
    return cmd


def decode_topic_command_adl(k_rdr: Reader, rdr: Reader):
    cmd = {}
    cmd['type'] = rdr.read_int8()
    if cmd['type'] == 0:
        # consume k_rdr to prevent error messages
        k_rdr.read_string()  # ns
        k_rdr.read_string()  # topic
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
        # consume k_rdr to prevent error messages
        k_rdr.read_string()  # ns
        k_rdr.read_string()  # topic
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
    elif cmd['type'] == 6:
        cmd['type_string'] = 'create_non_replicable_topic'
        cmd['topic'] = {
            'version': k_rdr.read_int8(),
            'source': {
                'namespace': k_rdr.read_string(),
                'topic': k_rdr.read_string(),
            },
            'name': {
                'namespace': k_rdr.read_string(),
                'topic': k_rdr.read_string(),
            },
        }
    elif cmd['type'] == 7:
        cmd['type_string'] = 'cancel_moving_partition_replicas'
        cmd['namespace'] = k_rdr.read_string()
        cmd['topic'] = k_rdr.read_string()
        cmd['partition'] = k_rdr.read_int32()
        cmd['force'] = rdr.read_bool()

    return cmd


def decode_topic_command(record):
    rdr = Reader(BytesIO(record.value))
    k_rdr = Reader(BytesIO(record.key))
    either_ald_or_serde = rdr.peek_int8()
    assert either_ald_or_serde >= -1, "unsupported serialization format"
    if either_ald_or_serde == -1:
        # serde encoding flag, consume it and proceed
        rdr.skip(1)
        return decode_topic_command_serde(k_rdr, rdr)
    else:
        return decode_topic_command_adl(k_rdr, rdr)


def decode_config(k_rdr: Reader, rdr: Reader):
    return read_raft_config(rdr)


def decode_user_command_serde(k_rdr: Reader, rdr: Reader):
    cmd = {'type': rdr.read_int8()}
    cmd['str_type'] = decode_user_cmd_type(cmd['type'])

    if cmd['type'] == 5 or cmd['type'] == 7:
        cmd['user'] = k_rdr.read_string()
        cmd['cred'] = rdr.read_envelope(
            lambda rdr, _: {
                # obfuscate secrets
                'salt': obfuscate_secret(rdr.read_iobuf().hex()),
                'server_key': obfuscate_secret(rdr.read_iobuf().hex()),
                'stored_key': obfuscate_secret(rdr.read_iobuf().hex()),
                'iterations': rdr.read_int32(),
            })
    elif cmd['type'] == 6:
        # skip one byte, unused field
        rdr.read_int8()
        cmd['user'] = k_rdr.read_string()

    return cmd


def decode_user_command_adl(k_rdr: Reader, rdr: Reader):
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


def read_acl_binding_serde(k_rdr: Reader):
    def read_pattern(rdr: Reader, version: int):
        r = {
            'resource': decode_acl_resource(rdr.read_serde_enum()),
            'name': rdr.read_string(),
            'pattern': decode_acl_pattern_type(rdr.read_serde_enum())
        }
        return r

    def read_principal(rdr: Reader, version: int):

        r = {'type': decode_acl_principal_type(rdr.read_serde_enum())}
        r |= {'name': rdr.read_string()}
        return r

    def read_host(rdr: Reader, version: int):
        return {
            'addr':
            rdr.read_optional(lambda k_rdr: {
                'ipv4': k_rdr.read_bool(),
                'data': k_rdr.read_iobuf().hex()
            })
        }

    def read_entry(rdr: Reader, version: int):
        return {
            'principal': rdr.read_envelope(read_principal),
            'host': rdr.read_envelope(read_host),
            'operation': decode_acl_operation(rdr.read_serde_enum()),
            'permission': decode_acl_permission(rdr.read_serde_enum()),
        }

    def do_read_binding(rdr: Reader, version: int):
        return {
            'pattern': rdr.read_envelope(read_pattern),
            'entry': rdr.read_envelope(read_entry),
        }

    return k_rdr.read_envelope(do_read_binding)


def decode_serialized_pattern_type(v):
    if 0 <= v <= 2:
        return ['literal', 'prefixed', 'match'][v]
    return 'error'


def read_acl_binding_filter_serde(k_rdr: Reader):
    # pattern class does not really use serde
    return k_rdr.read_envelope(
        lambda k_rdr, _: {
            'pattern': {
                'resource':
                k_rdr.read_optional(lambda k_rdr: decode_acl_resource(
                    k_rdr.read_serde_enum())),
                'name':
                k_rdr.read_optional(Reader.read_string),
                'pattern':
                k_rdr.read_optional(lambda k_rdr:
                                    decode_serialized_pattern_type(
                                        k_rdr.read_serde_enum())),
            },
            'acl':
            k_rdr.read_envelope(
                lambda k_rdr, _: {
                    'principal':
                    k_rdr.read_optional(lambda k_rdr: k_rdr.read_envelope(
                        lambda k_rdr, _: {
                            'type':
                            decode_acl_principal_type(k_rdr.read_serde_enum()),
                            'name':
                            k_rdr.read_string()
                        })),
                    'host':
                    k_rdr.read_optional(lambda k_rdr: k_rdr.read_envelope(
                        lambda k_rdr, _: {
                            'addr':
                            k_rdr.read_optional(
                                lambda k_rdr: {
                                    'ipv4': k_rdr.read_bool(),
                                    'data': k_rdr.read_iobuf().hex()
                                })
                        })),
                    'operation':
                    k_rdr.read_optional(lambda k_rdr: decode_acl_operation(
                        k_rdr.read_serde_enum())),
                    'permission':
                    k_rdr.read_optional(lambda k_rdr: decode_acl_permission(
                        k_rdr.read_serde_enum())),
                }),
        })


def decode_acl_command_serde(k_rdr: Reader, rdr: Reader):
    cmd = {}
    cmd['type'] = rdr.read_int8()
    # skip one byte, unused field
    rdr.read_int8()
    cmd['str_type'] = decode_acls_cmd_type(cmd['type'])
    if cmd['type'] == 8:
        cmd['acls'] = k_rdr.read_envelope(
            lambda k_rdr, _:
            {'bindings': k_rdr.read_serde_vector(read_acl_binding_serde)})
    elif cmd['type'] == 9:
        cmd |= k_rdr.read_envelope(lambda k_rdr, _: {
            'filters':
            k_rdr.read_serde_vector(read_acl_binding_filter_serde)
        })

    return cmd


def decode_acl_command_adl(k_rdr: Reader, rdr: Reader):
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


def decode_config_command_serde(k_rdr: Reader, rdr: Reader):
    cmd = {}
    cmd['type'] = rdr.read_int8()
    if cmd['type'] == 0:
        cmd['type_name'] = 'config_delta'
        cmd['version'] = k_rdr.read_int64()
        cmd |= rdr.read_envelope(
            lambda rdr, _: {
                'upsert':
                rdr.read_serde_vector(lambda rdr: rdr.read_envelope(
                    lambda rdr, _: {
                        'k': rdr.read_string(),
                        'v': rdr.read_string()
                    })),
                'remove':
                rdr.read_serde_vector(Reader.read_string),
            })
    elif cmd['type'] == 1:
        cmd['type_name'] = 'config_status'
        cmd['node_id'] = k_rdr.read_int32()
        cmd |= rdr.read_envelope(
            lambda rdr, _: {
                'status':
                rdr.read_envelope(
                    lambda rdr, _: {
                        'node': rdr.read_int32(),
                        'version': rdr.read_int64(),
                        'restart': rdr.read_bool(),
                        'unknown': rdr.read_serde_vector(Reader.read_string),
                        'invalid': rdr.read_serde_vector(Reader.read_string),
                    }),
            })
    return cmd


def decode_config_command_adl(k_rdr: Reader, rdr: Reader):
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


def decode_action_t(v):
    if 1 <= v <= 3:
        return ['', 'complete_preparing', 'activate', 'deactivate'][v]
    return v


def decode_license_t(k_rdr: Reader, version: int):
    cmd = {
        'format_version': k_rdr.read_uint8(),
        'type': k_rdr.read_serde_enum(),
        'organization': k_rdr.read_string(),
        'expiry': k_rdr.read_int64(),
    }

    if version >= 1:
        cmd |= {'checksum': k_rdr.read_string()}

    return cmd


def decode_feature_command_serde(k_rdr: Reader, rdr: Reader):
    cmd = {'type': rdr.read_int8()}
    rdr.read_int8()
    if cmd['type'] == 0:
        cmd['type_name'] = 'feature_update'
        cmd |= k_rdr.read_envelope(
            lambda k_rdr, _: {
                'cluster_version':
                k_rdr.read_int64(),
                'actions':
                k_rdr.read_serde_vector(lambda k_rdr: k_rdr.read_envelope(
                    lambda k_rdr, _: {
                        'feature_name': k_rdr.read_string(),
                        'action': decode_action_t(k_rdr.read_serde_enum()),
                    }))
            })
    elif cmd['type'] == 1:
        cmd['type_name'] = 'license_update'
        cmd |= k_rdr.read_envelope(
            lambda k_rdr, _: {
                'redpanda_license':
                k_rdr.read_envelope(decode_license_t, max_version=1)
            })
    return cmd


def decode_feature_command_adl(k_rdr: Reader, rdr: Reader):
    def decode_feature_update_action(r):
        action = {}
        action['v'] = r.read_int8()
        action['feature_name'] = r.read_string()
        action['action'] = decode_action_t(r.read_int16())
        return action

    cmd = {}
    cmd['type'] = rdr.read_int8()
    rdr.read_int8()
    if cmd['type'] == 0:
        cmd['type_name'] = 'feature_update'
        cmd['v'] = k_rdr.read_int8()
        cmd['cluster_version'] = k_rdr.read_int64()
        cmd['actions'] = k_rdr.read_vector(decode_feature_update_action)
    return cmd


def decode_node_management_command(k_rdr: Reader, rdr: Reader):
    cmd = {'type': rdr.read_int8()}
    if cmd['type'] == 0:
        cmd |= {
            'type_string': 'decommission_node',
            'node_id': k_rdr.read_int32()
        }
        rdr.read_int8()
    elif cmd['type'] == 1:
        cmd |= {
            'type_string': 'recommission_node',
            'node_id': k_rdr.read_int32()
        }
        rdr.read_int8()
    elif cmd['type'] == 2:
        cmd |= {
            'type_string': 'finish_reallocations',
            'node_id': k_rdr.read_int32()
        }
        rdr.read_int8()
    elif cmd['type'] == 3:
        cmd |= {
            'type_string': 'maintenance_mode',
            'node_id': k_rdr.read_int32(),
            'enabled': rdr.read_bool()
        }
    elif cmd['type'] == 4:
        cmd |= {
            'type_string': 'register_node_uuid',
            'uuid': k_rdr.read_uuid(),
            'id': rdr.read_optional(lambda r: r.read_int32())
        }
    return cmd


def decode_user_and_credential(rdr: Reader):
    def read_credentials(rdr: Reader, v: int):
        return {
            'salt': obfuscate_secret(rdr.read_iobuf().hex()),
            'server_key': obfuscate_secret(rdr.read_iobuf().hex()),
            'stored_key': obfuscate_secret(rdr.read_iobuf().hex()),
            'iterations': rdr.read_int32(),
        }

    return rdr.read_envelope(
        lambda r, _: {
            'username': r.read_string(),
            'credentials': r.read_envelope(read_credentials)
        })


def decode_bootstrap_cluster_cmd_data(rdr: Reader, version):
    decoded = {
        'cluster_uuid': rdr.read_uuid(),
        'bootstrap_user_cred': rdr.read_optional(decode_user_and_credential),
        'nodes_by_uuid': rdr.read_serde_map(Reader.read_uuid,
                                            Reader.read_int32)
    }
    if version >= 1:
        decoded |= {'founding_version': rdr.read_int64()}

    return decoded


def decode_cluster_bootstrap_command(k_rdr, rdr):
    cmd = {}
    rdr.skip(1)
    k_rdr.read_int8()
    cmd['type'] = rdr.read_int8()
    if cmd['type'] == 0:
        cmd['type_name'] = 'bootstrap_cluster'
        cmd |= rdr.read_envelope(decode_bootstrap_cluster_cmd_data,
                                 max_version=1)

    return cmd


def decode_adl_or_serde(k_rdr: Reader, rdr: Reader, adl_fn, serde_fn):
    either_adl_or_serde = rdr.peek_int8()
    assert either_adl_or_serde >= -1, "unsupported serialization format"
    if either_adl_or_serde == -1:
        # serde encoding flag, consume it and proceed
        rdr.skip(1)
        return serde_fn(k_rdr, rdr)
    else:
        return adl_fn(k_rdr, rdr)


def decode_record(batch, record, bin_dump: bool):
    ret = {}
    header = batch.header
    ret['type'] = batch.type.name
    ret['epoch'] = header.first_ts
    ret['offset'] = header.base_offset + record.offset_delta
    ret['ts'] = datetime.datetime.utcfromtimestamp(
        header.first_ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
    if bin_dump:
        ret['key_dump'] = record.key.__str__()
        ret['value_dump'] = record.value.__str__()
    ret['data'] = None

    rdr = Reader(BytesIO(record.value))
    k_rdr = Reader(BytesIO(record.key))

    if batch.type == BatchType.raft_configuration:
        ret['data'] = decode_config(k_rdr, rdr)
    if batch.type == BatchType.topic_management_cmd:
        ret['data'] = decode_adl_or_serde(k_rdr, rdr, decode_topic_command_adl,
                                          decode_topic_command_serde)
    if batch.type == BatchType.user_management_cmd:
        ret['data'] = decode_adl_or_serde(k_rdr, rdr, decode_user_command_adl,
                                          decode_user_command_serde)
    if batch.type == BatchType.acl_management_cmd:
        ret['data'] = decode_adl_or_serde(k_rdr, rdr, decode_acl_command_adl,
                                          decode_acl_command_serde)
    if batch.type == BatchType.cluster_config_cmd:
        ret['data'] = decode_adl_or_serde(k_rdr, rdr,
                                          decode_config_command_adl,
                                          decode_config_command_serde)
    if batch.type == BatchType.feature_update:
        ret['data'] = decode_adl_or_serde(k_rdr, rdr,
                                          decode_feature_command_adl,
                                          decode_feature_command_serde)
    if batch.type == BatchType.node_management_cmd:
        ret['data'] = decode_adl_or_serde(k_rdr, rdr,
                                          decode_node_management_command,
                                          decode_node_management_command)
    if batch.type == BatchType.cluster_bootstrap_cmd:
        ret['data'] = decode_cluster_bootstrap_command(k_rdr, rdr)

    k_unread = k_rdr.remaining()
    v_unread = rdr.remaining()
    if k_unread != 0 or v_unread != 0:
        ret['unread'] = {'key': k_unread, 'value': v_unread}
        logger.error(
            f"@{ret['type']} unread bytes. k:{k_unread} v:{v_unread} {json.dumps(ret)}"
        )

    return ret


class ControllerLog:
    def __init__(self, ntp, bin_dump: bool):
        self.ntp = ntp
        self.bin_dump = bin_dump

    def __iter__(self):
        for path in self.ntp.segments:
            s = Segment(path)
            for b in s:
                for r in b:
                    yield decode_record(b, r, self.bin_dump)


def read_feature_state_snapshot(rdr: Reader, version: int):
    ret = {}
    ret |= {"name": rdr.read_string()}
    ret |= {"state": rdr.read_serde_enum()}
    return ret


def read_license(rdr: Reader, version: int):
    ret = {}
    ret |= {"format_version": rdr.read_int8()}
    ret |= {"type": rdr.read_serde_enum()}
    ret |= {"organization": rdr.read_string()}
    ret |= {"expiry": rdr.read_int64()}
    ret |= {"checksum": rdr.read_string()}
    return ret


def read_features_table_snapshot(rdr: Reader, version: int):
    ret = {}
    ret |= {"applied_offset": rdr.read_int64()}
    ret |= {"cluster_version": rdr.read_int64()}
    ret |= {
        "states":
        rdr.read_serde_vector(lambda rdr: rdr.read_envelope(
            type_read=read_feature_state_snapshot))
    }

    ret |= {
        "license":
        rdr.read_optional(lambda rdr: rdr.read_envelope(type_read=read_license,
                                                        max_version=1))
    }
    ret |= {"original_version": rdr.read_int64()}
    return ret


def read_config_status(rdr: Reader, v: int):
    r = {'node': rdr.read_int32()}
    r |= {'version': rdr.read_int64()}
    r |= {'restart': rdr.read_bool()}
    r |= {'unknown': rdr.read_vector(Reader.read_string)}
    r |= {'invalid': rdr.read_vector(Reader.read_string)}
    return r


def read_topic_metadata_fields(rdr: Reader, v: int):
    v = {}
    v |= {"configuration": rdr.read_envelope(read_topic_config, max_version=2)}
    v |= {"src_topic": rdr.read_optional(Reader.read_string)}
    v |= {"revision": rdr.read_int64()}
    v |= {"remote_revision": rdr.read_optional(Reader.read_int64)}

    return v


def read_disabled_partitions_set(rdr: Reader, v: int):
    return {
        "partitions":
        rdr.read_optional(lambda r: r.read_serde_vector(Reader.read_int32))
    }


def read_role_member(rdr: Reader, version: int):
    return {"type": rdr.read_serde_enum(), "name": rdr.read_string()}


def read_role(rdr: Reader, version: int):
    return {
        "members":
        rdr.read_serde_vector(lambda r: r.read_envelope(read_role_member))
    }


def read_transform_metadata(rdr: Reader, version: int):
    return {
        "name": rdr.read_string(),
        "input_topic": read_topic_namespace(rdr),
        "output_topics": rdr.read_serde_vector(read_topic_namespace),
        "environment": rdr.read_serde_map(Reader.read_string,
                                          Reader.read_string),
        "uuid": rdr.read_uuid(),
        "offset_options": rdr.read_envelope()
    }


class ControllerSnapshot():
    def __init__(self, ntp, bin_dump: bool):
        self.ntp = ntp
        self.bin_dump = bin_dump

    @property
    def snapshot_path(self):
        return Path(self.ntp.path) / "snapshot"

    def read_bootstrap(self, rdr: Reader, version: int):
        return {"uuid": rdr.read_optional(Reader.read_uuid)}

    def read_features(self, rdr: Reader, version: int):
        return rdr.read_envelope(type_read=read_features_table_snapshot,
                                 max_version=1)

    def read_members(self, rdr: Reader, version: int):
        def read_update_t(inner: Reader, v: int):
            ret = {}
            ret |= {"update_type": inner.read_serde_enum()}
            ret |= {"offset": inner.read_int64()}
            ret |= {
                "decommission_update_revision":
                inner.read_optional(Reader.read_int64)
            }
            return ret

        def read_node_t(inner: Reader, v: int):
            ret = {}
            ret |= {
                "broker":
                inner.read_envelope(type_read=lambda r, _: read_broker(r))
            }
            ret |= {
                "state":
                inner.read_envelope(
                    type_read=lambda r, _: read_broker_state(r), max_version=1)
            }
            return ret

        ret = {}
        ret |= {
            'node_ids_by_uuid':
            rdr.read_serde_map(Reader.read_uuid, Reader.read_int32)
        }
        ret |= {"next_assigned_id": rdr.read_int32()}
        ret |= {
            "nodes":
            rdr.read_serde_map(
                Reader.read_int32,
                lambda r: r.read_envelope(type_read=read_node_t))
        }

        ret |= {
            "removed_nodes":
            rdr.read_serde_map(
                Reader.read_int32,
                lambda r: r.read_envelope(type_read=read_node_t))
        }
        ret |= {
            "removed_nodes_still_in_raft0":
            rdr.read_serde_vector(Reader.read_int32)
        }
        ret |= {
            "in_progress_updates":
            rdr.read_serde_map(
                Reader.read_int32,
                lambda r: r.read_envelope(type_read=read_update_t))
        }
        ret |= {"first_node_operation_command_offset": rdr.read_int64()}
        return ret

    def read_config(self, rdr: Reader, version: int):
        ret = {}
        ret |= {"config_version": rdr.read_int64()}
        ret |= {
            "values":
            rdr.read_serde_map(k_reader=Reader.read_string,
                               v_reader=Reader.read_string)
        }
        ret |= {
            'nodes_status':
            rdr.read_serde_vector(
                lambda r: r.read_envelope(type_read=read_config_status))
        }

        return ret

    def read_topics(self, rdr: Reader, version: int):
        def read_tp_ns_to_str(rdr: Reader):
            return read_topic_namespace(rdr)

        def read_partition_t(rdr: Reader, version: int):
            v = {}
            v |= {'group_id': rdr.read_int64()}
            v |= {'replicas': rdr.read_serde_vector(read_broker_shard)}
            v |= {
                'replica_revisions':
                rdr.read_serde_map(Reader.read_int32, Reader.read_int64)
            }
            v |= {'last_finished_rev': rdr.read_int64()}
            return v

        def read_update_t(rdr: Reader, version: int):
            v = {}
            v |= {
                'target_assignment': rdr.read_serde_vector(read_broker_shard)
            }
            v |= {'state': rdr.read_serde_enum()}
            v |= {'revision': rdr.read_int64()}
            v |= {'last_cmd_revision': rdr.read_int64()}
            v |= {'policy': rdr.read_serde_enum()}
            return v

        def read_topic_t(rdr: Reader, version: int):
            v = {}
            v |= {
                "metadata_fields":
                rdr.read_envelope(read_topic_metadata_fields)
            }
            v |= {
                "partitions":
                rdr.read_serde_map(
                    Reader.read_int32,
                    lambda r: r.read_envelope(type_read=read_partition_t))
            }
            v |= {
                "updates":
                rdr.read_serde_map(
                    Reader.read_int32,
                    lambda r: r.read_envelope(type_read=read_update_t))
            }
            v |= {
                "disabled_set":
                rdr.read_optional(
                    lambda r: r.read_envelope(read_disabled_partitions_set))
            }
            return v

        v = {}
        v |= {
            "topics":
            rdr.read_serde_map(
                read_tp_ns_to_str,
                lambda r: r.read_envelope(type_read=read_topic_t,
                                          max_version=1))
        }
        v |= {"highest_group_id": rdr.read_int64()}

        def read_nt_revision_to_str(rdr: Reader):
            res = rdr.read_envelope(
                lambda r, _: {
                    "topic_namespace": read_tp_ns_to_str(r),
                    "initial_revision_id": r.read_int64()
                })

            return f"{res['topic_namespace']}/{res['initial_revision_id']}"

        def read_nt_lifecycle_marker(rdr: Reader, version: int):
            return {
                "initial_revision_id": rdr.read_int64(),
                "timestamp": rdr.read_optional(Reader.read_int64)
            }

        v |= {
            "lifecycle_markers":
            rdr.read_serde_map(
                read_nt_revision_to_str,
                lambda r: r.read_envelope(read_nt_lifecycle_marker))
        }

        def read_ntp_to_str(rdr: Reader):
            ntp = read_ntp(rdr)
            return f"{ntp['namespace']/ntp['topic']/ntp['partition']}"

        def read_ntp_with_majority_loss(rdr: Reader):
            pass

        v |= {
            "force_recoverable_partitions":
            rdr.read_serde_map(
                read_ntp_to_str,
                lambda r: r.read_serde_vector(read_ntp_with_majority_loss))
        }

        return v

    def read_security(self, rdr: Reader, version: int):
        def read_named_role_t(rdr: Reader, version: int):
            return {
                "name": rdr.read_string(),
                "role": rdr.read_envelope(read_role)
            }

        r = {
            "user_credentials":
            rdr.read_serde_vector(decode_user_and_credential)
        }

        r |= {"acls": rdr.read_serde_vector(read_acl_binding_serde)}
        if version > 0:
            r |= {
                "roles":
                rdr.read_serde_vector(
                    lambda r: r.read_envelope(read_named_role_t))
            }

        return r

    def read_metrics_reporter(self, rdr: Reader, version: int):
        return rdr.read_envelope(lambda r, _: {
            "uuid": r.read_string(),
            "creation_ts": r.read_int64()
        })

    def read_plugins(self, rdr: Reader, version: int):
        return rdr.read_serde_map(
            Reader.read_int64,
            lambda r: r.read_envelope(read_transform_metadata, max_version=1))

    def read_cluster_recovery(self, rdr: Reader, version: int):
        def read_cluster_metadata_manifest(rdr: Reader, version: int):
            r = {"upload_time_since_epoch": rdr.read_int64()}
            r |= {"cluster_uuid": rdr.read_uuid()}
            r |= {"metadata_id": rdr.read_int64()}
            r |= {"controller_snapshot_offset": rdr.read_int64()}
            r |= {"controller_snapshot_path": rdr.read_string()}
            r |= {
                "offsets_snapshots_by_partition":
                rdr.read_vector(lambda r: r.read_vector(Reader.read_string))
            }
            return r

        def read_cluster_recovery_state(rdr: Reader, version: int):
            r = {}
            r |= {"stage": rdr.read_serde_enum()}
            r |= {
                "manifest": rdr.read_envelope(read_cluster_metadata_manifest)
            }
            r |= {"bucket": rdr.read_string()}
            r |= {"wait_for_nodes": rdr.read_bool()}
            r |= {"error_message": rdr.read_optional(Reader.read_string)}

            return r

        return {
            "recovery_states":
            rdr.read_serde_vector(
                lambda r: r.read_envelope(read_cluster_recovery_state))
        }

    def read_snapshot(self, rdr: Reader):
        data = {}
        data['bootstrap'] = rdr.read_envelope(
            type_read=lambda r, v: self.read_bootstrap(r, v), max_version=0)
        data['features'] = rdr.read_envelope(
            type_read=lambda r, v: self.read_features(r, v), max_version=0)
        data['members'] = rdr.read_envelope(
            type_read=lambda r, v: self.read_members(r, v), max_version=1)
        data['config'] = rdr.read_envelope(
            type_read=lambda r, v: self.read_config(r, v), max_version=0)
        data['topics'] = rdr.read_envelope(
            type_read=lambda r, v: self.read_topics(r, v), max_version=1)
        data['security'] = rdr.read_envelope(
            type_read=lambda r, v: self.read_security(r, v), max_version=1)
        data['metrics_reporter'] = rdr.read_envelope(
            type_read=lambda r, v: self.read_metrics_reporter(r, v),
            max_version=0)
        data['plugins'] = rdr.read_envelope(
            type_read=lambda r, v: self.read_plugins(r, v), max_version=0)
        data['cluster_recovery'] = rdr.read_envelope(
            type_read=lambda r, v: self.read_cluster_recovery(r, v),
            max_version=0)
        for _, v in data.items():
            if 'envelope' in v:
                del v['envelope']
        return data

    def parse_snapshot(self, snapshot_file):
        meta = {}
        reader = Reader(snapshot_file)
        meta['header_crc'] = reader.read_uint32()
        meta['header_data_crc'] = reader.read_uint32()
        meta['header_version'] = reader.read_int8()
        meta['md_size'] = reader.read_uint32()
        meta['last_included_index'] = reader.read_int64()
        meta['last_included_term'] = reader.read_int64()
        meta['current_version'] = reader.read_int8()
        meta['configuration'] = decode_config(None, reader)
        meta['ts'] = reader.read_int64()
        meta['log_start_delta'] = reader.read_int64()

        data = reader.read_checksum_envelope(
            type_read=lambda r, _: self.read_snapshot(r), max_version=2)

        return {'metadata': meta, 'data': data}

    def to_dict(self):
        if not self.snapshot_path.exists:
            return {}

        with open(self.snapshot_path, "rb") as sf:
            return self.parse_snapshot(sf)
