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


def read_ntp(reader):
    ntp = {}
    ntp['namespace'] = reader.read_string()
    ntp['topic'] = reader.read_string()
    ntp['partition'] = reader.read_int32()
    return ntp


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


def read_configuration_update(rdr):
    return {
        'replicas_to_add': rdr.read_vector(read_vnode),
        'replicas_to_remove': rdr.read_vector(read_vnode)
    }


def read_raft_config(rdr):
    cfg = {}

    cfg['version'] = rdr.read_int8()
    if cfg['version'] < 5:
        cfg['brokers'] = rdr.read_vector(read_broker)
    cfg['current_config'] = read_group_nodes(rdr)
    cfg['prev_config'] = rdr.read_optional(read_group_nodes)
    cfg['revision'] = rdr.read_int64()

    if cfg['version'] >= 4:
        cfg['configuration_update'] = rdr.read_optional(
            lambda ordr: read_configuration_update(ordr))

    return cfg


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
        return 'literal'
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
