from typing import Any
from reader import Reader
from controller import read_topic_config


def to_compression_str(value: int) -> str:
    return ['none', 'gzip', 'snappy', 'lz4', 'zstd'][value]


def to_cleanup_policy_bitflags_str(value: int) -> str:
    return ['none', 'delete', 'compact', 'compact,delete'][value]


def to_compaction_strategy_str(value: int) -> str:
    return ['offset', 'timestamp', 'header'][value]


def to_timestamp_type_str(value: int) -> str:
    return ['CreateTime', 'LogAppendTime'][value]


def to_opt_json(value: Any | None, converter=lambda x: x) -> Any | None:
    return converter(value) if value is not None else None


def to_tristate_json(key: str,
                     value: dict,
                     converter=lambda x: x) -> dict[str, Any | None]:
    if 'value' in value:
        return {key: converter(value['value'])}
    if value['state'] == 'empty':
        return {key: None}
    return {}


def decode_topic_manifest(path: str) -> dict[str, Any]:
    return Reader(open(path, "rb")).read_envelope(
        lambda rdr, _: {
            'cfg': rdr.read_envelope(read_topic_config, max_version=2),
            'initial_revision': rdr.read_int64()
        })


def decode_topic_manifest_to_legacy_v1_json(path: str):
    decoded = decode_topic_manifest(path)
    res = {
        'version':
        1,
        'namespace':
        decoded['cfg']['namespace'],
        'topic':
        decoded['cfg']['topic'],
        'partition_count':
        decoded['cfg']['partitions'],
        'replication_factor':
        decoded['cfg']['replication_factor'],
        'revision_id':
        decoded['initial_revision'],
        'compression':
        to_opt_json(decoded['cfg']['properties']['compression'],
                    to_compression_str),
        'cleanup_policy_bitflags':
        to_opt_json(decoded['cfg']['properties']['cleanup_policy_bitflags'],
                    to_cleanup_policy_bitflags_str),
        'compaction_strategy':
        to_opt_json(decoded['cfg']['properties']['compaction_strategy'],
                    to_compaction_strategy_str),
        'timestamp_type':
        to_opt_json(decoded['cfg']['properties']['timestamp_type'],
                    to_timestamp_type_str),
        'segment_size':
        to_opt_json(decoded['cfg']['properties']['segment_size']),
    }

    res |= to_tristate_json('retention_bytes',
                            decoded['cfg']['properties']['retention_bytes'])
    res |= to_tristate_json('retention_duration',
                            decoded['cfg']['properties']['retention_duration'])

    return res
