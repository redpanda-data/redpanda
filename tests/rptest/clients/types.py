# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from enum import Enum
import random
import string


class TopicSpec:
    """
    A topic specification.

    It is often the case that in a test the name of a topic does not matter. To
    simplify for this case, a random name is generated if none is provided.
    """
    CLEANUP_COMPACT = "compact"
    CLEANUP_DELETE = "delete"
    CLEANUP_COMPACT_DELETE = "compact,delete"

    PROPERTY_COMPRESSSION = "compression.type"
    PROPERTY_CLEANUP_POLICY = "cleanup.policy"
    PROPERTY_COMPACTION_STRATEGY = "compaction.strategy"
    PROPERTY_TIMESTAMP_TYPE = "message.timestamp.type"
    PROPERTY_SEGMENT_SIZE = "segment.bytes"
    PROPERTY_RETENTION_BYTES = "retention.bytes"
    PROPERTY_RETENTION_TIME = "retention.ms"
    PROPERTY_DATA_POLICY_FUNCTION_NAME = "redpanda.datapolicy.function.name"
    PROPERTY_DATA_POLICY_SCRIPT_NAME = "redpanda.datapolicy.script.name"
    PROPERTY_RETENTION_LOCAL_TARGET_BYTES = "retention.local.target.bytes"
    PROPERTY_RETENTION_LOCAL_TARGET_MS = "retention.local.target.ms"
    PROPERTY_REMOTE_DELETE = "redpanda.remote.delete"
    PROPERTY_SEGMENT_MS = "segment.ms"

    class CompressionTypes(str, Enum):
        """
        compression types
        """
        NONE = "none"
        PRODUCER = "producer"
        GZIP = "gzip"
        LZ4 = "lz4"
        SNAPPY = "snappy"
        ZSTD = "zstd"

    # compression types
    COMPRESSION_NONE = CompressionTypes.NONE
    COMPRESSION_PRODUCER = CompressionTypes.PRODUCER
    COMPRESSION_GZIP = CompressionTypes.GZIP
    COMPRESSION_LZ4 = CompressionTypes.LZ4
    COMPRESSION_SNAPPY = CompressionTypes.SNAPPY
    COMPRESSION_ZSTD = CompressionTypes.ZSTD

    # timestamp types
    TIMESTAMP_CREATE_TIME = "CreateTime"
    TIMESTAMP_LOG_APPEND_TIME = "LogAppendTime"

    class SubjectNameStrategy(str, Enum):
        TOPIC_NAME = "TopicNameStrategy"
        RECORD_NAME = "RecordNameStrategy"
        TOPIC_RECORD_NAME = "TopicRecordNameStrategy"

    PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION = "redpanda.key.schema.id.validation"
    PROPERTY_RECORD_KEY_SUBJECT_NAME_STRATEGY = "redpanda.key.subject.name.strategy"
    PROPERTY_RECORD_VALUE_SCHEMA_ID_VALIDATION = "redpanda.value.schema.id.validation"
    PROPERTY_RECORD_VALUE_SUBJECT_NAME_STRATEGY = "redpanda.value.subject.name.strategy"

    class SubjectNameStrategyCompat(str, Enum):
        TOPIC_NAME = "io.confluent.kafka.serializers.subject.TopicNameStrategy"
        RECORD_NAME = "io.confluent.kafka.serializers.subject.RecordNameStrategy"
        TOPIC_RECORD_NAME = "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy"

    PROPERTY_RECORD_KEY_SCHEMA_ID_VALIDATION_COMPAT = "confluent.key.schema.validation"
    PROPERTY_RECORD_KEY_SUBJECT_NAME_STRATEGY_COMPAT = "confluent.key.subject.name.strategy"
    PROPERTY_RECORD_VALUE_SCHEMA_ID_VALIDATION_COMPAT = "confluent.value.schema.validation"
    PROPERTY_RECORD_VALUE_SUBJECT_NAME_STRATEGY_COMPAT = "confluent.value.subject.name.strategy"

    PROPERTY_INITIAL_RETENTION_LOCAL_TARGET_BYTES = "initial.retention.local.target.bytes"
    PROPERTY_INITIAL_RETENTION_LOCAL_TARGET_MS = "initial.retention.local.target.ms"
    PROPERTY_VIRTUAL_CLUSTER_ID = "redpanda.virtual.cluster.id"

    def __init__(self,
                 *,
                 name: str | None = None,
                 partition_count: int = 1,
                 replication_factor: int = 3,
                 cleanup_policy: str = CLEANUP_DELETE,
                 compression_type: CompressionTypes = COMPRESSION_PRODUCER,
                 message_timestamp_type=TIMESTAMP_CREATE_TIME,
                 segment_bytes=None,
                 retention_bytes=None,
                 retention_ms=None,
                 redpanda_datapolicy=None,
                 redpanda_remote_read=None,
                 redpanda_remote_write=None,
                 redpanda_remote_delete=None,
                 segment_ms=None,
                 max_message_bytes=None,
                 record_key_schema_id_validation=None,
                 record_key_schema_id_validation_compat=None,
                 record_key_subject_name_strategy=None,
                 record_key_subject_name_strategy_compat=None,
                 record_value_schema_id_validation=None,
                 record_value_schema_id_validation_compat=None,
                 record_value_subject_name_strategy=None,
                 record_value_subject_name_strategy_compat=None,
                 initial_retention_local_target_bytes=None,
                 initial_retention_local_target_ms=None,
                 virtual_cluster_id=None):
        self.name = name or f"topic-{self._random_topic_suffix()}"
        self.partition_count = partition_count
        self.replication_factor = replication_factor
        self.cleanup_policy = cleanup_policy
        self.compression_type = compression_type
        self.message_timestamp_type = message_timestamp_type
        self.segment_bytes = segment_bytes
        self.retention_bytes = retention_bytes
        self.retention_ms = retention_ms
        self.redpanda_datapolicy = redpanda_datapolicy
        self.redpanda_remote_read = redpanda_remote_read
        self.redpanda_remote_write = redpanda_remote_write
        self.redpanda_remote_delete = redpanda_remote_delete
        self.segment_ms = segment_ms
        self.max_message_bytes = max_message_bytes
        self.record_key_schema_id_validation = record_key_schema_id_validation
        self.record_key_schema_id_validation_compat = record_key_schema_id_validation_compat
        self.record_key_subject_name_strategy = record_key_subject_name_strategy
        self.record_key_subject_name_strategy_compat = record_key_subject_name_strategy_compat
        self.record_value_schema_id_validation = record_value_schema_id_validation
        self.record_value_schema_id_validation_compat = record_value_schema_id_validation_compat
        self.record_value_subject_name_strategy = record_value_subject_name_strategy
        self.record_value_subject_name_strategy_compat = record_value_subject_name_strategy_compat
        self.initial_retention_local_target_bytes = initial_retention_local_target_bytes
        self.initial_retention_local_target_ms = initial_retention_local_target_ms
        self.virtual_cluster_id = virtual_cluster_id

    def __str__(self):
        return self.name

    def __eq__(self, other):
        if not isinstance(other, TopicSpec):
            return False
        return self.name == other.name and \
                self.partition_count == other.partition_count and \
                self.replication_factor == other.replication_factor and \
                self.cleanup_policy == other.cleanup_policy

    def _random_topic_suffix(self, size=10):
        return "".join(
            random.choice(string.ascii_lowercase) for _ in range(size))
