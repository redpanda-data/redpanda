# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
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

    # compression types
    COMPRESSION_NONE = "none"
    COMPRESSION_PRODUCER = "producer"
    COMPRESSION_GZIP = "gzip"
    COMPRESSION_LZ4 = "lz4"
    COMPRESSION_SNAPPY = "snappy"
    COMPRESSION_ZSTD = "zstd"

    # timestamp types
    TIMESTAMP_CREATE_TIME = "CreateTime"
    TIMESTAMP_LOG_APPEND_TIME = "LogAppendTime"

    def __init__(self,
                 *,
                 name=None,
                 partition_count=1,
                 replication_factor=3,
                 cleanup_policy=CLEANUP_DELETE,
                 compression_type=COMPRESSION_PRODUCER,
                 message_timestamp_type=TIMESTAMP_CREATE_TIME,
                 segment_bytes=None,
                 retention_bytes=None,
                 retention_ms=None,
                 redpanda_datapolicy=None,
                 redpanda_remote_read=None,
                 redpanda_remote_write=None,
                 redpanda_remote_delete=None,
                 segment_ms=None):
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
