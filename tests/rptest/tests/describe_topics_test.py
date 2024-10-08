# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random

from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.kcl import KCL
import re


class ConfigProperty:
    def __init__(self,
                 config_type: str,
                 value: str,
                 doc_string: str,
                 source_type: str = "default_config"):
        self.config_type = config_type.lower()
        self.value = value.lower()
        self.doc_string = doc_string.lower()
        self.source_type = source_type.lower()


class DescribeTopicsTest(RedpandaTest):
    @cluster(num_nodes=3)
    def test_describe_topics(self):
        big = self.scale.ci or self.scale.release
        num_topics = 20 if big else 2

        topics = [
            TopicSpec(partition_count=random.randint(1, 20),
                      replication_factor=random.choice((1, 3)))
            for _ in range(num_topics)
        ]

        self.client().create_topic(topics)

        def check():
            client = KafkaCliTools(self.redpanda)
            # bulk describe
            output = client.describe_topics()
            for topic in topics:
                if f"PartitionCount: {topic.partition_count}" not in output:
                    return False
                if f"ReplicationFactor: {topic.replication_factor}" not in output:
                    return False
            # and targetted topic describe
            topics_described = [
                client.describe_topic(topic.name) for topic in topics
            ]
            for meta in zip(topics, topics_described):
                if meta[0].partition_count != meta[1].partition_count:
                    return False
                if meta[0].replication_factor != meta[1].replication_factor:
                    return False
            return True

        wait_until(check,
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg=f"Failed to describe all topics {topics}")

    @cluster(num_nodes=3)
    def test_describe_topics_with_documentation_and_types(self):
        # All the config properties and their values reported by topic describe
        properties = {
            "cleanup.policy":
            ConfigProperty(
                config_type="STRING",
                value="DELETE",
                doc_string=
                "default cleanup policy for topic logs. the topic property `cleanup.policy` overrides the value of `log_cleanup_policy` at the topic level.",
                source_type="DYNAMIC_TOPIC_CONFIG"),
            "compression.type":
            ConfigProperty(
                config_type="STRING",
                value="producer",
                doc_string=
                "Default topic compression type. The topic property `compression.type` overrides the value of `log_compression_type` at the topic level."
            ),
            "max.message.bytes":
            ConfigProperty(
                config_type="INT",
                value="1048576",
                doc_string=
                "Maximum size of a batch processed by the server. If the batch is compressed, the limit applies to the compressed batch size."
            ),
            "message.timestamp.type":
            ConfigProperty(
                config_type="STRING",
                value="CreateTime",
                doc_string=
                "Default timestamp type for topic messages (CreateTime or LogAppendTime). The topic property `message.timestamp.type` overrides the value of `log_message_timestamp_type` at the topic level."
            ),
            "redpanda.remote.delete":
            ConfigProperty(
                config_type="BOOLEAN",
                value="true",
                doc_string=
                "Controls whether topic deletion should imply deletion in S3"),
            "redpanda.remote.read":
            ConfigProperty(
                config_type="BOOLEAN",
                value="false",
                doc_string="Default remote read config value for new topics"),
            "redpanda.remote.write":
            ConfigProperty(
                config_type="BOOLEAN",
                value="false",
                doc_string="Default remote write value for new topics"),
            "retention.bytes":
            ConfigProperty(
                config_type="LONG",
                value="-1",
                doc_string=
                "Default maximum number of bytes per partition on disk before triggering "
                "deletion of the oldest messages. If `null` (the default value), no "
                "limit is applied. The topic property `retention.bytes` overrides the "
                "value of `retention_bytes` at the topic level.",
            ),
            "retention.local.target.bytes":
            ConfigProperty(
                config_type="LONG",
                value="-1",
                doc_string=
                "Local retention size target for partitions of topics with object "
                "storage write enabled. If `null`, the property is disabled. This "
                "property can be overridden on a per-topic basis by setting "
                "`retention.local.target.bytes` in each topic enabled for Tiered "
                "Storage. Both `retention_local_target_bytes_default` and "
                "`retention_local_target_ms_default` can be set. The limit that is "
                "reached earlier is applied."),
            "retention.local.target.ms":
            ConfigProperty(
                config_type="LONG",
                value="86400000",
                doc_string=
                "Local retention time target for partitions of topics with object "
                "storage write enabled. This property can be overridden on a per-topic "
                "basis by setting `retention.local.target.ms` in each topic enabled for "
                "Tiered Storage. Both `retention_local_target_bytes_default` and "
                "`retention_local_target_ms_default` can be set. The limit that is "
                "reached first is applied."),
            "retention.ms":
            ConfigProperty(
                config_type="LONG",
                value="604800000",
                doc_string=
                "The amount of time to keep a log file before deleting it (in "
                "milliseconds). If set to `-1`, no time limit is applied. This is a "
                "cluster-wide default when a topic does not set or disable "
                "`retention.ms`."),
            "segment.bytes":
            ConfigProperty(
                config_type="LONG",
                value="134217728",
                doc_string=
                "Default log segment size in bytes for topics which do not set segment.bytes"
            ),
            "segment.ms":
            ConfigProperty(
                config_type="LONG",
                value="1209600000",  # 2 weeks in ms
                doc_string=
                "Default lifetime of log segments. If `null`, the property is disabled, "
                "and no default lifetime is set. Any value under 60 seconds (60000 ms) "
                "is rejected. This property can also be set in the Kafka API using the "
                "Kafka-compatible alias, `log.roll.ms`. The topic property `segment.ms` "
                "overrides the value of `log_segment_ms` at the topic level."),
            "redpanda.key.schema.id.validation":
            ConfigProperty(
                config_type="BOOLEAN",
                value="false",
                doc_string=
                "Enable validation of the schema id for keys on a record"),
            "redpanda.key.subject.name.strategy":
            ConfigProperty(
                config_type="STRING",
                value="TopicNameStrategy",
                doc_string=
                "The subject name strategy for keys if redpanda.key.schema.id.validation is enabled"
            ),
            "redpanda.value.schema.id.validation":
            ConfigProperty(
                config_type="BOOLEAN",
                value="false",
                doc_string=
                "Enable validation of the schema id for values on a record"),
            "redpanda.value.subject.name.strategy":
            ConfigProperty(
                config_type="STRING",
                value="TopicNameStrategy",
                doc_string=
                "The subject name strategy for values if redpanda.value.schema.id.validation is enabled"
            ),
            "confluent.key.schema.validation":
            ConfigProperty(
                config_type="BOOLEAN",
                value="false",
                doc_string=
                "Enable validation of the schema id for keys on a record"),
            "confluent.key.subject.name.strategy":
            ConfigProperty(
                config_type="STRING",
                value=
                "io.confluent.kafka.serializers.subject.TopicNameStrategy",
                doc_string=
                "The subject name strategy for keys if confluent.key.schema.validation is enabled"
            ),
            "confluent.value.schema.validation":
            ConfigProperty(
                config_type="BOOLEAN",
                value="false",
                doc_string=
                "Enable validation of the schema id for values on a record"),
            "confluent.value.subject.name.strategy":
            ConfigProperty(
                config_type="STRING",
                value=
                "io.confluent.kafka.serializers.subject.TopicNameStrategy",
                doc_string=
                "The subject name strategy for values if confluent.value.schema.validation is enabled"
            ),
            "initial.retention.local.target.bytes":
            ConfigProperty(
                config_type="LONG",
                value="-1",
                doc_string=
                "Initial local retention size target for partitions of topics with Tiered Storage enabled. If no initial local target retention is configured all locally retained data will be delivered to learner when joining partition replica set."
            ),
            "initial.retention.local.target.ms":
            ConfigProperty(
                config_type="LONG",
                value="-1",
                doc_string=
                "Initial local retention time target for partitions of topics with Tiered Storage enabled. If no initial local target retention is configured all locally retained data will be delivered to learner when joining partition replica set."
            ),
            "write.caching":
            ConfigProperty(
                config_type="STRING",
                value="false",
                doc_string=
                "The default write caching mode to apply to user topics. Write caching "
                "acknowledges a message as soon as it is received and acknowledged on a "
                "majority of brokers, without waiting for it to be written to disk. With "
                "`acks=all`, this provides lower latency while still ensuring that a "
                "majority of brokers acknowledge the write. Fsyncs follow "
                "`raft_replica_max_pending_flush_bytes` and "
                "`raft_replica_max_flush_delay_ms`, whichever is reached first. The "
                "`write_caching_default` cluster property can be overridden with the "
                "`write.caching` topic property. Accepted values: * `true` * `false` * "
                "`disabled`: This takes precedence over topic overrides and disables "
                "write caching for the entire cluster.",
            ),
            "flush.ms":
            ConfigProperty(
                config_type="LONG",
                value="100",
                doc_string=
                "Maximum delay between two subsequent flushes. After this delay, the log is automatically force flushed."
            ),
            "flush.bytes":
            ConfigProperty(
                config_type="LONG",
                value="262144",
                doc_string=
                "Maximum number of bytes that are not flushed per partition. If the configured threshold is reached, the log is automatically flushed even if it has not been explicitly requested."
            ),
            "redpanda.iceberg.enabled":
            ConfigProperty(
                config_type="BOOLEAN",
                value="false",
                doc_string=
                "Iceberg format translation enabled on this topic if true."),
            "redpanda.leaders.preference":
            ConfigProperty(
                config_type="STRING",
                value="none",
                doc_string=
                "Preferred location (e.g. rack) for partition leaders of this topic."
            ),
        }

        tp_spec = TopicSpec()
        self.client().create_topic([tp_spec])
        kcl = KCL(self.redpanda)
        output = kcl.describe_topic(tp_spec.name,
                                    with_docs=True,
                                    with_types=True).lower().splitlines()

        # The output format from topic describe has the following structure
        #
        # - A table section with descriptions for the property name, type, value, and source
        # - An empty line
        # - Three lines for each property, called the documentation section:
        #     1. Property name with a colon (e.g., cleanup.policy:)
        #     2. Property documentation string
        #     3. An empty line

        property_re = re.compile(
            r"^(?P<name>[a-z.]+?)\s+(?P<type>[a-z]+?)\s+(?P<value>\S+?)\s+(?P<src>[a-z_]+?)$"
        )

        last_pos = None
        for i, line in enumerate(output):
            self.logger.debug(f"Property line {line}")
            prop_match = property_re.match(line)

            if prop_match is None:
                last_pos = i
                break

            name = prop_match.group("name")
            config_type = prop_match.group("type")
            value = prop_match.group("value")
            source_type = prop_match.group("src")
            self.logger.debug(
                f"name: {name}, type: {config_type}, value: {value}, src: {source_type}"
            )
            assert name in properties
            prop = properties[name]
            assert config_type == prop.config_type
            assert value == prop.value
            assert source_type == prop.source_type

        # The first empty line is where the table ends and the doc section begins
        assert last_pos is not None, "Something went wrong with property match"
        self.logger.debug(
            f"Table separator {last_pos} {len(output[last_pos])}")
        assert len(output[last_pos]) == 0, "Expected empty line"

        # Make a list from the leftover lines
        assert len(output) > last_pos + 1, "Missing docs section"
        doc_lines = output[last_pos + 1:]
        assert len(doc_lines) % 3 == 0

        # The property name in the doc section has a colon
        name_re = re.compile(r"^(?P<name>[a-z.]+?):$")

        # Finally, check the doc section
        for i in range(0, len(doc_lines) // 3):
            name_line = doc_lines[3 * i]
            doc_string = doc_lines[3 * i + 1]
            empty_line = doc_lines[3 * i + 2]
            self.logger.debug(
                f"Doc line {name_line}, {doc_string}, {empty_line}")

            name_match = name_re.match(name_line)
            assert name_match is not None
            name = name_match.group("name")
            assert name in properties
            prop = properties[name]
            assert doc_string == prop.doc_string, f"{doc_string} != {prop.doc_string}"
            assert len(empty_line) == 0
