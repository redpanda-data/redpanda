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
        tp_spec = TopicSpec()

        self.client().create_topic([tp_spec])

        kcl = KCL(self.redpanda)

        output = kcl.describe_topic(tp_spec.name,
                                    with_docs=True,
                                    with_types=True).splitlines()

        assert len(output) >= 26

        # First thirteen lines follow a format
        property_lines = output[:13]
        # Lines after that are for documentation
        documentation_lines = output[13:]
        self.logger.debug(f"Nyalia\n{property_lines}")
        self.logger.debug(f"Lui\n{documentation_lines}")

        # All the config properties and their values reported by topic describe
        properties = {
            "cleanup.policy":
            ConfigProperty(config_type="STRING",
                           value="DELETE",
                           doc_string="Default topic cleanup policy",
                           source_type="DYNAMIC_TOPIC_CONFIG"),
            "compression.type":
            ConfigProperty(config_type="STRING",
                           value="producer",
                           doc_string="Default topic compression type"),
            "max.message.bytes":
            ConfigProperty(
                config_type="INT",
                value="1048576",
                doc_string=
                "Maximum size of a batch processed by server. If batch is compressed the limit applies to compressed batch size"
            ),
            "message.timestamp.type":
            ConfigProperty(config_type="STRING",
                           value="CreateTime",
                           doc_string="Default topic messages timestamp type"),
            "redpanda.datapolicy":
            ConfigProperty(config_type="STRING",
                           value="function_name: script_name:",
                           doc_string="Datapolicy property for v8_engine"),
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
                "Default max bytes per partition on disk before triggering a compaction"
            ),
            "retention.local.target.bytes":
            ConfigProperty(
                config_type="LONG",
                value="-1",
                doc_string=
                "Local retention size target for partitions of topics with cloud storage write enabled"
            ),
            "retention.local.target.ms":
            ConfigProperty(
                config_type="LONG",
                value="86400000",
                doc_string=
                "Local retention time target for partitions of topics with cloud storage write enabled"
            ),
            "retention.ms":
            ConfigProperty(
                config_type="LONG",
                value="604800000",
                doc_string="delete segments older than this - default 1 week"),
            "segment.bytes":
            ConfigProperty(
                config_type="LONG",
                value="1073741824",
                doc_string=
                "Default log segment size in bytes for topics which do not set segment.bytes"
            )
        }

        for line in property_lines:
            col = line.strip().lower().split()
            self.logger.debug(col)
            assert len(col) >= 4
            name = col[0]
            config_type = col[1]
            value = col[2]
            source_type = col[3]

            # The above .split() also applies to the value for datapolicy
            # so we need to re-concatenate the string
            if name == "redpanda.datapolicy":
                assert len(col) == 5
                value = f'{col[2]} {col[3]}'
                source_type = col[4]

            assert name in properties.keys()
            prop = properties[name]
            assert config_type == prop.config_type
            assert value == prop.value
            assert source_type == prop.source_type

        # The output format follows:
        # line 1 is empty
        # line 2 is the property name
        # line 3 is the property docstring
        prop = None
        for i in range(0, len(documentation_lines), 3):
            # The property names have a colon at the end
            prop_match = re.match(r"^(?P<name>[A-Za-z.]+?):$", line[1])
            if prop_match is not None:
                name = prop_match.group("name").lower()
                assert name in properties.keys()
                prop = properties[name]
                assert line[2].lower() == prop.doc_string
