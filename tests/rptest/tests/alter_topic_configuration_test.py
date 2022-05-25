# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import string

from rptest.services.cluster import cluster
from ducktape.mark import parametrize
from rptest.clients.kafka_cli_tools import KafkaCliTools

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest


class AlterTopicConfiguration(RedpandaTest):
    """
    Change a partition's replica set.
    """
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, test_context):
        super(AlterTopicConfiguration,
              self).__init__(test_context=test_context, num_brokers=3)

        self.kafka_tools = KafkaCliTools(self.redpanda)

    @cluster(num_nodes=3)
    @parametrize(property=TopicSpec.PROPERTY_CLEANUP_POLICY, value="compact")
    @parametrize(property=TopicSpec.PROPERTY_SEGMENT_SIZE,
                 value=10 * (2 << 20))
    @parametrize(property=TopicSpec.PROPERTY_RETENTION_BYTES,
                 value=200 * (2 << 20))
    @parametrize(property=TopicSpec.PROPERTY_RETENTION_TIME, value=360000)
    @parametrize(property=TopicSpec.PROPERTY_TIMESTAMP_TYPE,
                 value="LogAppendTime")
    def test_altering_topic_configuration(self, property, value):
        topic = self.topics[0].name
        self.client().alter_topic_configs(topic, {property: value})
        kafka_tools = KafkaCliTools(self.redpanda)
        spec = kafka_tools.describe_topic(topic)

        # e.g. retention.ms is TopicSpec.retention_ms
        attr_name = property.replace(".", "_")
        assert getattr(spec, attr_name, None) == value

    @cluster(num_nodes=3)
    def test_altering_multiple_topic_configurations(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        self.client().alter_topic_configs(
            topic, {
                TopicSpec.PROPERTY_SEGMENT_SIZE: 1024,
                TopicSpec.PROPERTY_RETENTION_TIME: 360000,
                TopicSpec.PROPERTY_TIMESTAMP_TYPE: "LogAppendTime"
            })
        spec = kafka_tools.describe_topic(topic)

        assert spec.segment_bytes == 1024
        assert spec.retention_ms == 360000
        assert spec.message_timestamp_type == "LogAppendTime"

    def random_string(self, size):
        return ''.join(
            random.choice(string.ascii_uppercase + string.digits)
            for _ in range(size))

    @cluster(num_nodes=3)
    def test_set_config_from_describe(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        topic_config = kafka_tools.describe_topic_config(topic=topic)
        self.client().alter_topic_configs(topic, topic_config)

    @cluster(num_nodes=3)
    def test_configuration_properties_kafka_config_allowlist(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        spec = kafka_tools.describe_topic(topic)
        self.client().alter_topic_configs(
            topic, {
                "unclean.leader.election.enable": True,
                TopicSpec.PROPERTY_SEGMENT_SIZE: spec.segment_bytes + 1,
            })

        spec.segment_bytes += 1
        new_spec = kafka_tools.describe_topic(topic)
        assert new_spec == spec

    @cluster(num_nodes=3)
    def test_configuration_properties_name_validation(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        spec = kafka_tools.describe_topic(topic)
        for _ in range(0, 5):
            key = self.random_string(5)
            try:
                self.client().alter_topic_configs(topic, {key: "123"})
            except Exception as inst:
                self.logger.info(
                    "alter failed as expected: expected exception %s", inst)
            else:
                raise RuntimeError("Alter should have failed but succeeded!")

        new_spec = kafka_tools.describe_topic(topic)
        # topic spec shouldn't change
        assert new_spec == spec

    @cluster(num_nodes=3)
    def test_shadow_indexing_config(self):
        topic = self.topics[0].name
        original_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"original_output={original_output}")
        assert original_output["redpanda.remote.read"] == "false"
        assert original_output["redpanda.remote.write"] == "false"

        self.client().alter_topic_config(topic, "redpanda.remote.read", "true")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "true"
        assert altered_output["redpanda.remote.write"] == "false"

        self.client().alter_topic_config(topic, "redpanda.remote.read",
                                         "false")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "false"
        assert altered_output["redpanda.remote.write"] == "false"

        self.client().alter_topic_config(topic, "redpanda.remote.read", "true")
        self.client().alter_topic_config(topic, "redpanda.remote.write",
                                         "true")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "true"
        assert altered_output["redpanda.remote.write"] == "true"

        self.client().alter_topic_config(topic, "redpanda.remote.read",
                                         "false")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "false"
        assert altered_output["redpanda.remote.write"] == "true"

        self.client().alter_topic_config(topic, "redpanda.remote.read", "true")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "true"
        assert altered_output["redpanda.remote.write"] == "true"

        self.client().alter_topic_config(topic, "redpanda.remote.write",
                                         "false")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "true"
        assert altered_output["redpanda.remote.write"] == "false"

        self.client().alter_topic_config(topic, "redpanda.remote.read",
                                         "false")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "false"
        assert altered_output["redpanda.remote.write"] == "false"

    @cluster(num_nodes=3)
    def test_set_remote_read_and_write_in_one_command(self):
        topic = self.topics[0].name
        original_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"original_output={original_output}")
        assert original_output["redpanda.remote.read"] == "false"
        assert original_output["redpanda.remote.write"] == "false"

        self.client().alter_topic_configs(topic, {
            "redpanda.remote.read": "true",
            "redpanda.remote.write": "true",
        })
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "true"
        assert altered_output["redpanda.remote.write"] == "true"

        self.client().alter_topic_configs(topic, {
            "redpanda.remote.read": "false",
            "redpanda.remote.write": "false",
        })
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "false"
        assert altered_output["redpanda.remote.write"] == "false"


class ShadowIndexingGlobalConfig(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, test_context):
        self._extra_rp_conf = dict(cloud_storage_enable_remote_read=True,
                                   cloud_storage_enable_remote_write=True)
        super(ShadowIndexingGlobalConfig,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf=self._extra_rp_conf)

    @cluster(num_nodes=3)
    def test_overrides_set(self):
        topic = self.topics[0].name
        original_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"original_output={original_output}")
        assert original_output["redpanda.remote.read"] == "true"
        assert original_output["redpanda.remote.write"] == "true"

        self.client().alter_topic_config(topic, "redpanda.remote.read",
                                         "false")
        self.client().alter_topic_config(topic, "redpanda.remote.write",
                                         "false")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "false"
        assert altered_output["redpanda.remote.write"] == "false"

    @cluster(num_nodes=3)
    def test_overrides_remove(self):
        topic = self.topics[0].name
        original_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"original_output={original_output}")
        assert original_output["redpanda.remote.read"] == "true"
        assert original_output["redpanda.remote.write"] == "true"

        # disable shadow indexing for topic
        self.client().alter_topic_config(topic, "redpanda.remote.read",
                                         "false")
        self.client().alter_topic_config(topic, "redpanda.remote.write",
                                         "false")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "false"
        assert altered_output["redpanda.remote.write"] == "false"

        # delete topic configs (value from configuration should be used)
        self.client().delete_topic_config(topic, "redpanda.remote.read")
        self.client().delete_topic_config(topic, "redpanda.remote.write")
        altered_output = self.client().describe_topic_configs(topic)
        self.logger.info(f"altered_output={altered_output}")
        assert altered_output["redpanda.remote.read"] == "true"
        assert altered_output["redpanda.remote.write"] == "true"
