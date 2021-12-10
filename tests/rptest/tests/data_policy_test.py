# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec

import subprocess

from rptest.tests.redpanda_test import RedpandaTest


class DataPolicyTest(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, test_context):
        super(DataPolicyTest, self).__init__(test_context=test_context,
                                             num_brokers=3)

        self.kafka_tools = KafkaCliTools(self.redpanda)

    def _get_data_policy(self, function_name, script_name):
        return "function_name: {} script_name: {}".format(
            function_name, script_name)

    @cluster(num_nodes=3)
    def test_default_data_policy(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        spec = kafka_tools.describe_topic(topic)
        assert spec.redpanda_datapolicy == None

    @cluster(num_nodes=3)
    def test_set_data_policy(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        res = kafka_tools.alter_topic_config(
            topic, {
                TopicSpec.PROPERTY_DATA_POLICY_FUNCTION_NAME: "1",
                TopicSpec.PROPERTY_DATA_POLICY_SCRIPT_NAME: "2"
            })
        spec = kafka_tools.describe_topic(topic)
        assert spec.redpanda_datapolicy == self._get_data_policy(1, 2)

    @cluster(num_nodes=3)
    def test_incremental_config(self):
        topic = self.topics[0].name
        kafka_tools = KafkaCliTools(self.redpanda)
        kafka_tools.alter_topic_config(
            topic, {
                TopicSpec.PROPERTY_DATA_POLICY_FUNCTION_NAME: "1",
                TopicSpec.PROPERTY_DATA_POLICY_SCRIPT_NAME: "2"
            })
        spec = kafka_tools.describe_topic(topic)
        assert spec.redpanda_datapolicy == self._get_data_policy(1, 2)

        # Expect that trying to set with a function name but no script fails.
        try:
            r = kafka_tools.alter_topic_config(
                topic, {TopicSpec.PROPERTY_DATA_POLICY_FUNCTION_NAME: "3"})
        except subprocess.CalledProcessError as e:
            # Expected: request fails to update topic
            self.logger.info(f"Kafka CLI alter failed as expected: {e.stdout}")
            assert "unable to parse property" in e.stdout
        else:
            raise RuntimeError(f"Expected API error, got {r}")

        # Expect that the failed alter operation has not modified the topic
        spec = kafka_tools.describe_topic(topic)
        assert spec.redpanda_datapolicy == self._get_data_policy(1, 2)
