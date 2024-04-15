# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaMixedTest


class DefaultClientTest(RedpandaMixedTest):
    def __init__(self, ctx):
        super().__init__(test_context=ctx, min_brokers=1)

    @cluster(num_nodes=1)
    def test_create_delete_topic(self):
        name = 'test-create-delete-topic'
        spec = TopicSpec(name=name, replication_factor=1)

        client = self.client()

        client.create_topic(spec)

        desc = client.describe_topic(name)
        assert desc.name == name
        assert len(desc.partitions) > 0

        client.delete_topic(name)


class KafkaCliToolsTest(RedpandaMixedTest):
    def __init__(self, ctx):
        super().__init__(test_context=ctx, min_brokers=1)

    @cluster(num_nodes=1)
    def test_produce_consume(self):
        name = 'test-produce-consume'
        spec = TopicSpec(name=name, replication_factor=1)
        client = KafkaCliTools(self.redpanda)
        rpk = RpkTool(self.redpanda)

        client.create_topic(spec)

        # produce and consume a message to the topic
        client.produce(topic=name, num_records=1, record_size=100)
        rpk.consume(topic=name, n=1, timeout=10)

        client.delete_topic(name)


class RedpandaMixedTestSelfTest(RedpandaMixedTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, min_brokers=1, **kwargs)

    @cluster(num_nodes=1)
    def test_rpk(self):
        """A very basic rpk test."""
        rpk = RpkTool(self.redpanda)

        rpk.list_topics()

        name = 'test-rpk-create-topic'
        rpk.create_topic(name)
        rpk.delete_topic(name)

    @cluster(num_nodes=1)
    def test_metrics(self):
        """Test metrics_sample() can retrieve internal metrics.
        """
        vectorized_application_uptime = self.redpanda.metrics_sample(
            sample_pattern='vectorized_application_uptime')
        assert vectorized_application_uptime is not None, 'expected some metrics'

        sample_patterns = [
            'vectorized_application_uptime', 'vectorized_reactor_utilization'
        ]
        samples = self.redpanda.metrics_samples(sample_patterns)
        assert samples is not None, 'expected sample patterns to match'

        count = self.redpanda.metric_sum('vectorized_application_uptime')
        assert count > 0, 'expected count greater than 0'
