# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random

from ducktape.mark.resource import cluster
from ducktape.mark import ignore
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools


class CreatePartitionsTest(RedpandaTest):
    def _partition_count(self, topic):
        client = KafkaCliTools(self.redpanda)
        meta = client.describe_topic(topic)
        return meta.partition_count

    def _create_topic_partitions(self, topic, count):
        client = KafkaCliTools(self.redpanda)
        client.create_topic_partitions(topic, count)

    def _create_add_verify(self, topic, new_parts):
        self.logger.info(
            f"Testing topic {topic.name} with partitions {topic.partition_count} replicas {topic.replication_factor} expected partitions {new_parts}"
        )

        self.redpanda.create_topic(topic)

        wait_until(
            lambda: self._partition_count(topic.name) == topic.partition_count,
            timeout_sec=20,
            backoff_sec=2,
            err_msg=
            f"Initial topic doesn't have expected {topic.partition_count} partitions, found {self._partition_count(topic.name)}"
        )

        self._create_topic_partitions(topic.name, new_parts)

        expected_parts = new_parts
        wait_until(
            lambda: self._partition_count(topic.name) == expected_parts,
            timeout_sec=20,
            backoff_sec=2,
            err_msg=
            f"Error waiting for partitions to be created, expected {expected_parts} partitions, found {self._partition_count(topic.name)}"
        )

    @cluster(num_nodes=3)
    def test_create_partitions(self):
        big = self.scale.ci or self.scale.release
        iterations = 12 if big else 2

        for _ in range(iterations):
            partition_count = random.randint(1, 10)
            new_partition_count = random.randint(partition_count + 1, 20)
            topic = TopicSpec(partition_count=partition_count,
                              replication_factor=random.choice((1, 3)))
            self._create_add_verify(topic, new_partition_count)
