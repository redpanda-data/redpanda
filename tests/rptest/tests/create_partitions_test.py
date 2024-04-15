# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaMixedTest
from ducktape.tests.test import TestContext
from rptest.clients.types import TopicSpec


class CreatePartitionsTest(RedpandaMixedTest):
    def __init__(self, test_context: TestContext):
        super().__init__(test_context=test_context)

    def _partition_count(self, topic: str):
        meta = self.client().describe_topic(topic)
        return len(meta.partitions)

    def _create_add_verify(self, topic: TopicSpec, new_parts: int):
        self.logger.info(
            f"Testing topic {topic.name} with partitions {topic.partition_count} replicas {topic.replication_factor} expected partitions {new_parts}"
        )

        self.client().create_topic(topic)

        wait_until(
            lambda: self._partition_count(topic.name) == topic.partition_count,
            timeout_sec=20,
            backoff_sec=2,
            err_msg=
            f"Initial topic doesn't have expected {topic.partition_count} partitions, found {self._partition_count(topic.name)}"
        )

        self.client().alter_topic_partition_count(topic.name, new_parts)

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
            self.client().delete_topic(topic.name)
