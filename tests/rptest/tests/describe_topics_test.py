# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from ducktape.mark.resource import cluster
from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools


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
                if f"partition_count={topic.partition_count}" not in output:
                    return False
                if f"replication_factor={topic.replication_factor}" not in output:
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
