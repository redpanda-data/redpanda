# Copyright 2025 Redpanda Data, Inc.

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
from rptest.tests.redpanda_test import RedpandaTest


class TopicLimitsTest(RedpandaTest):
    def __init__(self, test_context):
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf={"auto_create_topics_enabled": False},
        )
        self.rpk = RpkTool(self.redpanda)
        self.kafka_tools = KafkaCliTools(self.redpanda)

    def _within(self, actual, expected, err) -> bool:
        return (actual >= expected * (1 - err)) & (actual <= expected * (1 + err))

    @cluster(num_nodes=3)
    def test_limit_manual_creation(self):
        def create_topics(topics: list[TopicSpec]):
            failed_attempts = 0
            for t in topics:
                try:
                    self.client().create_topic(t)
                except Exception:
                    failed_attempts += 1
            return failed_attempts

        def delete_topics(topics: list[TopicSpec]):
            for t in topics:
                self.client().delete_topic(t.name)

        topic_limit = 20
        self.redpanda.set_cluster_config({"kafka_topics_max": topic_limit})

        topics_to_create = [TopicSpec() for _ in range(2 * topic_limit)]

        failed_attempts = create_topics(topics_to_create)
        assert self._within(failed_attempts, topic_limit, 0.25)

        topic_count = len(self.client().describe_topics())
        assert self._within(topic_count, topic_limit, 0.25)
        assert (topic_count + failed_attempts) == (2 * topic_limit)

        # Now that we know the exact topic limit ensure that it is repeatable.
        topics_successfully_created = (2 * topic_limit) - failed_attempts
        delete_topics(topics_to_create[:topics_successfully_created])
        failed_attempts = create_topics(topics_to_create[:topics_successfully_created])
        assert failed_attempts == 0
        failed_attempts = create_topics(
            [topics_to_create[topics_successfully_created + 1]]
        )
        assert failed_attempts == 1

        # Raise limit and ensure all topics can be created.
        self.redpanda.set_cluster_config({"kafka_topics_max": 2 * topic_limit})

        failed_attempts = create_topics(topics_to_create[topics_successfully_created:])
        assert failed_attempts == 0

        topic_count = len(self.client().describe_topics())
        assert topic_count == 2 * topic_limit

    @cluster(num_nodes=3)
    def test_limit_auto_creation(self):
        current_topic_id = 0

        def create_topics(c: int):
            nonlocal current_topic_id
            for _ in range(c):
                try:
                    self.kafka_tools.produce(
                        f"auto_created_topic_{current_topic_id}", 1, 1024
                    )
                    current_topic_id += 1
                except Exception:
                    pass

        topic_limit = 5
        self.redpanda.set_cluster_config({"kafka_topics_max": topic_limit})
        self.redpanda.set_cluster_config({"auto_create_topics_enabled": True})

        create_topics(2 * topic_limit)
        failed_attempts = (2 * topic_limit) - len(self.client().describe_topics())
        assert failed_attempts >= 1

        self.redpanda.set_cluster_config({"kafka_topics_max": 2 * topic_limit})

        create_topics(failed_attempts)
        failed_attempts = (2 * topic_limit) - len(self.client().describe_topics())
        assert failed_attempts == 0
