# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses / BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint
from rptest.tests.redpanda_test import RedpandaTest


class TopicLabelAggregationTest(RedpandaTest):
    TOPIC_LABEL = "topic"
    PARTITION_LABEL = "partition"

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(num_brokers=3, *args, **kwargs)

    def _aggregated_metrics_have_topic_label(self) -> bool:
        for node in self.redpanda.nodes:
            for family in self.redpanda.metrics(
                node, metrics_endpoint=MetricsEndpoint.METRICS
            ):
                for sample in family.samples:
                    # If the partition label isn't aggregated then neither will the topic label.
                    if (
                        self.TOPIC_LABEL in sample.labels.keys()
                        and self.PARTITION_LABEL not in sample.labels.keys()
                    ):
                        return True

        return False

    def _unaggregated_metrics_have_topic_label(self) -> bool:
        for node in self.redpanda.nodes:
            for family in self.redpanda.metrics(
                node, metrics_endpoint=MetricsEndpoint.METRICS
            ):
                for sample in family.samples:
                    # Only consider series that would have their topic label aggregated
                    # when topic label aggregation is enabled.
                    if (
                        self.TOPIC_LABEL in sample.labels.keys()
                        and self.PARTITION_LABEL in sample.labels.keys()
                    ):
                        return True

        return False

    def _create_topics(self, topics: list[TopicSpec]):
        for t in topics:
            self.client().create_topic(t)

    def _delete_topics(self, topics: list[TopicSpec]):
        for t in topics:
            self.client().delete_topic(t.name)

    @cluster(num_nodes=3)
    def test_topic_label_aggregation(self):
        initial_topic_count = len(self.client().describe_topics())
        topics_to_create = [TopicSpec() for _ in range(50)]
        topic_aggregation_limit = len(topics_to_create) + initial_topic_count - 1

        self.redpanda.set_cluster_config(
            {
                "aggregate_metrics": True,
                "topic_label_aggregation_limit": topic_aggregation_limit,
            }
        )
        assert self._aggregated_metrics_have_topic_label(), (
            "topic label shouldn't be aggregated yet"
        )

        self._create_topics(topics_to_create)
        wait_until(
            lambda: not self._aggregated_metrics_have_topic_label(),
            timeout_sec=60,
            backoff_sec=10,
            err_msg="topic label wasn't aggregated",
        )

        # Internally the topic label isn't unaggregated until the total topic count
        # falls below 95% of the aggregation limit. Hence delete enough topics to go
        # under the configured limit, but stay within 5% of it to ensure that the label
        # stays aggregated.
        current_topic_count = len(topics_to_create) + initial_topic_count
        agg_limit_lower_bound = 0.95 * topic_aggregation_limit
        assert (topic_aggregation_limit - agg_limit_lower_bound) > 1
        topic_count_to_delete = int(current_topic_count - agg_limit_lower_bound) - 1
        assert topic_count_to_delete > 1

        self._delete_topics(topics_to_create[:topic_count_to_delete])
        assert not self._aggregated_metrics_have_topic_label()

        # Have topic count fall bellow the 95% limit.
        self._delete_topics(
            topics_to_create[topic_count_to_delete : (topic_count_to_delete + 2)]
        )
        wait_until(
            lambda: self._aggregated_metrics_have_topic_label(),
            timeout_sec=60,
            backoff_sec=10,
            err_msg="topic label wasn't un-aggregated",
        )

        self._create_topics(topics_to_create[: (topic_count_to_delete + 2)])
        wait_until(
            lambda: not self._aggregated_metrics_have_topic_label(),
            timeout_sec=60,
            backoff_sec=10,
            err_msg="topic label wasn't aggregated",
        )

        self.redpanda.set_cluster_config({"topic_label_aggregation_limit": None})
        wait_until(
            lambda: self._aggregated_metrics_have_topic_label(),
            timeout_sec=60,
            backoff_sec=10,
            err_msg="topic label wasn't un-aggregated",
        )

    @cluster(num_nodes=3)
    def test_topic_aggregate_on_toggle(self):
        """
        Topic labels are not aggregated when metrics aggregation is turned off in general.
        This tests that they are aggregated when metrics aggregation is enabled. Assuming
        that the topic count meets the aggregation threshold.
        """
        initial_topic_count = len(self.client().describe_topics())
        topics_to_create = [TopicSpec() for _ in range(50)]
        topic_aggregation_limit = len(topics_to_create) + initial_topic_count - 1

        self.redpanda.set_cluster_config(
            {
                "aggregate_metrics": False,
                "topic_label_aggregation_limit": topic_aggregation_limit,
            }
        )

        self._create_topics(topics_to_create)
        assert self._unaggregated_metrics_have_topic_label(), (
            "topic label shouldn't be aggregated yet"
        )

        self.redpanda.set_cluster_config(
            {
                "aggregate_metrics": True,
            }
        )
        wait_until(
            lambda: not self._aggregated_metrics_have_topic_label(),
            timeout_sec=60,
            backoff_sec=10,
            err_msg="topic label wasn't aggregated",
        )

    @cluster(num_nodes=3)
    def test_topic_aggregate_on_restart(self):
        """
        Tests that topic labels remain aggregated when a node restarts.
        """
        initial_topic_count = len(self.client().describe_topics())
        topics_to_create = [TopicSpec() for _ in range(50)]
        topic_aggregation_limit = len(topics_to_create) + initial_topic_count - 1

        self.redpanda.set_cluster_config(
            {
                "aggregate_metrics": True,
                "topic_label_aggregation_limit": topic_aggregation_limit,
            }
        )

        self._create_topics(topics_to_create)
        wait_until(
            lambda: not self._aggregated_metrics_have_topic_label(),
            timeout_sec=60,
            backoff_sec=10,
            err_msg="topic label wasn't aggregated",
        )

        self.redpanda.rolling_restart_nodes(self.redpanda.nodes)
        wait_until(
            lambda: not self._aggregated_metrics_have_topic_label(),
            timeout_sec=60,
            backoff_sec=10,
            err_msg="topic label wasn't aggregated",
        )
