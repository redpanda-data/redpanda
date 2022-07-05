# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import re

from typing import Optional
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster import ClusterNode

from rptest.clients.rpk import RpkTool
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.metrics_check import MetricCheck
from rptest.services.redpanda import MetricSamples, MetricsEndpoint


class ClusterMetricsTest(RedpandaTest):
    cluster_level_metrics: list[str] = [
        "cluster_brokers",
        "cluster_topics",
        "cluster_partitions",
        "cluster_unavailable_partitions",
    ]

    def _stop_controller_node(self) -> ClusterNode:
        """
        Stop the current controller node
        """
        prev = self.redpanda.controller()
        self.redpanda.stop_node(prev)

        return prev

    def _wait_until_controller_leader_is_stable(self):
        """
        Wait for the controller leader to stabilise.
        This helper considers the leader stable if the same node
        is reported by two consecutive admin API queries.
        """
        prev = None

        def controller_stable():
            nonlocal prev
            curr = self.redpanda.controller()

            if prev != curr:
                prev = curr
                return False
            else:
                return True

        wait_until(
            controller_stable,
            timeout_sec=10,
            backoff_sec=2,
            err_msg="Controller leader did not stabilise",
        )

    def _failover(self):
        """
        Stop current controller node and wait for failover
        """
        prev = self._stop_controller_node()

        def new_controller_elected():
            curr = self.redpanda.controller()
            return curr and curr != prev

        wait_until(
            new_controller_elected,
            timeout_sec=20,
            backoff_sec=1,
            err_msg="Controller did not failover",
        )

        return prev

    def _get_value_from_samples(self, samples: MetricSamples):
        """
        Extract the metric value from the samples.
        Only one sample is expected as cluster level metrics have no labels.
        """
        assert len(samples.samples) == 1
        return samples.samples[0].value

    def _get_cluster_metrics(
            self, node: ClusterNode) -> Optional[dict[str, MetricSamples]]:
        """
        Get all the cluster level metrics exposed by a specified
        node in the cluster.
        """
        def get_metrics_from_node(pattern: str):
            return self.redpanda.metrics_sample(pattern, [node],
                                                MetricsEndpoint.PUBLIC_METRICS)

        metrics_samples = {}
        for name in ClusterMetricsTest.cluster_level_metrics:
            samples = get_metrics_from_node(name)
            if samples is not None:
                metrics_samples[name] = samples

        if not metrics_samples:
            return None
        else:
            return metrics_samples

    def _assert_reported_by_controller(self):
        """
        Enforce the fact that only the controller leader should
        report cluster level metrics. If there's no leader, no
        node should report these metrics.
        """
        current_controller = self.redpanda.controller()
        for node in self.redpanda.started_nodes():
            metrics = self._get_cluster_metrics(node)

            if current_controller is None:
                assert (
                    metrics is None
                ), f"Node {node.name} reported cluster metrics, but the cluster has no leader"
            elif current_controller == node:
                assert (
                    metrics is not None
                ), f"Node {node.name} is controller leader, but did not report cluster metrics"
            else:
                assert (
                    metrics is None
                ), f"Node {node.name} is not controller leader, but it reported cluster metrics"

    @cluster(num_nodes=3)
    def cluster_metrics_reported_only_by_leader_test(self):
        """
        Test that only the controller leader reports the cluster
        level metrics at any given time.
        """
        # Assert metrics are reported once in a fresh, three node cluster
        self._assert_reported_by_controller()

        # Restart the controller node and assert.
        controller = self.redpanda.controller()
        self.redpanda.restart_nodes([controller],
                                    start_timeout=10,
                                    stop_timeout=10)
        self._wait_until_controller_leader_is_stable()
        self._assert_reported_by_controller()

        # Stop the controller node and assert.
        self._failover()
        self._assert_reported_by_controller()

        # Stop the controller node and assert again.
        # This time the metrics should not be reported as a controller
        # couldn't be elected due to lack of quorum.
        self._stop_controller_node()
        self._assert_reported_by_controller()

    @cluster(num_nodes=3)
    def cluster_metrics_correctness_test(self):
        """
        Test that the cluster level metrics move in the expected way
        after creating a topic.
        """
        self._assert_reported_by_controller()

        controller_node = self.redpanda.controller()
        cluster_metrics = MetricCheck(
            self.logger,
            self.redpanda,
            controller_node,
            re.compile("redpanda_cluster_.*"),
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)

        RpkTool(self.redpanda).create_topic("test-topic", partitions=3)

        # Check that the metrics have moved in the expected way by the creation
        # of one topic with three partitions.
        cluster_metrics.expect([
            ("redpanda_cluster_brokers", lambda a, b: a == b == 3),
            ("redpanda_cluster_topics", lambda a, b: b - a == 1),
            ("redpanda_cluster_partitions", lambda a, b: b - a == 3),
            ("redpanda_cluster_unavailable_partitions",
             lambda a, b: a == b == 0)
        ])

    @cluster(num_nodes=3)
    def cluster_metrics_disabled_by_config_test(self):
        """
        Test that the cluster level metrics have the expected values
        before and after creating a topic.
        """
        # 'disable_public_metrics' defaults to false so cluster metrics
        # are expected
        self._assert_reported_by_controller()

        self.redpanda.set_cluster_config({"disable_public_metrics": "true"},
                                         expect_restart=True)

        # The 'public_metrics' endpoint that serves cluster level
        # metrics should not return anything when
        # 'disable_public_metrics' == true
        cluster_metrics = self._get_cluster_metrics(self.redpanda.controller())
        assert cluster_metrics is None
