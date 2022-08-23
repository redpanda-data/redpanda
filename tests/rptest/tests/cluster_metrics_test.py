# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import re

from typing import Optional, Callable
from rptest.util import wait_until_result
from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import TimeoutError

from rptest.clients.rpk import RpkTool
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
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

    def __init__(self, test_context):
        super(ClusterMetricsTest, self).__init__(test_context=test_context)
        self.admin = Admin(self.redpanda)

    def _stop_controller_node(self) -> ClusterNode:
        """
        Stop the current controller node
        """
        prev = self.redpanda.controller()
        self.redpanda.stop_node(prev)

        return prev

    def _wait_until_controller_leader_is_stable(
            self,
            hosts: Optional[list[str]] = None,
            check: Callable[[int],
                            bool] = lambda node_id: True) -> ClusterNode:
        node_id = self.admin.await_stable_leader(topic="controller",
                                                 partition=0,
                                                 namespace="redpanda",
                                                 timeout_s=30,
                                                 check=check,
                                                 hosts=hosts)

        return self.redpanda.get_node(node_id)

    def _restart_controller_node(self) -> ClusterNode:
        """
        Stop and re-start the current controller node. After stopping,
        wait for controller leadership to migrate to a new node before
        proceeding with the re-start.
        """
        prev = self._stop_controller_node()

        started_hosts = [
            n.account.hostname for n in self.redpanda.started_nodes()
        ]

        self._wait_until_controller_leader_is_stable(
            hosts=started_hosts,
            check=lambda node_id: node_id != self.redpanda.idx(prev))

        self.redpanda.start_node(prev)
        return self._wait_until_controller_leader_is_stable()

    def _failover(self) -> ClusterNode:
        """
        Stop current controller node and wait for failover.
        Returns the new stable controller node.
        """
        prev = self._stop_controller_node()

        started_hosts = [
            n.account.hostname for n in self.redpanda.started_nodes()
        ]
        return self._wait_until_controller_leader_is_stable(
            hosts=started_hosts,
            check=lambda node_id: node_id != self.redpanda.idx(prev))

    def _get_value_from_samples(self, samples: MetricSamples):
        """
        Extract the metric value from the samples.
        Only one sample is expected as cluster level metrics have no labels.
        """
        assert len(samples.samples) == 1
        return samples.samples[0].value

    def _assert_cluster_metrics(
            self, node: ClusterNode,
            expect_metrics: bool) -> Optional[dict[str, MetricSamples]]:
        """
        Assert that cluster metrics are reported (or not) from the specified node.
        """
        def get_metrics_from_node_sync(pattern: str):
            samples = self.redpanda.metrics_sample(
                pattern, [node], MetricsEndpoint.PUBLIC_METRICS)
            success = samples is not None
            return success, samples

        def get_metrics_from_node(pattern: str):
            metrics = None

            try:
                metrics = wait_until_result(
                    lambda: get_metrics_from_node_sync(pattern),
                    timeout_sec=1,
                    backoff_sec=.1)
            except TimeoutError as e:
                if expect_metrics:
                    raise e

            return metrics

        metrics_samples = {}
        for name in ClusterMetricsTest.cluster_level_metrics:
            samples = get_metrics_from_node(name)
            if samples is not None:
                metrics_samples[name] = samples

        if expect_metrics:
            assert metrics_samples, f"Missing expected metrics from node {node.name}"
        else:
            assert not metrics_samples, f"Received unexpected metrics from node {node.name}"

    def _assert_reported_by_controller(
            self, current_controller: Optional[ClusterNode]):
        """
        Enforce the fact that only the controller leader should
        report cluster level metrics. If there's no leader, no
        node should report these metrics.
        """

        # Validate the controller metrics first.
        if current_controller is not None:
            self._assert_cluster_metrics(current_controller,
                                         expect_metrics=True)

        # Make sure that followers are not reporting cluster metrics.
        for node in self.redpanda.started_nodes():
            if node == current_controller:
                continue

            self._assert_cluster_metrics(node, expect_metrics=False)

    @cluster(num_nodes=3)
    def cluster_metrics_reported_only_by_leader_test(self):
        """
        Test that only the controller leader reports the cluster
        level metrics at any given time.
        """
        # Assert metrics are reported once in a fresh, three node cluster
        controller = self._wait_until_controller_leader_is_stable()
        self._assert_reported_by_controller(controller)

        # Restart the controller node and assert.
        controller = self._restart_controller_node()
        self._assert_reported_by_controller(controller)

        # Stop the controller node and assert.
        controller = self._failover()
        self._assert_reported_by_controller(controller)

        # Stop the controller node and assert again.
        # This time the metrics should not be reported as a controller
        # couldn't be elected due to lack of quorum.
        self._stop_controller_node()
        self._assert_reported_by_controller(None)

    @cluster(num_nodes=3)
    def cluster_metrics_correctness_test(self):
        """
        Test that the cluster level metrics move in the expected way
        after creating a topic.
        """
        controller = self._wait_until_controller_leader_is_stable()
        self._assert_reported_by_controller(controller)

        cluster_metrics = MetricCheck(
            self.logger,
            self.redpanda,
            controller,
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
        controller = self._wait_until_controller_leader_is_stable()
        self._assert_reported_by_controller(controller)

        self.redpanda.set_cluster_config({"disable_public_metrics": "true"},
                                         expect_restart=True)

        # The 'public_metrics' endpoint that serves cluster level
        # metrics should not return anything when
        # 'disable_public_metrics' == true
        controller = self._wait_until_controller_leader_is_stable()
        self._assert_cluster_metrics(controller, expect_metrics=False)
