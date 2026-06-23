# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import math
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from ducktape.utils.util import wait_until

from ducktape.cluster.cluster import ClusterNode
from ducktape.tests.test import TestContext
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    RESTART_LOG_ALLOW_LIST,
    MetricsEndpoint,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import repeat_check, wait_until_result


# Single source of truth for the redpanda_cluster_health_* / vectorized_cluster_health_*
# metrics exposed by CORE-16394. Each entry encodes the metric's short name
# (suffix after the prefix), its kind (status-contributing vs verdict vs
# informational), an optional predictor that derives the expected value
# from the admin GET /v1/cluster/health_overview response, and an optional
# constraint for cases where we can't predict exactly. All groupings the
# test cares about (status-only, all metrics, etc.) are derived from this
# table.
class _MetricKind(Enum):
    INPUT_STATUS = "input_status"  # contributes to unhealthy_reasons
    VERDICT = "verdict"  # the final unhealthy_reasons gauge
    INFORMATIONAL = "informational"  # exposed but does not gate health


HovDict = dict[str, Any]
Predictor = Callable[[HovDict], float]
Constraint = Callable[[float], bool]


def _nonneg(v: float) -> bool:
    return v >= 0


@dataclass(frozen=True)
class _HealthMetric:
    suffix: str
    kind: _MetricKind
    # Returns the expected metric value derived from the admin endpoint
    # response. None when we can't predict exactly (in which case
    # `constraint` is used).
    predictor: Predictor | None = None
    # Looser validation when `predictor` is None. Default: value >= 0.
    constraint: Constraint = field(default=_nonneg)


def _expected_no_elected_controller(hov: HovDict) -> float:
    # The admin endpoint emits controller_id=-1 when no leader is elected.
    controller_id = hov.get("controller_id")
    return 0 if controller_id is not None and controller_id >= 0 else 1


_HEALTH_METRICS: list[_HealthMetric] = [
    # Status inputs that gate cluster health.
    _HealthMetric(
        "nodes_down", _MetricKind.INPUT_STATUS, predictor=lambda h: len(h["nodes_down"])
    ),
    _HealthMetric(
        "high_disk_usage_nodes",
        _MetricKind.INPUT_STATUS,
        predictor=lambda h: len(h["high_disk_usage_nodes"]),
    ),
    _HealthMetric(
        "leaderless_partitions",
        _MetricKind.INPUT_STATUS,
        predictor=lambda h: int(h["leaderless_count"]),
    ),
    _HealthMetric(
        "under_replicated_partitions",
        _MetricKind.INPUT_STATUS,
        predictor=lambda h: int(h["under_replicated_count"]),
    ),
    _HealthMetric(
        "no_elected_controller",
        _MetricKind.INPUT_STATUS,
        predictor=_expected_no_elected_controller,
    ),
    # no_health_report is hard to deterministically trigger from a
    # ducktape test; we can't predict the exact value, only that it's
    # a 0/1 gauge. It still counts toward the unhealthy_reasons invariant.
    _HealthMetric(
        "no_health_report", _MetricKind.INPUT_STATUS, constraint=lambda v: v in (0, 1)
    ),
    # Final verdict: the size of unhealthy_reasons.
    _HealthMetric(
        "unhealthy_reasons",
        _MetricKind.VERDICT,
        predictor=lambda h: len(h["unhealthy_reasons"]),
    ),
    # Informational metrics that do NOT gate cluster health.
    _HealthMetric(
        "nodes_in_recovery_mode",
        _MetricKind.INFORMATIONAL,
        predictor=lambda h: len(h["nodes_in_recovery_mode"]),
    ),
    _HealthMetric(
        "cluster_size",
        _MetricKind.INFORMATIONAL,
        predictor=lambda h: len(h["all_nodes"]),
    ),
    _HealthMetric("bytes_in_cloud_storage", _MetricKind.INFORMATIONAL),
]

_INPUT_STATUS_METRICS = [
    m for m in _HEALTH_METRICS if m.kind == _MetricKind.INPUT_STATUS
]
_VERDICT_METRIC = next(m for m in _HEALTH_METRICS if m.kind == _MetricKind.VERDICT)
_ALL_HEALTH_METRICS = [m.suffix for m in _HEALTH_METRICS]

_PUBLIC_PREFIX = "redpanda_cluster_health_"
_INTERNAL_PREFIX = "vectorized_cluster_health_"


class ClusterHealthOverviewTestBase(RedpandaTest):
    def __init__(self, test_context: TestContext, num_brokers: int):
        super().__init__(
            test_context=test_context,
            num_brokers=num_brokers,
            extra_rp_conf={
                # The metric refresh interval is 10x this, i.e. 1s, kept small
                # so the metric cache tracks the admin endpoint within the
                # test's wait windows.
                "health_monitor_max_metadata_age": 100,  # ms
                "health_monitor_metrics_enabled": True,
                # Work around bug where leadership transfers cause bad health reports
                # https://github.com/redpanda-data/redpanda/issues/5253
                "enable_leader_balancer": False,
            },
            environment={"__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"},
        )

        self.admin = Admin(self.redpanda)

    def create_topics(self):
        topics = []
        for i in range(0, 8):
            topics.append(
                TopicSpec(partition_count=random.randint(1, 6), replication_factor=3)
            )
        for i in range(0, 8):
            topics.append(
                TopicSpec(partition_count=random.randint(1, 6), replication_factor=1)
            )
        self.client().create_topic(topics)
        return topics

    def get_health(self, node: ClusterNode | None = None):
        """Wrapper around admin.get_cluster_health_overview which validates some invariants
        about each health report, and cross-checks the
        redpanda_cluster_health_* Prometheus metrics against the admin
        endpoint response.

        Every node emits the cluster_health_* gauges; the cross-check
        compares the response from whichever node we queried against the
        metrics scraped from that same node, so we exercise non-controller
        emitters too.
        """

        # Pin the admin call to a specific started node so we can scrape
        # metrics from the same node for the cross-check. Mirrors the
        # node selection that admin._request does internally for node=None.
        if node is None:
            started = self.redpanda.started_nodes()
            assert started, "no started nodes to query"
            node = random.choice(started)

        hov = self.admin.get_cluster_health_overview(node=node)

        # these invariants should always hold
        if hov["is_healthy"]:
            assert len(hov["nodes_down"]) == 0
            assert len(hov["leaderless_partitions"]) == 0
            assert hov["leaderless_count"] == 0
            assert len(hov["under_replicated_partitions"]) == 0
            assert hov["under_replicated_count"] == 0
            assert len(hov["high_disk_usage_nodes"]) == 0
            assert len(hov["unhealthy_reasons"]) == 0
            assert len(hov["all_nodes"]) > 0
        else:
            assert len(hov["unhealthy_reasons"]) > 0
            # these next two are true just because we don't go over the max of 128
            # reported partitions in these tests
            assert len(hov["leaderless_partitions"]) == hov["leaderless_count"]
            assert (
                len(hov["under_replicated_partitions"]) == hov["under_replicated_count"]
            )

        # Cross-check the metrics emission against the admin endpoint on
        # the same node.
        self._check_health_metrics(node)

        return hov

    def _scrape_health_metrics(
        self,
        node: ClusterNode,
        endpoint: MetricsEndpoint,
    ) -> dict[str, tuple[float, dict[str, str]]]:
        """Scrape every redpanda_cluster_health_* metric from the given
        node and endpoint. Returns a {short_name: (value, labels)} dict.
        Short name is the metric name with the endpoint-specific prefix
        stripped. Every node emits exactly one sample per metric (shard 0
        only).
        """
        prefix = (
            _PUBLIC_PREFIX
            if endpoint == MetricsEndpoint.PUBLIC_METRICS
            else _INTERNAL_PREFIX
        )
        full_names = [prefix + s for s in _ALL_HEALTH_METRICS]

        def try_scrape() -> tuple[bool, dict[str, tuple[float, dict[str, str]]]]:
            scraped: dict[str, tuple[float, dict[str, str]]] = {}
            samples_by_name = self.redpanda.metrics_samples(
                nodes=[node],
                metrics_endpoint=endpoint,
                names=full_names,
            )
            for short_name, full_name in zip(_ALL_HEALTH_METRICS, full_names):
                ms = samples_by_name.get(full_name)
                # The overview metrics aren't registered until the probe
                # has cached its first real overview; treat absence as
                # "not ready yet".
                if ms is None or not ms.samples:
                    return False, {}
                s = ms.samples[0]
                scraped[short_name] = (s.value, s.labels)
            return True, scraped

        return wait_until_result(
            try_scrape,
            timeout_sec=15,
            backoff_sec=1,
            err_msg=(
                f"Timed out waiting for cluster health metrics on {endpoint} "
                f"endpoint of node {node.account.hostname}"
            ),
        )

    def _check_health_metrics(self, node: ClusterNode) -> None:
        # The admin endpoint and the metric scrape are two independent
        # network round-trips; an in-flight async update can publish a
        # snapshot of a slightly different cluster state between them.
        # Re-issue the matched pair on the SAME node and retry briefly to
        # absorb the race. If the node was stopped between selection and
        # the call, re-pick from currently-started nodes on retry.
        deadline = time.time() + 15
        while True:
            try:
                node_hov = self.admin.get_cluster_health_overview(node=node)
                metrics = self._scrape_health_metrics(
                    node, MetricsEndpoint.PUBLIC_METRICS
                )
                self._assert_metrics_match(node_hov, metrics)
                return
            except (AssertionError, ConnectionError, TimeoutError):
                if time.time() >= deadline:
                    raise
                started = self.redpanda.started_nodes()
                if started:
                    node = random.choice(started)
                time.sleep(0.5)

    def _assert_metrics_match(
        self,
        hov: dict[str, Any],
        metrics: dict[str, tuple[float, dict[str, str]]],
    ) -> None:
        for m in _HEALTH_METRICS:
            actual, _ = metrics[m.suffix]
            assert not math.isnan(actual), f"metric {m.suffix} should not be NaN"
            if m.predictor is not None:
                expected = m.predictor(hov)
                assert actual == expected, (
                    f"metric {m.suffix}={actual} but admin endpoint implies "
                    f"{expected} (hov={hov})"
                )
            else:
                assert m.constraint(actual), (
                    f"metric {m.suffix}={actual} failed constraint"
                )

        # Verdict invariant: unhealthy_reasons equals the count of input-
        # status metrics whose value is >0 (including no_health_report).
        unhealthy_metric = int(metrics[_VERDICT_METRIC.suffix][0])
        nonzero_inputs = sum(
            1 for m in _INPUT_STATUS_METRICS if metrics[m.suffix][0] > 0
        )
        assert unhealthy_metric == nonzero_inputs, (
            f"{_VERDICT_METRIC.suffix} metric={unhealthy_metric} but counted "
            f"{nonzero_inputs} input-status metrics >0 (metrics={metrics})"
        )

        # And cross-check the verdict against the admin endpoint's
        # unhealthy_reasons list length.
        assert unhealthy_metric == len(hov["unhealthy_reasons"]), (
            f"{_VERDICT_METRIC.suffix} metric={unhealthy_metric} but admin "
            f"endpoint reports {len(hov['unhealthy_reasons'])} reasons "
            f"({hov['unhealthy_reasons']})"
        )

    def wait_until_healthy(self) -> HovDict:
        """Wait until a node reports the cluster healthy and return that
        healthy overview. Note this is one node's view at one moment;
        another node may still serve a stale (or fresher) view afterwards.
        """

        def is_healthy() -> tuple[bool, HovDict]:
            res = self.get_health()
            ok = res["is_healthy"] == True and len(res["all_nodes"]) == len(
                self.redpanda.nodes
            )
            return ok, res

        return wait_until_result(is_healthy, 30, 2)


class ClusterHealthOverviewTest(ClusterHealthOverviewTestBase):
    def __init__(self, test_context: TestContext):
        super().__init__(test_context, num_brokers=5)

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def cluster_health_overview_baseline_test(self):
        topics = self.create_topics()

        # in initial state after all nodes joined cluster should be healthy
        self.wait_until_healthy()

        # Stop the current controller specifically. This deterministically
        # forces a controller-down -> re-election transition, so
        # _check_health_metrics exercises both the brief no-controller
        # window (canonical = lowest alive id, controller=1 nowhere) AND
        # the post-election state (both labels true on the new controller).
        hov = self.get_health()
        controller_id = hov["controller_id"]
        assert controller_id is not None and controller_id >= 0, (
            f"expected an elected controller before first stop, got {hov}"
        )
        first_down = self.redpanda.get_node_by_id(controller_id)
        assert first_down is not None, (
            f"could not resolve controller node from id {controller_id}"
        )
        self.redpanda.stop_node(first_down)

        # when one node is down some partitions with replication factor of 1
        # should be reported as leaderless
        rf_1_topics = {spec.name for spec in topics if spec.replication_factor == 1}

        @repeat_check(5)  # wait for leaderhip and leaderlessness to stabilize
        def one_node_down():
            hov = self.get_health()
            if not hov["is_healthy"] and len(hov["nodes_down"]) > 0:
                # when the health report flips to not healthy, we check that
                # the expected node is reported as down and unhealthy reasons line up
                assert [self.redpanda.idx(first_down)] == hov["nodes_down"]
                # next check is "in" instead of "==" because we may also have under_replicated_partitions
                assert "nodes_down" in hov["unhealthy_reasons"]
                assert len(hov["leaderless_partitions"]) > 0
                assert hov["leaderless_count"] == len(hov["leaderless_partitions"])
                # Only rf=1 topics should be leaderless after one node is stopped
                if any(
                    ntp.split("/")[1] not in rf_1_topics
                    for ntp in hov["leaderless_partitions"]
                ):
                    return False
                return True, hov
            return False, None

        wait_until_result(one_node_down, 30, 2)

        # stop another node, cluster should start reporting leaderless
        # partitions with two out of five nodes down

        second_down = random.choice(self.redpanda.nodes)
        while self.redpanda.idx(second_down) == self.redpanda.idx(first_down):
            second_down = random.choice(self.redpanda.nodes)

        self.redpanda.stop_node(second_down)

        @repeat_check(5)  # wait for leaderhip and leaderlessness to stabilize
        def two_nodes_down():
            hov = self.get_health()
            if hov["is_healthy"] or len(hov["nodes_down"]) != 2:
                return False

            if len(hov["leaderless_partitions"]) == 0:
                return False

            contains_rf_3_topics = not all(
                ntp.split("/")[1] in rf_1_topics for ntp in hov["leaderless_partitions"]
            )

            assert "leaderless_partitions" in hov["unhealthy_reasons"]

            return contains_rf_3_topics

        wait_until(two_nodes_down, 30, 2)

        # restart both nodes, cluster should be healthy back again
        self.redpanda.start_node(first_down)
        self.redpanda.start_node(second_down)

        self.wait_until_healthy()

    @cluster(
        num_nodes=5, log_allow_list=[".*cluster - storage space alert: free space.*"]
    )
    def cluster_health_overview_disk_usage_alert_test(self):
        # Test that high_disk_usage_nodes is reported correctly
        self.create_topics()
        self.wait_until_healthy()

        # Fake alert
        self.redpanda.set_cluster_config(
            {"storage_space_alert_free_threshold_percent": 100}
        )

        def ensure_high_disk_usage_reported(node: ClusterNode):
            hov = self.get_health(node=node)
            return (
                not hov["is_healthy"]
                and "high_disk_usage_nodes" in hov["unhealthy_reasons"]
                and len(hov["high_disk_usage_nodes"]) == 5
            )

        def ensure_unhealthy_report():
            return all(
                ensure_high_disk_usage_reported(node)
                for node in self.redpanda.started_nodes()
            )

        wait_until(ensure_unhealthy_report, 30, 2)

        # Disable disk usage alert
        self.redpanda.set_cluster_config(
            {"storage_space_alert_free_threshold_percent": 5}
        )
        self.wait_until_healthy()


class ClusterHealthMetricsTest(ClusterHealthOverviewTestBase):
    def __init__(self, test_context: TestContext):
        super().__init__(test_context, num_brokers=4)

    @cluster(num_nodes=4)
    def cluster_health_metrics_test(self):
        """Smoke test for the redpanda_cluster_health_* metrics. Asserts
        that on a healthy cluster every status metric is 0, the metrics
        are registered on both the public and internal endpoints, and
        that the values from both endpoints agree."""

        self.create_topics()

        # Use the healthy overview returned by the wait itself: re-reading
        # health from another (randomly chosen) node right after the wait
        # can race with that node's own cache and flap back to unhealthy.
        hov = self.wait_until_healthy()

        controller_id = hov["controller_id"]
        assert controller_id is not None
        controller_node = self.redpanda.get_node_by_id(controller_id)
        assert controller_node is not None

        # Each node refreshes its metric cache on the metrics refresh
        # interval (10x health_monitor_max_metadata_age), so the controller's
        # gauges may still reflect a pre-healthy snapshot (e.g. a
        # partition that was briefly leaderless during topic creation)
        # even though wait_until_healthy() saw a healthy cluster,
        # possibly on another node. Retry until the controller's cache
        # catches up.
        wait_until(
            lambda: self._assert_healthy_metrics(controller_node),
            timeout_sec=20,
            backoff_sec=1,
            err_msg="timed out waiting for healthy cluster_health_* metrics",
            retry_on_exc=True,
        )

    def _assert_healthy_metrics(self, controller_node: ClusterNode) -> bool:
        public_metrics = self._scrape_health_metrics(
            controller_node, MetricsEndpoint.PUBLIC_METRICS
        )
        internal_metrics = self._scrape_health_metrics(
            controller_node, MetricsEndpoint.METRICS
        )

        # Both endpoints must expose every metric.
        for name in _ALL_HEALTH_METRICS:
            assert name in public_metrics, (
                f"metric {_PUBLIC_PREFIX}{name} missing on public endpoint"
            )
            assert name in internal_metrics, (
                f"metric {_INTERNAL_PREFIX}{name} missing on internal endpoint"
            )

        # And the values must agree between the two endpoints.
        for name in _ALL_HEALTH_METRICS:
            pub_val, _ = public_metrics[name]
            internal_val, _ = internal_metrics[name]
            assert pub_val == internal_val, (
                f"metric {name} disagrees between endpoints: "
                f"public={pub_val} internal={internal_val}"
            )

        # All status/verdict metrics should be 0 in a healthy cluster.
        for m in _HEALTH_METRICS:
            if m.kind == _MetricKind.INFORMATIONAL:
                continue
            value, _ = public_metrics[m.suffix]
            assert value == 0, f"healthy cluster: expected {m.suffix} == 0, got {value}"

        # cluster_size matches broker count.
        assert public_metrics["cluster_size"][0] == len(self.redpanda.nodes)
        assert public_metrics["nodes_in_recovery_mode"][0] == 0
        assert public_metrics["bytes_in_cloud_storage"][0] >= 0
        return True
