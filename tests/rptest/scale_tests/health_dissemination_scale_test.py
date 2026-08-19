# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, LoggingConfig
from rptest.services.redpanda_installer import (
    RedpandaInstaller,
    wait_for_num_versions,
)
from rptest.tests.redpanda_test import RedpandaTest


class HealthDisseminationScaleTest(RedpandaTest):
    """Compare internal RPC bandwidth between legacy and new health
    dissemination paths on a 10-node cluster with many dormant partitions.
    To be deleted after new major release is out"""

    FEATURE_NAME = "health_dissemination"
    MEASUREMENT_SECONDS = 120
    NUM_TOPICS = 20
    PARTITIONS_PER_TOPIC = 50

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context=test_context,
            num_brokers=10,
            extra_rp_conf={
                "enable_leader_balancer": False,
                "partition_autobalancing_mode": "off",
            },
            log_config=LoggingConfig("info", {"cluster": "debug"}),
        )
        self.admin = Admin(self.redpanda)
        self.installer = self.redpanda._installer

    def setUp(self):
        self.old_version = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD
        )
        # Install old version on first node before cluster starts — prevents
        # downgrade assertion failure.
        self.installer.install([self.redpanda.nodes[0]], self.old_version)
        super().setUp()

    METRIC_NAMES = [
        "vectorized_internal_rpc_sent_bytes",
        "vectorized_internal_rpc_received_bytes",
        "vectorized_internal_rpc_requests_completed",
    ]

    def _get_rpc_stats(self) -> dict[str, int]:
        """Query internal RPC bytes and request counts summed across all
        nodes."""
        metrics = self.redpanda.metrics_samples(self.METRIC_NAMES)

        def total(name: str) -> int:
            return int(sum(s.value for s in metrics[name].samples))

        return {
            "sent": total("vectorized_internal_rpc_sent_bytes"),
            "recv": total("vectorized_internal_rpc_received_bytes"),
            "requests": total("vectorized_internal_rpc_requests_completed"),
        }

    def _measure_rpc_stats(self, label: str) -> dict[str, int]:
        """Measure RPC bytes and request counts over MEASUREMENT_SECONDS."""
        before = self._get_rpc_stats()
        self.logger.info(
            f"[{label}] Sleeping {self.MEASUREMENT_SECONDS}s to measure "
            f"RPC stats (baseline: {before})"
        )
        time.sleep(self.MEASUREMENT_SECONDS)
        after = self._get_rpc_stats()
        delta = {k: after[k] - before[k] for k in before}
        secs = self.MEASUREMENT_SECONDS
        self.logger.info(
            f"[{label}] RPC delta over {secs}s: "
            f"sent={delta['sent']} ({delta['sent'] / secs:.0f} B/s), "
            f"recv={delta['recv']} ({delta['recv'] / secs:.0f} B/s), "
            f"requests={delta['requests']} ({delta['requests'] / secs:.1f}/s)"
        )
        return delta

    def _wait_until_healthy(self):
        def is_healthy():
            hov = self.admin.get_cluster_health_overview()
            return (
                hov["is_healthy"]
                and hov["controller_id"] >= 0
                and len(hov["all_nodes"]) == 10
                and len(hov["nodes_down"]) == 0
            )

        wait_until(is_healthy, timeout_sec=60, backoff_sec=2)

    @cluster(num_nodes=10, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_rpc_bandwidth_comparison(self):
        """Start one node on old version (legacy path), measure RPC,
        then upgrade to new version (dissemination path) and compare."""

        first_node = self.redpanda.nodes[0]

        # setUp() already installed old version on first_node; cluster is up
        # in mixed-version mode.
        wait_for_num_versions(self.redpanda, 2)

        # Create dormant topics
        topics = [
            TopicSpec(
                name=f"topic-{i:03d}",
                partition_count=self.PARTITIONS_PER_TOPIC,
                replication_factor=3,
            )
            for i in range(self.NUM_TOPICS)
        ]
        self.client().create_topic(topics)

        self._wait_until_healthy()

        # Verify legacy path is active (feature should NOT be active).
        # The old binary doesn't know about the feature at all, so
        # supports_feature() may raise KeyError — that's fine, it means
        # the feature is definitely not active.
        try:
            feature_active = self.admin.supports_feature(self.FEATURE_NAME)
        except KeyError:
            feature_active = False
        assert not feature_active, (
            f"Feature {self.FEATURE_NAME} should NOT be active in mixed-version cluster"
        )

        # Phase 1: measure legacy path
        legacy_delta = self._measure_rpc_stats("legacy")

        # Phase 2: upgrade the old node to HEAD
        self.logger.info("Upgrading old node to HEAD")
        self.installer.install([first_node], RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes([first_node])

        wait_for_num_versions(self.redpanda, 1)

        wait_until(
            lambda: self.admin.supports_feature(self.FEATURE_NAME),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Timeout waiting for {self.FEATURE_NAME} feature",
        )

        self._wait_until_healthy()

        # Phase 2: measure new path
        new_delta = self._measure_rpc_stats("dissemination")

        # Phase 3: baseline — effectively disable health collection by setting
        # an extremely high max metadata age (10 min). This measures the RPC
        # floor from heartbeats, raft, controller, etc. — everything except
        # health monitor traffic.
        self.logger.info(
            "Setting health_monitor_max_metadata_age to 600000ms "
            "(10 min) to disable health collection"
        )
        self.redpanda.set_cluster_config(
            {
                "health_monitor_max_metadata_age": 600000,
            }
        )
        # Let a few seconds pass so any in-flight health collection finishes.
        time.sleep(15)

        baseline_delta = self._measure_rpc_stats("baseline (health disabled)")

        # Health-only overhead = total - baseline
        legacy_health_sent = legacy_delta["sent"] - baseline_delta["sent"]
        new_health_sent = new_delta["sent"] - baseline_delta["sent"]

        self.logger.info(
            f"=== RPC Comparison (over {self.MEASUREMENT_SECONDS}s) ===\n"
            f"  {'':20s} {'baseline':>12s} {'legacy':>12s} {'new':>12s}\n"
            f"  {'sent bytes':20s} {baseline_delta['sent']:>12}  {legacy_delta['sent']:>12}  {new_delta['sent']:>12}\n"
            f"  {'recv bytes':20s} {baseline_delta['recv']:>12}  {legacy_delta['recv']:>12}  {new_delta['recv']:>12}\n"
            f"  {'requests':20s} {baseline_delta['requests']:>12}  {legacy_delta['requests']:>12}  {new_delta['requests']:>12}\n"
            f"  Health-only sent:  legacy={legacy_health_sent}  new={new_health_sent}\n"
            f"  Health sent reduction: "
            f"{(1 - new_health_sent / max(legacy_health_sent, 1)) * 100:.1f}%"
        )

        assert legacy_health_sent > 0, (
            f"Legacy health overhead should be positive, got {legacy_health_sent}"
        )
        reduction = 1 - new_health_sent / legacy_health_sent
        assert reduction >= 0.10, (
            f"Expected at least 10% reduction in health traffic, "
            f"got {reduction * 100:.1f}% "
            f"(legacy={legacy_health_sent}, new={new_health_sent})"
        )
