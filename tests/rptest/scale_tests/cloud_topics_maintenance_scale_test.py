# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from typing import Any

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.util import wait_until_with_progress_check

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierMultiProducer,
    KgoVerifierParams,
)
from rptest.services.admin import Admin
from rptest.services.redpanda import (
    MetricsEndpoint,
    SISettings,
)
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.scale_parameters import ScaleParameters


class CloudTopicsMaintenanceScaleTest(PreallocNodesTest):
    """
    Shared scaffolding for cloud-topics maintenance scale soaks
    """

    topics = ()

    NUM_BROKERS = 9
    NUM_CLIENT_NODES = 3
    NUM_TOPICS = 3
    MSG_SIZE = 4096
    PRODUCE_DURATION_SEC = 10 * 60
    QUIESCE_TIMEOUT_SEC = 10 * 60
    SAMPLE_INTERVAL_SEC = 30

    # Cap partitions per topic so each partition accumulates enough data for
    # maintenance to have meaningful work. With too many partitions, records
    # spread too thinly.
    MAX_PARTITIONS_PER_TOPIC = 256

    # Maintenance-specific cluster config, merged on top of the shared config.
    EXTRA_RP_CONF: dict[str, Any] = {}
    # Maintenance-specific topic config, merged on top of the cloud storage
    # mode every topic gets.
    TOPIC_CONFIG: dict[str, str] = {}

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )

        extra_rp_conf = {
            "enable_cluster_metadata_upload_loop": False,
            **self.EXTRA_RP_CONF,
        }

        environment = {
            "__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON",
        }

        super().__init__(
            test_context,
            num_brokers=self.NUM_BROKERS,
            node_prealloc_count=self.NUM_CLIENT_NODES,
            si_settings=si_settings,
            extra_rp_conf=extra_rp_conf,
            environment=environment,
        )
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    def _metric_sum(self, metric_name: str) -> float:
        return self.redpanda.metric_sum(
            metric_name=metric_name,
            metrics_endpoint=MetricsEndpoint.METRICS,
            expect_metric=True,
        )

    def _get_managed_logs(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_scheduler_managed_log_count"
        )

    def _cluster_healthy(self) -> bool:
        overview = self.admin.get_cluster_health_overview()
        healthy = overview.get("is_healthy", False)
        if not healthy:
            self.logger.warning(f"Cluster unhealthy: {overview}")
        return healthy

    def _create_topics(self, topic_names: list[str], partitions_per_topic: int):
        for name in topic_names:
            self.rpk.create_topic(
                topic=name,
                partitions=partitions_per_topic,
                replicas=3,
                config={
                    TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
                    **self.TOPIC_CONFIG,
                },
            )

    def _partitions_per_topic(self, scale: ScaleParameters) -> int:
        return min(
            self.MAX_PARTITIONS_PER_TOPIC,
            max(1, int(scale.partition_limit / self.NUM_TOPICS)),
        )


class CloudTopicsCompactionScaleTest(CloudTopicsMaintenanceScaleTest):
    """
    Scale test that drives heavy L1 compaction across many partitions on a
    large cluster for a sustained period. Validates that Redpanda doesn't
    fault, OOM, or stall — not that compaction produces perfectly
    deduplicated output.
    """

    KEYS_PER_TOPIC = 10_000
    MIN_COMPACTION_LAG_MS = 5_000
    KEY_MAP_MEMORY = 1024 * 1024  # 1 MB — forces multi-pass at high cardinality
    COMPACTION_INTERVAL_MS = 5_000

    # After quiesce, expect compaction to have removed at least this fraction
    # of the theoretical maximum duplicates (produced - keys_per_topic * partitions).
    MIN_REMOVAL_FRACTION = 0.50

    EXTRA_RP_CONF = {
        "cloud_topics_compaction_interval_ms": COMPACTION_INTERVAL_MS,
        "cloud_topics_compaction_key_map_memory": KEY_MAP_MEMORY,
    }
    TOPIC_CONFIG = {
        "cleanup.policy": TopicSpec.CLEANUP_COMPACT,
        "min.cleanable.dirty.ratio": "0.0",
        "min.compaction.lag.ms": str(MIN_COMPACTION_LAG_MS),
    }

    def _get_log_compactions(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_scheduler_log_compactions"
        )

    def _get_records_removed(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_worker_records_removed"
        )

    @cluster(
        num_nodes=12,
    )
    @skip_debug_mode
    def test_compaction_soak(self):
        scale = ScaleParameters(self.redpanda, replication_factor=3)
        partitions_per_topic = self._partitions_per_topic(scale)

        topic_names = [f"compaction_scale_{i}" for i in range(self.NUM_TOPICS)]
        self.logger.info(
            f"Creating {self.NUM_TOPICS} topics with "
            f"{partitions_per_topic} partitions each "
            f"(partition_limit={scale.partition_limit})"
        )
        self._create_topics(topic_names, partitions_per_topic)

        # Wait for compaction scheduler to pick up all partitions
        expected_managed = partitions_per_topic * self.NUM_TOPICS
        wait_until(
            lambda: self._get_managed_logs() >= expected_managed,
            timeout_sec=120,
            backoff_sec=2,
            err_msg=lambda: (
                f"Compaction scheduler managing "
                f"{self._get_managed_logs()}/{expected_managed} logs"
            ),
        )

        # Rate limit: split across producers, cap at 100 MB/s total
        total_rate = min(100 * 1024 * 1024, scale.expect_bandwidth)
        per_producer_rate = int(total_rate / self.NUM_TOPICS)

        # Large msg_count ceiling — we stop by time, not by completion
        msg_count_ceiling = 100_000_000

        topic_params = [
            KgoVerifierParams(
                topic=name,
                msg_size=self.MSG_SIZE,
                msg_count=msg_count_ceiling,
                key_set_cardinality=self.KEYS_PER_TOPIC,
                rate_limit_bps=per_producer_rate,
                tolerate_failed_produce=True,
            )
            for name in topic_names
        ]

        multi_producer = KgoVerifierMultiProducer(
            self.test_context,
            self.redpanda,
            topic_params,
            custom_node=self.preallocated_nodes,
        )
        multi_producer.start()
        try:
            self.logger.info(
                f"Started {self.NUM_TOPICS} producers, "
                f"rate={per_producer_rate} bytes/s each, "
                f"producing for {self.PRODUCE_DURATION_SEC}s"
            )

            # ── Produce phase: write under concurrent compaction ─────────
            start = time.time()
            wait_until_with_progress_check(
                check=self._get_log_compactions,
                condition=lambda: time.time() - start >= self.PRODUCE_DURATION_SEC,
                timeout_sec=self.PRODUCE_DURATION_SEC + 60,
                progress_sec=self.SAMPLE_INTERVAL_SEC,
                backoff_sec=5,
                err_msg="Compaction stalled during produce phase",
                logger=self.logger,
            )
        finally:
            multi_producer.stop()

        total_produced = sum(p.produce_status.acked for p in multi_producer.producers)
        self.logger.info(
            f"Produce phase done: {total_produced} records produced, "
            f"compactions={self._get_log_compactions()}, "
            f"removed={self._get_records_removed()}"
        )

        # ── Wait for compaction to remove duplicates ─────────────────
        unique_records = self.KEYS_PER_TOPIC * partitions_per_topic * self.NUM_TOPICS
        max_removable = max(1, total_produced - unique_records)
        min_removed = int(max_removable * self.MIN_REMOVAL_FRACTION)

        self.logger.info(
            f"Waiting for compaction to remove >= {min_removed} records "
            f"({self.MIN_REMOVAL_FRACTION:.0%} of {max_removable} duplicates)"
        )

        wait_until_with_progress_check(
            check=self._get_records_removed,
            condition=lambda: self._get_records_removed() >= min_removed,
            timeout_sec=self.QUIESCE_TIMEOUT_SEC,
            progress_sec=self.SAMPLE_INTERVAL_SEC,
            backoff_sec=5,
            err_msg=f"Compaction did not remove enough duplicates (need {min_removed})",
            logger=self.logger,
        )

        final_removed = self._get_records_removed()
        self.logger.info(
            f"Compaction converged: removed={final_removed} "
            f"({final_removed / max_removable:.1%} of duplicates)"
        )

        # Cluster health check
        self.logger.info("Checking cluster health...")
        wait_until(
            self._cluster_healthy,
            timeout_sec=60,
            backoff_sec=5,
            err_msg="Cluster has unavailable partitions after soak test",
        )
        self.logger.info("Cluster healthy — test passed")


class CloudTopicsLevelingScaleTest(CloudTopicsMaintenanceScaleTest):
    """
    Scale test that drives heavy L1 leveling across many partitions on a large
    cluster for a sustained period. Validates that Redpanda doesn't fault, OOM,
    or stall, and that leveling keeps up — folding the many undersized objects
    that fanning writes thinly across hundreds of partitions produces.
    """

    LEVELING_INTERVAL_MS = 5_000
    MAX_CONCURRENT_LEVELING = 8
    # Small objects + a high undersized ratio so nearly every reconciled object
    # is leveling-eligible, giving leveling abundant work to fold.
    RECONCILIATION_MAX_OBJECT_SIZE = 1024 * 1024  # 1 MiB
    MIN_EXTENT_RATIO = 0.9

    PRODUCE_DURATION_SEC = 5 * 60

    EXTRA_RP_CONF = {
        "cloud_topics_leveling_interval_ms": LEVELING_INTERVAL_MS,
        "cloud_topics_max_concurrent_leveling_jobs_per_shard": MAX_CONCURRENT_LEVELING,
        "cloud_topics_reconciliation_max_object_size": RECONCILIATION_MAX_OBJECT_SIZE,
        "cloud_topics_leveling_min_extent_size_ratio": MIN_EXTENT_RATIO,
    }

    def _get_leveling_ranges_completed(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_scheduler_leveling_ranges_completed"
        )

    def _get_extents_reclaimed(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_worker_leveling_extents_reclaimed"
        )

    def _get_leveling_queue_length(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_scheduler_leveling_queue_length"
        )

    @cluster(
        num_nodes=12,
    )
    @skip_debug_mode
    def test_leveling_soak(self):
        scale = ScaleParameters(self.redpanda, replication_factor=3)
        partitions_per_topic = self._partitions_per_topic(scale)
        total_partitions = partitions_per_topic * self.NUM_TOPICS

        topic_names = [f"leveling_scale_{i}" for i in range(self.NUM_TOPICS)]
        self.logger.info(
            f"Creating {self.NUM_TOPICS} topics with "
            f"{partitions_per_topic} partitions each "
            f"({total_partitions} total, partition_limit={scale.partition_limit})"
        )
        self._create_topics(topic_names, partitions_per_topic)

        wait_until(
            lambda: self._get_managed_logs() >= total_partitions,
            timeout_sec=120,
            backoff_sec=2,
            err_msg=lambda: (
                f"Maintenance scheduler managing "
                f"{self._get_managed_logs()}/{total_partitions} logs"
            ),
        )

        # Rate limit: split across producers, cap at 100 MB/s total. Fanning
        # this thinly across hundreds of partitions is what yields undersized
        # objects (each partition reconciles small flushes), i.e. leveling
        # fodder.
        total_rate = min(100 * 1024 * 1024, scale.expect_bandwidth)
        per_producer_rate = int(total_rate / self.NUM_TOPICS)

        # Large msg_count ceiling — we stop by time, not by completion.
        msg_count_ceiling = 100_000_000

        topic_params = [
            KgoVerifierParams(
                topic=name,
                msg_size=self.MSG_SIZE,
                msg_count=msg_count_ceiling,
                rate_limit_bps=per_producer_rate,
                tolerate_failed_produce=True,
            )
            for name in topic_names
        ]

        multi_producer = KgoVerifierMultiProducer(
            self.test_context,
            self.redpanda,
            topic_params,
            custom_node=self.preallocated_nodes,
        )
        try:
            multi_producer.start()
            self.logger.info(
                f"Started {self.NUM_TOPICS} producers, "
                f"rate={per_producer_rate} bytes/s each, "
                f"producing for {self.PRODUCE_DURATION_SEC}s"
            )

            # Produce phase: write while leveling is enabled.
            # Use leveling progress (extents reclaimed) as the liveness check:
            # leveling must keep folding throughout, not stall under load.
            start = time.time()
            wait_until_with_progress_check(
                check=self._get_extents_reclaimed,
                condition=lambda: time.time() - start >= self.PRODUCE_DURATION_SEC,
                timeout_sec=self.PRODUCE_DURATION_SEC + 60,
                progress_sec=self.SAMPLE_INTERVAL_SEC,
                backoff_sec=5,
                err_msg="Leveling stalled during produce phase",
                logger=self.logger,
            )
        finally:
            multi_producer.stop()

        total_produced = sum(p.produce_status.acked for p in multi_producer.producers)
        self.logger.info(
            f"Produce phase done: {total_produced} records produced, "
            f"ranges_completed={self._get_leveling_ranges_completed()}, "
            f"extents_reclaimed={self._get_extents_reclaimed()}"
        )

        # Wait for leveling to converge
        prev = [self._get_extents_reclaimed()]
        stable_since = [time.time()]
        stable_sec = 30

        def converged() -> bool:
            reclaimed = self._get_extents_reclaimed()
            if reclaimed != prev[0]:
                self.logger.info(
                    f"Leveling still folding: extents_reclaimed "
                    f"{prev[0]} -> {reclaimed}"
                )
                prev[0] = reclaimed
                stable_since[0] = time.time()
                return False
            if self._get_leveling_queue_length() != 0:
                return False
            return time.time() - stable_since[0] >= stable_sec

        wait_until(
            converged,
            timeout_sec=self.QUIESCE_TIMEOUT_SEC,
            backoff_sec=5,
            err_msg="Leveling did not converge after writes stopped",
        )

        reclaimed = self._get_extents_reclaimed()
        self.logger.info(
            f"Leveling converged: {reclaimed} extents reclaimed across {total_partitions} partitions"
        )

        assert reclaimed >= total_partitions, (
            f"expected leveling to reclaim >= {total_partitions} extents "
            f"(>= ~1 per partition), got {reclaimed}"
        )

        # Cluster health check.
        self.logger.info("Checking cluster health...")
        wait_until(
            self._cluster_healthy,
            timeout_sec=60,
            backoff_sec=5,
            err_msg="Cluster has unavailable partitions after soak test",
        )
        self.logger.info("Cluster healthy — test passed")
