# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
End-to-end coverage for ctp_stm local retention in cloud-topic partitions
(see docs/superpowers/specs/2026-05-28-ctp-stm-local-retention-design.md).

min_allowed_local_threshold (MASH) is a kafka-offset floor set by L1 compaction:
below it, local data is non-authoritative and ctp_stm housekeeping is free to
evict. For cleanup.policy=delete + storage.mode=tiered_cloud, L1 compaction
does not run, so MASH stays unset; the cap on local data comes from the storage
layer (`compute_local_retention_offset`, which folds in strict retention +
retention.local.target.bytes + cloud_gc_offset) and is driven by ctp_stm
housekeeping.

These tests verify the resulting observable behavior:
- tiered_cloud + delete + strict: local footprint converges near
  retention.local.target.bytes (storage-layer retention path).
- Flipping back to storage.mode=cloud short-circuits the storage-layer query
  in ctp_stm and collapses local data to LRO.
- Cluster-wide disk pressure (via _cloud_gc_offset) reclaims past the local
  target.
- Switching cleanup.policy to compact causes L1 compaction to run, which
  advances MASH; ctp_stm housekeeping then evicts up to the new floor.
"""

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.cloud_topics.e2e_test import EndToEndCloudTopicsBase


class TieredCloudLocalRetentionTest(EndToEndCloudTopicsBase):
    """
    Verify that tiered_cloud partitions honor retention.local.target.bytes
    (via the storage-layer retention path consulted by ctp_stm), that
    flipping to cloud mode or applying disk pressure evicts aggressively,
    and that enabling compaction advances MASH via L1 compaction.
    """

    topic_name = "tc_local_retention"

    # Override base class topics - we create them per test with custom configs.
    topics = ()

    # Sizes are deliberately small so the test runs quickly. The local
    # target is a few segments; retention.bytes is large enough that
    # regular size-based retention does not trigger.
    segment_size = 1 * 1024 * 1024  # 1 MiB
    local_target_bytes = 4 * segment_size  # 4 MiB
    retention_bytes = 1024 * 1024 * 1024  # 1 GiB - effectively unlimited
    bytes_to_produce = 32 * segment_size  # 32 MiB - well above local target
    msg_size = 16 * 1024  # 16 KiB

    def __init__(self, test_context: TestContext):
        extra_rp_conf = {
            # Fast reconciliation so L0->L1 movement happens quickly.
            "cloud_topics_reconciliation_min_interval": 1000,  # 1s
            "cloud_topics_reconciliation_max_interval": 2000,  # 2s
            # Fast L1 compaction so cleanup.policy=compact tests observe
            # MASH advance within the test timeout.
            "cloud_topics_compaction_interval_ms": 5000,  # 5s
            # Fast housekeeping/GC so prefix-truncate and local eviction
            # happen on test timescales.
            "cloud_storage_housekeeping_interval_ms": 2000,  # 2s
            "log_compaction_interval_ms": 2000,  # 2s
            # Use the configured segment size for all topics.
            "log_segment_size": self.segment_size,
            "log_segment_size_min": self.segment_size,
            # Short trim interval so the space manager's control loop
            # fires within the lifetime of fast tests.
            "retention_local_trim_interval": 2000,  # 2s
            # ctp_stm consults compute_local_retention_offset, which only
            # caps local data at retention.local.target.bytes when strict
            # retention is engaged (mirrors
            # disk_log_impl::maybe_apply_local_storage_overrides).
            "retention_local_strict": True,
            "retention_local_strict_override": True,
        }

        super().__init__(
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
        )

    def setUp(self):
        # Start the cluster with cloud topics enabled. We will create our
        # own topics inside each test so we can pick storage_mode and
        # cleanup.policy explicitly.
        assert self.redpanda
        self.redpanda.start()
        self.redpanda.set_feature_active("tiered_cloud_topics", True, timeout_sec=30)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _create_topic(
        self,
        storage_mode: str,
        cleanup_policy: str = TopicSpec.CLEANUP_DELETE,
        partitions: int = 1,
    ):
        rpk = RpkTool(self.redpanda)
        config = {
            TopicSpec.PROPERTY_STORAGE_MODE: storage_mode,
            "cleanup.policy": cleanup_policy,
            "retention.bytes": str(self.retention_bytes),
            "retention.local.target.bytes": str(self.local_target_bytes),
            "segment.bytes": str(self.segment_size),
        }
        rpk.create_topic(
            topic=self.topic_name,
            partitions=partitions,
            replicas=3,
            config=config,
        )

    def _produce(
        self,
        bytes_to_produce: int,
        key_set_cardinality: int | None = None,
    ):
        msg_count = bytes_to_produce // self.msg_size
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size=self.msg_size,
            msg_count=msg_count,
            key_set_cardinality=key_set_cardinality,
            timeout_sec=180,
        )

    def _local_partition_bytes(self, topic: str | None = None) -> int:
        """
        Sum the on-disk footprint of all replicas of the topic across all
        nodes, using the partition_size Prometheus metric exposed by each
        broker. This is what the space manager / local-retention path is
        actually shrinking.
        """
        if topic is None:
            topic = self.topic_name
        total = 0
        assert self.redpanda is not None
        samples = self.redpanda.metrics_sample("partition_size")
        if samples is None:
            return 0
        for s in samples.samples:
            if s.labels.get("topic") == topic:
                total += int(s.value)
        return total

    def _wait_local_below(self, ceiling_bytes: int, timeout_sec: int = 120):
        last = [-1]

        def cond() -> bool:
            size = self._local_partition_bytes()
            last[0] = size
            self.logger.info(
                f"local partition_size sum = {size}, ceiling = {ceiling_bytes}"
            )
            return size <= ceiling_bytes

        wait_until(
            cond,
            timeout_sec=timeout_sec,
            backoff_sec=3,
            err_msg=lambda: (
                f"local footprint did not drop below {ceiling_bytes} "
                f"(last observed: {last[0]})"
            ),
        )

    def _wait_local_at_least(self, floor_bytes: int, timeout_sec: int = 60):
        last = [-1]

        def cond() -> bool:
            size = self._local_partition_bytes()
            last[0] = size
            self.logger.info(
                f"local partition_size sum = {size}, floor = {floor_bytes}"
            )
            return size >= floor_bytes

        wait_until(
            cond,
            timeout_sec=timeout_sec,
            backoff_sec=3,
            err_msg=lambda: (
                f"local footprint did not reach {floor_bytes} "
                f"(last observed: {last[0]})"
            ),
        )

    def _wait_for_partition_info(self, timeout_sec: int = 60):
        """Wait until rpk can describe the partition (i.e. it is replicated)."""
        rpk = RpkTool(self.redpanda)

        def has_partition() -> bool:
            try:
                parts = list(rpk.describe_topic(self.topic_name))
                return len(parts) > 0
            except Exception:
                return False

        wait_until(
            has_partition,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg=f"topic {self.topic_name} did not become describable",
        )

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    @cluster(num_nodes=4)
    def test_local_data_grows_in_tiered_cloud(self):
        """
        In tiered_cloud + cleanup.policy=delete with strict retention engaged,
        compute_local_retention_offset caps local data at
        retention.local.target.bytes via the standard
        maybe_apply_local_storage_overrides path. ctp_stm housekeeping reads
        that offset and prefix-truncates accordingly. The local footprint
        should converge near the local target rather than near LRO.

        L1 compaction does not run for cleanup.policy=delete, so MASH stays
        unset and the floor contribution is min(); the cap comes entirely
        from the storage-layer query.

        Replication factor is 3, so the cluster-wide sum of partition_size is
        roughly 3 * per-replica retention.
        """
        self._create_topic(storage_mode=TopicSpec.STORAGE_MODE_TIERED_CLOUD)
        self._wait_for_partition_info()

        self._produce(self.bytes_to_produce)
        self.wait_until_reconciled(topic=self.topic_name, partition=0)

        # We expect each replica to converge near retention.local.target.bytes.
        # The floor (half the target) proves we are not collapsing to LRO; the
        # ceiling proves local retention actually trims toward the target. Allow
        # two segments of headroom per replica: one for the never-evicted active
        # segment, one for trim-cycle latency / replica lag. Still ~5x below the
        # produced footprint, so a regression where retention/the floor is
        # ignored fails.
        replication = 3
        per_replica_floor = self.local_target_bytes
        per_replica_ceiling = self.local_target_bytes + 2 * self.segment_size

        self._wait_local_at_least(
            floor_bytes=replication * per_replica_floor // 2,
            timeout_sec=120,
        )
        self._wait_local_below(
            ceiling_bytes=replication * per_replica_ceiling,
            timeout_sec=180,
        )

        final = self._local_partition_bytes()
        self.logger.info(f"final local footprint: {final}")
        # And critically: it must not have collapsed to ~segment_size, which
        # would mean LRO-only retention.
        assert final > replication * self.segment_size, (
            f"local footprint collapsed to LRO-only ({final} bytes); "
            f"expected near {replication * self.local_target_bytes} bytes"
        )

    @cluster(num_nodes=4)
    def test_flip_back_to_cloud_evicts_aggressively(self):
        """
        Starting in tiered_cloud with a healthy local footprint, flipping the
        topic's storage.mode back to cloud takes the cloud branch in
        ctp_stm::prefix_truncate_target (retention_target = offset::max()),
        which makes target collapse to max_removable_local_log_offset. Local
        data shrinks to roughly one segment.
        """
        self._create_topic(storage_mode=TopicSpec.STORAGE_MODE_TIERED_CLOUD)
        self._wait_for_partition_info()

        self._produce(self.bytes_to_produce)
        self.wait_until_reconciled(topic=self.topic_name, partition=0)

        # Confirm we accumulated meaningful local data (not just LRO).
        replication = 3
        self._wait_local_at_least(
            floor_bytes=replication * self.local_target_bytes // 2,
            timeout_sec=120,
        )

        # Flip back to cloud.
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(
            self.topic_name,
            TopicSpec.PROPERTY_STORAGE_MODE,
            TopicSpec.STORAGE_MODE_CLOUD,
        )

        # Local should collapse to roughly one segment per replica (LRO-only).
        # Use a generous ceiling of 3 segments per replica to absorb pacing.
        self._wait_local_below(
            ceiling_bytes=replication * 3 * self.segment_size,
            timeout_sec=180,
        )

    @cluster(num_nodes=4)
    def test_space_manager_reclaim_under_pressure(self):
        """
        Under cluster-wide disk pressure, resource_mgmt/storage.cc sets
        _cloud_gc_offset on each candidate log. ctp_stm's
        compute_local_retention_offset returns _cloud_gc_offset when it is
        engaged and cloud retention is active, so the retention floor
        advances past the local target. ctp_stm housekeeping then
        prefix-truncates aggressively, shrinking local data to roughly
        one segment per replica.

        max_removable_local_log_offset remains the active-reader/LRLO
        ceiling and is unaffected by MASH or by the local target, so
        nothing prevents the truncation from happening.
        """
        self._create_topic(storage_mode=TopicSpec.STORAGE_MODE_TIERED_CLOUD)
        self._wait_for_partition_info()

        self._produce(self.bytes_to_produce)
        self.wait_until_reconciled(topic=self.topic_name, partition=0)

        # First, confirm the hint took effect and we are holding meaningful
        # local data above one segment.
        replication = 3
        self._wait_local_at_least(
            floor_bytes=replication * self.local_target_bytes // 2,
            timeout_sec=120,
        )

        # Now apply tight cluster-wide disk pressure. retention_local_strict
        # makes the space manager treat retention_local_target_capacity_bytes
        # as a hard ceiling and reclaim aggressively from any partition,
        # regardless of per-topic retention.local.target.bytes or the
        # reconciler-published hint.
        tight_capacity = 2 * self.segment_size
        assert self.redpanda is not None
        self.redpanda.set_cluster_config(
            {
                "retention_local_strict": True,
                "retention_local_target_capacity_bytes": tight_capacity,
            }
        )

        # Local footprint should shrink well below the per-topic local target,
        # proving the space manager reclaimed past it. Each replica retains
        # the active segment plus a small tail buffer the LRO/active-reader
        # ceiling can't trim, so be generous: ~5 segments per replica.
        self._wait_local_below(
            ceiling_bytes=replication * 5 * self.segment_size,
            timeout_sec=180,
        )

    @cluster(num_nodes=4)
    def test_compact_topic_evicts_via_l1_floor(self):
        """
        On a tiered_cloud + cleanup.policy=compact topic the local log is a
        cache: ctp_stm housekeeping truncates aggressively (the same way it
        does for storage.mode=cloud) because the authoritative compacted view
        lives in L1. So once L1 compaction has run against the partition's L1
        objects, local segments are evicted regardless of the storage-layer
        local-retention target.

        We assert observable outcomes:
        1. L1 compaction actually runs and removes records (requires keyed
           data with duplicates, which we drive with low key_set_cardinality).
        2. Local footprint shrinks below the strict-retention cap, showing
           that compacted topics evict aggressively rather than holding data
           up to the local-retention target.
        """
        # Low key cardinality so compaction has plenty of duplicates to
        # dedupe. With ~2k messages and 64 keys, every key is updated many
        # times, and L1 compaction can reduce the log meaningfully.
        key_set_cardinality = 64
        self._create_topic(
            storage_mode=TopicSpec.STORAGE_MODE_TIERED_CLOUD,
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
        )
        self._wait_for_partition_info()

        self._produce(
            self.bytes_to_produce,
            key_set_cardinality=key_set_cardinality,
        )
        self.wait_until_reconciled(topic=self.topic_name, partition=0)

        replication = 3

        # Wait for L1 compaction to actually do work: at least one log
        # compaction round must complete with records removed. Metrics are
        # exported by the compaction scheduler / worker (see
        # cloud_topics/compaction_stress_test.py for the pattern).
        def compaction_records_removed() -> float:
            assert self.redpanda is not None
            return self.redpanda.metric_sum(
                metric_name=(
                    "vectorized_cloud_topics_compaction_worker_records_removed"
                ),
                expect_metric=True,
            )

        def compaction_rounds() -> float:
            assert self.redpanda is not None
            return self.redpanda.metric_sum(
                metric_name=(
                    "vectorized_cloud_topics_compaction_scheduler_log_compactions"
                ),
                expect_metric=True,
            )

        wait_until(
            lambda: compaction_rounds() >= 1,
            timeout_sec=240,
            backoff_sec=3,
            err_msg="L1 compaction did not run for the compacted topic",
        )
        wait_until(
            lambda: compaction_records_removed() > 0,
            timeout_sec=240,
            backoff_sec=3,
            err_msg="L1 compaction did not remove any records",
        )

        # ctp_stm housekeeping trims the local log aggressively for compacted
        # topics. Use a generous ceiling: 5 segments per replica absorbs the
        # active segment + tail buffer that the LRO/active-reader ceiling pins.
        self._wait_local_below(
            ceiling_bytes=replication * 5 * self.segment_size,
            timeout_sec=240,
        )
