# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
End-to-end coverage for the tiered_cloud local-retention hint produced by the
reconciler (see docs/superpowers/specs/2026-05-15-tiered-cloud-local-retention-design.md).

When a partition is in storage.mode=tiered_cloud with cleanup.policy=delete, the
reconciler should publish an allowed_local_start_offset hint that lets local data
stay around up to retention.local.target.bytes, even when retention.bytes is much
larger than the local target. Flipping back to storage.mode=cloud, or enabling
compaction, should clear the hint and cause local data to collapse to LRO again.
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
    (via the reconciler hint), and that flipping back to cloud mode or
    enabling compaction restores the original LRO-collapsing behavior.
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
            # Fast reconciliation so the hint is published quickly.
            "cloud_topics_reconciliation_min_interval": 1000,  # 1s
            "cloud_topics_reconciliation_max_interval": 2000,  # 2s
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
            # The reconciler only publishes an allowed_local_start_offset
            # hint when local retention is treated as a hard cap (mirrors
            # disk_log_impl::maybe_apply_local_storage_overrides). Tests
            # in this file rely on the hint to keep local data around.
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

    def _produce(self, bytes_to_produce: int):
        msg_count = bytes_to_produce // self.msg_size
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size=self.msg_size,
            msg_count=msg_count,
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
        In tiered_cloud mode with retention.bytes >> retention.local.target.bytes,
        the local footprint should converge near retention.local.target.bytes,
        not collapse to LRO (segment_size or smaller).

        Replication factor is 3, so the cluster-wide sum of partition_size is
        roughly 3 * per-replica retention.
        """
        self._create_topic(storage_mode=TopicSpec.STORAGE_MODE_TIERED_CLOUD)
        self._wait_for_partition_info()

        self._produce(self.bytes_to_produce)
        self.wait_until_reconciled(topic=self.topic_name, partition=0)

        # We expect each replica to hold somewhere between roughly the local
        # target and a few segments above it (segment boundaries + pacing).
        # With 3 replicas, the cluster sum should land in [3*target, 9*target]
        # ish. Use generous bounds: at least target (proves we are not
        # collapsing to LRO), at most 12*target (proves retention.bytes is
        # not the effective limit).
        replication = 3
        per_replica_floor = self.local_target_bytes
        per_replica_ceiling = 6 * self.local_target_bytes

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
        topic's storage.mode back to cloud should clear the hint, so prefix
        truncation targets LRO again and the local footprint shrinks to
        roughly one segment.
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
    def test_compact_topic_clears_hint(self):
        """
        Enabling compaction on a tiered_cloud topic should make the reconciler
        stop publishing the local-retention hint (the trigger predicate only
        fires for cleanup.policy=delete). Existing hints should be cleared,
        and local data should evict aggressively again.
        """
        self._create_topic(
            storage_mode=TopicSpec.STORAGE_MODE_TIERED_CLOUD,
            cleanup_policy=TopicSpec.CLEANUP_DELETE,
        )
        self._wait_for_partition_info()

        self._produce(self.bytes_to_produce)
        self.wait_until_reconciled(topic=self.topic_name, partition=0)

        replication = 3
        self._wait_local_at_least(
            floor_bytes=replication * self.local_target_bytes // 2,
            timeout_sec=120,
        )

        # Switch the topic to compacted. The reconciler should drop the
        # allowed_local_start_offset hint, returning prefix truncation to
        # the LRO target.
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(
            self.topic_name,
            "cleanup.policy",
            TopicSpec.CLEANUP_COMPACT,
        )

        self._wait_local_below(
            ceiling_bytes=replication * 3 * self.segment_size,
            timeout_sec=180,
        )
