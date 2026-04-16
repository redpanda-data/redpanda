# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.redpanda import MetricsEndpoint
from rptest.tests.cloud_topics.e2e_test import EndToEndCloudTopicsBase
from rptest.utils.mode_checks import is_debug_mode


class CompactionStressBase(EndToEndCloudTopicsBase):
    """
    Base class for cloud topics compaction stress tests.

    Provides metric helpers and a produce-compact-verify workflow for
    exercising pathological compaction workloads.
    """

    # Override — no default topics, each test creates its own
    topics = ()

    def __init__(
        self,
        test_context: TestContext,
        extra_rp_conf: dict[str, Any] | None = None,
    ):
        conf = {
            "cloud_topics_compaction_interval_ms": 5000,
        }
        if extra_rp_conf:
            conf.update(extra_rp_conf)

        environment = {
            "__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON",
        }
        super().__init__(
            test_context,
            extra_rp_conf=conf,
            environment=environment,
        )

    # ── Metric helpers ──────────────────────────────────────────────

    def _metric_sum(self, metric_name: str) -> float:
        assert self.redpanda
        return self.redpanda.metric_sum(
            metric_name=metric_name,
            metrics_endpoint=MetricsEndpoint.METRICS,
            expect_metric=True,
        )

    def get_log_compactions(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_scheduler_log_compactions"
        )

    def get_records_removed(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_worker_records_removed"
        )

    def get_managed_logs(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_scheduler_managed_log_count"
        )

    # ── Wait helpers ────────────────────────────────────────────────

    def wait_for_managed_logs(self, timeout_sec: int = 60):
        wait_until(
            lambda: self.get_managed_logs() > 0,
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg="Did not see management of compact-enabled CTPs.",
        )

    def wait_for_compaction_rounds(
        self,
        min_rounds: int,
        timeout_sec: int = 360,
    ):
        """Wait until at least `min_rounds` compaction rounds have completed."""
        wait_until(
            lambda: self.get_log_compactions() >= min_rounds,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg=lambda: (
                f"Expected >= {min_rounds} compaction rounds, "
                f"got {self.get_log_compactions()}"
            ),
        )

    def wait_for_records_removed(
        self,
        min_removed: int,
        timeout_sec: int = 360,
    ):
        """Wait until at least `min_removed` records have been removed."""
        wait_until(
            lambda: self.get_records_removed() >= min_removed,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg=lambda: (
                f"Expected >= {min_removed} records removed, "
                f"got {self.get_records_removed()}"
            ),
        )

    def wait_for_compaction_quiesce(
        self,
        stable_sec: int = 30,
        timeout_sec: int = 360,
    ):
        """
        Wait for records_removed to stabilize, meaning compaction has
        converged and there is nothing left to remove. The metric must
        remain unchanged for `stable_sec` consecutive seconds.
        """
        import time

        # First wait for at least one removal so we know compaction started.
        wait_until(
            lambda: self.get_records_removed() > 0,
            timeout_sec=60,
            backoff_sec=2,
            err_msg="Compaction never started removing records",
        )

        prev = self.get_records_removed()
        stable_since = time.time()

        def _stable() -> bool:
            nonlocal prev, stable_since
            now_removed = self.get_records_removed()
            if now_removed != prev:
                self.logger.info(
                    f"Compaction still active: records_removed {prev} -> {now_removed}"
                )
                prev = now_removed
                stable_since = time.time()
            return time.time() - stable_since >= stable_sec

        wait_until(
            _stable,
            timeout_sec=timeout_sec,
            backoff_sec=5,
            err_msg=lambda: (
                f"Compaction did not quiesce within {timeout_sec}s "
                f"(records_removed={self.get_records_removed()})"
            ),
        )
        self.logger.info(
            f"Compaction quiesced: {self.get_records_removed()} records removed"
        )

    def produce_and_wait(
        self,
        topic: str,
        msg_size: int,
        msg_count: int,
        key_set_cardinality: int,
        rate_limit_bps: int | None = None,
        tombstone_probability: float = 0.0,
        tolerate_failed_produce: bool = False,
    ) -> KgoVerifierProducer:
        """Produce messages and wait for all acks + latest value map."""
        assert self.redpanda
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=msg_size,
            msg_count=msg_count,
            key_set_cardinality=key_set_cardinality,
            rate_limit_bps=rate_limit_bps,
            tombstone_probability=tombstone_probability,
            validate_latest_values=True,
            tolerate_failed_produce=tolerate_failed_produce,
        )
        producer.start()
        try:
            producer.wait_for_latest_value_map()
            producer.wait()
        finally:
            producer.stop()
        return producer

    def consume_and_verify(
        self,
        topic: str,
        producer: KgoVerifierProducer,
    ):
        """
        Single-pass consume that validates every record's value matches
        the last value the producer wrote for that key. Callers must
        wait for full compaction (by metric) before calling this.
        """
        assert self.redpanda
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=0,
            loop=False,
            compacted=True,
            validate_latest_values=True,
            nodes=[producer.nodes[0]],
        )
        consumer.start(clean=False)
        try:
            consumer.wait(timeout_sec=120)
        finally:
            consumer.stop()


class CompactionStressKeyCardinalityTest(CompactionStressBase):
    """
    Force multi-pass compaction by producing more unique keys than fit
    in the key-offset map.

    With a 1MB map budget (capacity ~25K keys) and 200K unique keys
    (10K in debug), compaction needs multiple passes to fully deduplicate.

    In debug/sanitizer builds the data volume is reduced to stay within
    the framework's per-test write budget.
    """

    TOPIC_NAME = "key_cardinality_stress"
    KEY_MAP_MEMORY = 1024 * 1024  # 1 MB
    MSG_SIZE = 512

    if is_debug_mode():
        key_set_cardinality = 10_000
        msg_count = 50_000
    else:
        key_set_cardinality = 200_000
        msg_count = 1_000_000

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context,
            extra_rp_conf={
                "cloud_topics_compaction_key_map_memory": self.KEY_MAP_MEMORY,
            },
        )

    def setUp(self):
        assert self.redpanda
        self.redpanda.start()
        storage_mode = (self.test_context.injected_args or {}).get(
            "storage_mode", TopicSpec.STORAGE_MODE_CLOUD
        )
        if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )
        self.rpk.create_topic(
            topic=self.TOPIC_NAME,
            partitions=1,
            replicas=3,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: storage_mode,
                "cleanup.policy": TopicSpec.CLEANUP_COMPACT,
                "min.cleanable.dirty.ratio": "0.0",
            },
        )

    @cluster(num_nodes=4)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_key_cardinality_overflow(self, storage_mode: str):
        self.wait_for_managed_logs()

        producer = self.produce_and_wait(
            topic=self.TOPIC_NAME,
            msg_size=self.MSG_SIZE,
            msg_count=self.msg_count,
            key_set_cardinality=self.key_set_cardinality,
        )

        # With ~25K map capacity and 200K keys, compaction needs ~8
        # passes to cover all keys. Wait for records_removed to
        # stabilize — once it stops changing, compaction has converged.
        self.wait_for_compaction_quiesce()

        self.consume_and_verify(
            topic=self.TOPIC_NAME,
            producer=producer,
        )


class CompactionStressExtremeDedupTest(CompactionStressBase):
    """
    Produce massive record counts for very few keys, verifying
    correctness under extreme deduplication ratios (100,000:1).
    """

    TOPIC_NAME = "extreme_dedup_stress"
    KEY_SET_CARDINALITY = 10
    MSG_SIZE = 256

    if is_debug_mode():
        msg_count = 50_000
    else:
        msg_count = 1_000_000

    def __init__(self, test_context: TestContext):
        super().__init__(test_context)

    def setUp(self):
        assert self.redpanda
        self.redpanda.start()
        self.rpk.create_topic(
            topic=self.TOPIC_NAME,
            partitions=1,
            replicas=3,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
                "cleanup.policy": TopicSpec.CLEANUP_COMPACT,
                "min.cleanable.dirty.ratio": "0.0",
            },
        )

    @cluster(num_nodes=4)
    def test_extreme_dedup_ratio(self):
        self.wait_for_managed_logs()

        producer = self.produce_and_wait(
            topic=self.TOPIC_NAME,
            msg_size=self.MSG_SIZE,
            msg_count=self.msg_count,
            key_set_cardinality=self.KEY_SET_CARDINALITY,
        )

        # With only 10 keys, compaction should converge quickly.
        # Wait for records_removed to stabilize before consuming.
        self.wait_for_compaction_quiesce()

        self.consume_and_verify(
            topic=self.TOPIC_NAME,
            producer=producer,
        )


class CompactionStressWritePressureTest(CompactionStressBase):
    """
    Continuous high-throughput writes during compaction with a short
    min_compaction_lag_ms. Verifies that compaction removes a meaningful
    number of duplicates while the producer is still running.

    The other stress tests cover post-write convergence and correctness.
    This test targets a different question: does compaction make forward
    progress under sustained write pressure?
    """

    TOPIC_NAME = "write_pressure_stress"
    MSG_SIZE = 512
    RATE_LIMIT_BPS = 50 * 1024 * 1024  # 50 MB/s
    MIN_COMPACTION_LAG_MS = 5_000  # 5 seconds

    if is_debug_mode():
        key_set_cardinality = 5_000
        min_produced = 50_000
    else:
        key_set_cardinality = 100_000
        min_produced = 750_000

    def __init__(self, test_context: TestContext):
        super().__init__(test_context)

    def setUp(self):
        assert self.redpanda
        self.redpanda.start()
        self.rpk.create_topic(
            topic=self.TOPIC_NAME,
            partitions=8,
            replicas=3,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
                "cleanup.policy": TopicSpec.CLEANUP_COMPACT,
                "min.cleanable.dirty.ratio": "0.0",
                "min.compaction.lag.ms": str(self.MIN_COMPACTION_LAG_MS),
            },
        )

    @cluster(num_nodes=4)
    def test_write_pressure_during_compaction(self):
        self.wait_for_managed_logs()

        assert self.redpanda
        # Use a large msg_count as a ceiling -- we stop by ack count
        msg_count = 10_000_000
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_NAME,
            msg_size=self.MSG_SIZE,
            msg_count=msg_count,
            key_set_cardinality=self.key_set_cardinality,
            rate_limit_bps=self.RATE_LIMIT_BPS,
            tolerate_failed_produce=True,
        )
        producer.start()
        try:
            # Wait for sustained production — enough records that compaction
            # has time to run concurrently with writes.
            self.logger.info(
                f"Write phase: waiting for {self.min_produced} acks "
                f"at {self.RATE_LIMIT_BPS} bytes/s"
            )
            wait_until(
                lambda: producer.produce_status.acked >= self.min_produced,
                timeout_sec=360,
                backoff_sec=5,
                err_msg=lambda: (
                    f"Producer only acked {producer.produce_status.acked}"
                    f"/{self.min_produced}"
                ),
            )

            produced = producer.produce_status.acked
            self.logger.info(
                f"Write phase complete: {produced} records produced, "
                f"{self.get_log_compactions()} compaction rounds, "
                f"{self.get_records_removed()} records removed"
            )

            # With ~7.5 overwrites per key on average, even partial compaction
            # should remove at least one duplicate per unique key. Wait for
            # this while the producer is still running to confirm compaction
            # works under write pressure.
            wait_until(
                lambda: self.get_records_removed() >= self.key_set_cardinality,
                timeout_sec=120,
                backoff_sec=5,
                err_msg=lambda: (
                    f"Expected at least {self.key_set_cardinality} records "
                    f"removed under write pressure, "
                    f"got {self.get_records_removed()} "
                    f"(produced={producer.produce_status.acked}, "
                    f"compactions={self.get_log_compactions()})"
                ),
            )
        finally:
            producer.stop()
