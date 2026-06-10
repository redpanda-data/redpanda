# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from ducktape.tests.test import TestContext
from typing import Any
from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from collections.abc import Callable, Iterable

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.admin.v2 import Admin, metastore_pb, ntp_pb
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.redpanda import (
    SISettings,
    make_redpanda_service,
    MetricsEndpoint,
)
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import Scale
import rptest.tests.cloud_topics.utils as ct_utils


class EndToEndCloudTopicsBase(EndToEndTest):
    s3_topic_name = "panda_topic"

    num_brokers = 3

    topics = (
        TopicSpec(
            name=s3_topic_name,
            partition_count=5,
            replication_factor=3,
        ),
    )

    rpk: RpkTool

    def __init__(
        self,
        test_context: TestContext,
        extra_rp_conf: dict[str, Any] | None = None,
        environment: dict[str, str] | None = None,
    ):
        super(EndToEndCloudTopicsBase, self).__init__(test_context=test_context)

        self.test_context = test_context
        self.topic = self.s3_topic_name

        conf = {
            "enable_cluster_metadata_upload_loop": False,
        }

        if extra_rp_conf:
            for k, v in conf.items():
                extra_rp_conf[k] = v
        else:
            extra_rp_conf = conf

        self.si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )
        self.s3_bucket_name = self.si_settings.cloud_storage_bucket
        self.si_settings.load_context(self.logger, test_context)
        self.scale = Scale(test_context)

        self.redpanda = make_redpanda_service(
            context=self.test_context,
            num_brokers=self.num_brokers,
            si_settings=self.si_settings,
            extra_rp_conf=extra_rp_conf,
            environment=environment,
        )
        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    def setUp(self):
        assert self.redpanda
        self.redpanda.start()
        # Allow tests to select storage mode via @matrix(storage_mode=...).
        # Default to cloud if not specified.
        storage_mode = (self.test_context.injected_args or {}).get(
            "storage_mode", TopicSpec.STORAGE_MODE_CLOUD
        )
        if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )
        for topic in self.topics:
            config = {
                TopicSpec.PROPERTY_STORAGE_MODE: storage_mode,
                "cleanup.policy": topic.cleanup_policy,
            }
            if topic.min_cleanable_dirty_ratio is not None:
                config["min.cleanable.dirty.ratio"] = topic.min_cleanable_dirty_ratio
            if topic.delete_retention_ms is not None:
                config["delete.retention.ms"] = topic.delete_retention_ms
            self.rpk.create_topic(
                topic=topic.name,
                partitions=topic.partition_count,
                replicas=topic.replication_factor,
                config=config,
            )

    def wait_until_reconciled(self, topic: str, partition: int, timeout_sec: int = 60):
        def get_offsets():
            last_record: int | None = None
            output = self.rpk.consume(
                topic,
                partition=partition,
                offset=":end",
                format="%o\n",
                read_committed=True,
            )
            for line in output.splitlines():
                last_record = int(line)
            metastore = self.admin.metastore()
            req = metastore_pb.GetOffsetsRequest(
                partition=ntp_pb.TopicPartition(topic=topic, partition=partition)
            )
            return metastore.get_offsets(req=req).offsets.next_offset, last_record

        def is_reconciled() -> bool:
            next_offset, last_record = get_offsets()
            # Check the last observable record's offset against the next offset expected.
            # For transactions, this could be much less than the HWM if there are aborts.
            return (next_offset - 1) == last_record

        def message() -> str:
            try:
                next_offset, last_record = get_offsets()
                return f"failed to reconcile all data: topic={topic}, partition={partition}, last_record={last_record}, next_offset={next_offset}"
            except Exception:
                return f"failed to reconcile all data: topic={topic}, partition={partition}, unable to fetch offsets"

        wait_until(
            condition=is_reconciled,
            timeout_sec=timeout_sec,
            backoff_sec=5,
            err_msg=message,
            retry_on_exc=True,
        )

    def wait_until_all_reconciled(self, topics: Iterable[TopicSpec] | None = None):
        for topic in topics or self.topics:
            for partition in range(topic.partition_count):
                self.wait_until_reconciled(topic=topic.name, partition=partition)

    # ── L1 maintenance metric helpers ───────────────────────────────

    def _metric_sum(self, metric_name: str) -> float:
        assert self.redpanda
        return self.redpanda.metric_sum(
            metric_name=metric_name,
            metrics_endpoint=MetricsEndpoint.METRICS,
            expect_metric=True,
        )

    def get_managed_logs(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_scheduler_managed_log_count"
        )

    def get_log_compactions(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_scheduler_log_compactions_total"
        )

    def get_records_removed(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_worker_records_removed_total"
        )

    def get_leveling_completed(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_scheduler_leveling_ranges_completed_total"
        )

    def get_leveling_queue_length(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_scheduler_leveling_queue_length"
        )

    def get_extents_reclaimed(self) -> float:
        """Net object/extent-count reduction from committed leveling ranges."""
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_worker_leveling_extents_reclaimed_total"
        )

    # ── L1 maintenance wait helpers ─────────────────────────────────

    def wait_for_managed_logs(self, timeout_sec: int = 60):
        wait_until(
            lambda: self.get_managed_logs() > 0,
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg="Did not see management of cloud-topic partitions.",
        )

    def _wait_for_maintenance_quiesce(
        self,
        kind: str,
        get_progress: Callable[[], float],
        get_queue_length: Callable[[], float] | None,
        stable_sec: int,
        timeout_sec: int,
    ):
        """Wait for one kind of L1 maintenance to start and then converge.

        The progress counter must stop advancing for `stable_sec` consecutive
        seconds AND (when a queue getter is given) the scheduler queue must be
        drained. Polling `queue_length == 0` alone is unreliable: the
        collector refills the queue every interval, and work that has been
        dequeued but not yet committed is not counted there, so the queue can
        read 0 mid-flight.
        """
        # First wait for the maintenance kind to actually start doing work.
        wait_until(
            lambda: get_progress() > 0,
            timeout_sec=60,
            backoff_sec=2,
            err_msg=f"{kind} never made any progress",
        )

        def status() -> str:
            s = f"progress={get_progress()}"
            if get_queue_length is not None:
                s += f", queue_length={get_queue_length()}"
            return s

        prev = get_progress()
        stable_since = time.time()

        def _quiesced() -> bool:
            nonlocal prev, stable_since
            progress = get_progress()
            if progress != prev:
                self.logger.info(f"{kind} still active: {prev} -> {progress}")
                prev = progress
                stable_since = time.time()
                return False
            # Progress is stable; also require the queue to be empty.
            if get_queue_length is not None and get_queue_length() != 0:
                return False
            return time.time() - stable_since >= stable_sec

        wait_until(
            _quiesced,
            timeout_sec=timeout_sec,
            backoff_sec=5,
            err_msg=lambda: (
                f"{kind} did not quiesce within {timeout_sec}s ({status()})"
            ),
        )
        self.logger.info(f"{kind} quiesced ({status()})")

    def wait_for_compaction_quiesce(
        self,
        stable_sec: int = 30,
        timeout_sec: int = 360,
    ):
        """
        Wait for records_removed to stabilize, meaning compaction has
        converged and there is nothing left to remove.
        """
        self._wait_for_maintenance_quiesce(
            "Compaction",
            self.get_records_removed,
            None,
            stable_sec,
            timeout_sec,
        )

    def wait_for_leveling_quiesce(
        self,
        stable_sec: int = 30,
        timeout_sec: int = 360,
    ):
        """
        Wait for leveling to converge: the reclaimed-extents counter must stop
        changing for `stable_sec` consecutive seconds AND the leveling queue
        must be drained. Together these mean leveling has folded everything it
        is going to and there is no pending work.
        """
        self._wait_for_maintenance_quiesce(
            "Leveling",
            self.get_extents_reclaimed,
            self.get_leveling_queue_length,
            stable_sec,
            timeout_sec,
        )

    def assert_extents_well_sized(
        self,
        topic: str,
        max_target_size: int,
        min_extent_ratio: float,
        min_healthy_byte_fraction: float = 0.5,
        max_size_tolerance: float = 2.0,
    ):
        """Assert that, after leveling, `topic`'s L1 extents are reasonably
        sized per the leveling configuration:
        * the bulk of the data lives in well-sized ("healthy") extents, i.e.
          extents at least `min_extent_ratio * max_target_size` bytes, which is
          the threshold below which an extent is undersized and leveling
          eligible. Most bytes should end up in healthy extents.
        * no extent is grossly larger than the `max_target_size`
          (which is a soft cap)
        """
        lengths = ct_utils.get_l1_extent_lengths(self.admin, topic=topic)
        assert lengths, "expected at least one L1 extent to inspect"

        total_bytes = sum(lengths)
        min_healthy = int(min_extent_ratio * max_target_size)
        max_allowed = int(max_target_size * max_size_tolerance)
        healthy_bytes = sum(length for length in lengths if length >= min_healthy)
        oversized = [length for length in lengths if length > max_allowed]
        healthy_fraction = healthy_bytes / total_bytes

        self.logger.info(
            f"L1 extent sizes after leveling: count={len(lengths)}, "
            f"total={total_bytes}, max={max(lengths)}, min={min(lengths)}, "
            f"healthy(>= {min_healthy})_byte_fraction={healthy_fraction}"
        )

        assert not oversized, (
            f"found {len(oversized)} extents larger than {max_allowed}B "
            f"({max_size_tolerance}x the {max_target_size}B soft cap)"
        )
        assert healthy_fraction >= min_healthy_byte_fraction, (
            f"only {healthy_fraction} of L1 bytes are in well-sized extents "
            f"(>= {min_healthy}B), expected >= {min_healthy_byte_fraction}; "
            f"leveling may not have consolidated the undersized extents"
        )


class EndToEndCloudTopicsTest(EndToEndCloudTopicsBase):
    def __init__(self, test_context, extra_rp_conf=None, env=None):
        super(EndToEndCloudTopicsTest, self).__init__(test_context, extra_rp_conf, env)

    def await_num_produced(self, min_records, timeout_sec=120):
        wait_until(
            lambda: self.producer.num_acked > min_records,
            timeout_sec=timeout_sec,
            err_msg="Producer failed to produce messages for %ds." % timeout_sec,
        )

    @cluster(num_nodes=5)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_write(self, storage_mode: str):
        self.start_producer()

        self.await_num_produced(min_records=50000)

        self.start_consumer()
        self.run_validation()

        self.wait_until_all_reconciled()

    @cluster(num_nodes=5)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_delete_records(self, storage_mode: str):
        self.start_producer()
        self.await_num_produced(min_records=50000)
        self.producer.stop()
        for part in self.rpk.describe_topic(self.s3_topic_name):
            self.logger.info(
                f"lwm={part.start_offset},hwm={part.high_watermark},lso={part.last_stable_offset}"
            )
        output = self.rpk.trim_prefix(self.s3_topic_name, 35)
        self.logger.info(f"{output}")
        for part in self.rpk.describe_topic(self.s3_topic_name):
            assert part.start_offset == 35, (
                f"expected the start offset to be 35 after, got: {part}"
            )
            self.logger.info(
                f"lwm={part.start_offset},hwm={part.high_watermark},lso={part.last_stable_offset}"
            )
        self.start_consumer()
        self.run_consumer_validation(
            expected_missing_records=35 * self.topics[0].partition_count
        )
        self.wait_until_all_reconciled()

    @cluster(num_nodes=4)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_get_size(self, storage_mode: str):
        """
        Test that the metastore GetSize RPC returns the correct partition size.

        1. Before any data is written, GetSize should return either 0 or NOT_FOUND
           (partitions are lazily created in the metastore).
        2. After writing data, GetSize should eventually return a positive value.
        """
        topic = self.s3_topic_name
        partition = 0

        def get_partition_size() -> int | None:
            return ct_utils.get_l1_partition_size(self.admin, topic, partition)

        # Before writing data, the partition should either not exist or have size 0
        initial_size = get_partition_size()
        assert initial_size is None or initial_size == 0, (
            f"Expected partition size to be 0 or not found before writing data, "
            f"got {initial_size}"
        )
        self.logger.info(
            f"Initial partition size: {initial_size} (None means not found)"
        )

        # Write data to the topic
        self.start_producer()
        self.await_num_produced(min_records=50000)
        self.producer.stop()

        # Wait for the data to be reconciled to the metastore
        self.wait_until_reconciled(topic=topic, partition=partition)

        # Waits until the the partition size reaches a reported positive size
        ct_utils.wait_until_l1_partition_size(
            self.admin, topic, partition, lambda size: size > 0
        )


class EndToEndCloudTopicsStorageModeToggleTest(EndToEndCloudTopicsBase):
    """Exercise toggling a topic between 'cloud' and 'tiered_cloud' storage
    modes while a rate-limited producer is running, then validate the
    resulting log with a sequential consumer."""

    topics = (
        TopicSpec(
            name=EndToEndCloudTopicsBase.s3_topic_name,
            partition_count=5,
            replication_factor=3,
        ),
    )

    def __init__(self, test_context, extra_rp_conf=None, env=None):
        super(EndToEndCloudTopicsStorageModeToggleTest, self).__init__(
            test_context, extra_rp_conf, env
        )
        self.msg_size = 16 * 1024
        # Size the workload so the producer is still sending when the
        # toggle window ends: at ~10 MB/s with 16 KiB messages this is
        # ~400s of traffic, vs. a 5-minute toggle window.
        self.msg_count = 250_000
        self.rate_limit_bps = 10 * 1024 * 1024  # 10 MB/s
        self.toggle_duration_sec = 5 * 60
        self.toggle_interval_sec = 5

    @cluster(num_nodes=5)
    def test_toggle_storage_mode(self):
        assert self.redpanda is not None
        assert self.topic is not None
        # Enable tiered cloud topics so we can flip into that mode. The
        # topic is created in 'cloud' mode by the base setUp.
        self.redpanda.set_feature_active("tiered_cloud_topics", True, timeout_sec=30)

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic,
            msg_size=self.msg_size,
            msg_count=self.msg_count,
            rate_limit_bps=self.rate_limit_bps,
            tolerate_failed_produce=True,
        )
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic,
            self.msg_size,
            loop=False,
            producer=producer,
        )
        try:
            producer.start()

            start = time.time()
            mode = TopicSpec.STORAGE_MODE_CLOUD
            while time.time() - start < self.toggle_duration_sec:
                time.sleep(self.toggle_interval_sec)
                mode = (
                    TopicSpec.STORAGE_MODE_TIERED_CLOUD
                    if mode == TopicSpec.STORAGE_MODE_CLOUD
                    else TopicSpec.STORAGE_MODE_CLOUD
                )
                self.rpk.alter_topic_config(
                    self.topic, TopicSpec.PROPERTY_STORAGE_MODE, mode
                )
                self.logger.info(
                    f"switched storage mode of {self.topic} to {mode} "
                    f"(acked={producer.produce_status.acked})"
                )

            # Let the producer run to completion so the consumer has a
            # natural stopping point.
            producer.wait(timeout_sec=10 * 60)
            self.logger.info(
                f"producer finished with acked={producer.produce_status.acked}, "
                f"bad_offsets={producer.produce_status.bad_offsets}"
            )

            # Passing the producer to the consumer makes wait() block
            # until the consumer has read every produced offset and
            # validates reads internally.
            consumer.start(clean=False)
            consumer.wait(timeout_sec=10 * 60)

            self.wait_until_all_reconciled()
        finally:
            producer.stop()
            consumer.stop()
            producer.free()
            consumer.free()


class EndToEndCloudTopicsTxTest(EndToEndCloudTopicsBase):
    """Cloud topics end-to-end test with transactions used."""

    topics = (
        TopicSpec(
            name=EndToEndCloudTopicsBase.s3_topic_name,
            partition_count=1,
            replication_factor=3,
        ),
    )
    kgo_producer: KgoVerifierProducer
    kgo_consumer: KgoVerifierSeqConsumer

    def __init__(self, test_context, extra_rp_conf=None, env=None):
        super(EndToEndCloudTopicsTxTest, self).__init__(
            test_context, extra_rp_conf, env
        )
        self.msg_size = 4096
        # Use a smaller message count to prevent timeouts
        self.msg_count = 1000
        self.per_transaction = 10

    def start_producer_with_tx(self):
        assert self.redpanda and self.topic
        self.kgo_producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic,
            msg_size=self.msg_size,
            msg_count=self.msg_count,
            use_transactions=True,
            transaction_abort_rate=0.1,
            msgs_per_transaction=self.per_transaction,
            debug_logs=True,
            tolerate_failed_produce=True,
        )
        self.kgo_producer.start()
        self.kgo_producer.wait()

    def start_consumer_with_tx(self):
        traffic_node = self.kgo_producer.nodes[0]
        assert self.redpanda and self.topic
        self.kgo_consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic,
            self.msg_size,
            loop=False,
            nodes=[traffic_node],
            use_transactions=True,
            debug_logs=True,
            trace_logs=True,
        )
        self.kgo_consumer.start(clean=False)
        self.kgo_consumer.wait()

    @cluster(num_nodes=4)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_write(self, storage_mode: str):
        self.start_producer_with_tx()
        self.start_consumer_with_tx()
        # Validate by checking stats
        pstatus = self.kgo_producer.produce_status
        cstatus = self.kgo_consumer.consumer_status
        committed_messages = pstatus.acked - pstatus.aborted_transaction_messages
        assert pstatus.acked == self.msg_count
        assert 0 < committed_messages <= self.msg_count
        assert cstatus.validator.valid_reads == committed_messages
        assert cstatus.validator.invalid_reads == 0
        assert cstatus.validator.out_of_scope_invalid_reads == 0
        self.wait_until_all_reconciled(self.topics)


class EndToEndCloudTopicsCompactionTest(EndToEndCloudTopicsBase):
    """Cloud topics end-to-end test with a compacted topic."""

    topics = (
        TopicSpec(
            name=EndToEndCloudTopicsBase.s3_topic_name,
            partition_count=1,
            replication_factor=3,
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
            min_cleanable_dirty_ratio=0.0,
            delete_retention_ms=3000,
        ),
    )
    kgo_producer: KgoVerifierProducer
    kgo_consumer: KgoVerifierSeqConsumer

    def __init__(self, test_context):
        key_map_memory_kb = test_context.injected_args[
            "cloud_topics_compaction_key_map_memory_kb"
        ]
        extra_rp_conf = {
            "cloud_topics_compaction_interval_ms": 4000,
            "cloud_topics_compaction_key_map_memory": key_map_memory_kb * 1024,
        }
        environment = {"__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"}
        super(EndToEndCloudTopicsCompactionTest, self).__init__(
            test_context,
            extra_rp_conf,
            environment,
        )
        self.msg_size = 4096
        # Use a smaller message count to prevent timeouts
        self.msg_count = 1000
        self.key_set_cardinality = 100
        self.tombstone_probability = 0.5

    def produce(self):
        assert self.redpanda
        assert self.topic
        try:
            self.kgo_producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic,
                msg_size=self.msg_size,
                msg_count=self.msg_count,
                key_set_cardinality=self.key_set_cardinality,
                tombstone_probability=self.tombstone_probability,
                validate_latest_values=True,
                tolerate_failed_produce=True,
            )
            self.kgo_producer.start()
            self.kgo_producer.wait_for_latest_value_map()
            self.kgo_producer.wait()
        finally:
            self.kgo_producer.stop()

    def consume(self):
        assert self.redpanda
        assert self.topic
        traffic_node = self.kgo_producer.nodes[0]
        try:
            self.kgo_consumer = KgoVerifierSeqConsumer(
                self.test_context,
                self.redpanda,
                self.topic,
                self.msg_size,
                loop=False,
                compacted=True,
                validate_latest_values=True,
                nodes=[traffic_node],
            )
            self.kgo_consumer.start(clean=False)
            self.kgo_consumer.wait()
        finally:
            self.kgo_consumer.stop()

    @cluster(num_nodes=4)
    @matrix(cloud_topics_compaction_key_map_memory_kb=[3, 10, 128 * 1024])
    def test_compact(self, cloud_topics_compaction_key_map_memory_kb):
        self.wait_for_managed_logs()

        num_rounds = 1
        self.prev_log_compactions = 0.0
        self.prev_removed_records = 0.0
        for i in range(0, num_rounds):
            self.produce()

            def seen_compaction():
                log_compactions = self.get_log_compactions()
                res = log_compactions > self.prev_log_compactions
                self.prev_log_compactions = log_compactions
                return res

            wait_until(
                seen_compaction,
                timeout_sec=360,
                backoff_sec=1,
                err_msg="Did not see compaction of managed CTPs.",
            )

            def seen_removed_records():
                removed_records = self.get_records_removed()
                res = removed_records > self.prev_removed_records
                self.prev_removed_records = removed_records
                return res

            wait_until(
                seen_removed_records,
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Did not see removed records during compaction of CTPs.",
            )

            def consumed_latest_values():
                try:
                    self.consume()
                    return True
                except Exception:
                    return False

            wait_until(
                consumed_latest_values,
                timeout_sec=360,
                backoff_sec=1,
                err_msg="Did not see a fully compacted CTP log.",
            )


class EndToEndCloudTopicsLevelingTest(EndToEndCloudTopicsBase):
    """End-to-end test for per-range leveling.

    Produces enough data to create many small L1 objects, raises the slot
    pool so multiple ranges per shard can run in parallel, lowers the
    leveling interval so the test does not wait several minutes, then waits
    for leveling to converge: the completed-ranges counter must stop
    advancing AND the queue must be empty. Verifies data integrity by
    reading all produced records back.
    """

    topics = (
        TopicSpec(
            name=EndToEndCloudTopicsBase.s3_topic_name,
            partition_count=1,
            replication_factor=3,
        ),
    )

    kgo_producer: KgoVerifierProducer
    kgo_consumer: KgoVerifierSeqConsumer

    LEVELING_INTERVAL_MS = 2000
    MAX_CONCURRENT = 4
    MIN_EXTENT_RATIO = 0.8
    RECONCILIATION_MAX_OBJECT_SIZE = 4 * 1024 * 1024
    TARGET_FILL_RATIO = 0.2

    # Rate-limit produce so each ~250ms reconciliation flush stays well
    # under the threshold (~0.5 MiB), yielding a long run of undersized extents.
    PRODUCE_RATE_BPS = 2 * 1024 * 1024

    def __init__(self, test_context):
        extra_rp_conf = {
            "cloud_topics_leveling_interval_ms": self.LEVELING_INTERVAL_MS,
            "cloud_topics_max_concurrent_leveling_jobs_per_shard": self.MAX_CONCURRENT,
            "cloud_topics_leveling_min_extent_size_ratio": self.MIN_EXTENT_RATIO,
            "cloud_topics_reconciliation_max_object_size": self.RECONCILIATION_MAX_OBJECT_SIZE,
            "cloud_topics_reconciliation_target_fill_ratio": self.TARGET_FILL_RATIO,
        }
        super(EndToEndCloudTopicsLevelingTest, self).__init__(
            test_context,
            extra_rp_conf,
        )
        self.msg_size = 4096
        self.msg_count = 20000

    def produce(self):
        assert self.redpanda
        assert self.topic
        try:
            self.kgo_producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic,
                msg_size=self.msg_size,
                msg_count=self.msg_count,
                rate_limit_bps=self.PRODUCE_RATE_BPS,
            )
            self.kgo_producer.start()
            self.kgo_producer.wait()
        finally:
            self.kgo_producer.stop()

    def consume(self):
        assert self.redpanda
        assert self.topic
        traffic_node = self.kgo_producer.nodes[0]
        try:
            self.kgo_consumer = KgoVerifierSeqConsumer(
                self.test_context,
                self.redpanda,
                self.topic,
                self.msg_size,
                loop=False,
                nodes=[traffic_node],
            )
            self.kgo_consumer.start(clean=False)
            self.kgo_consumer.wait()
        finally:
            self.kgo_consumer.stop()

    @cluster(num_nodes=4)
    def test_per_range_leveling(self):
        self.produce()

        # Wait until at least one leveling range has been completed, so we know
        # leveling actually engaged before checking for convergence.
        wait_until(
            lambda: self.get_leveling_completed() > 0,
            timeout_sec=120,
            backoff_sec=1,
            err_msg="No leveling ranges were completed",
        )

        # Wait for leveling to fully converge before verifying data integrity.
        self.wait_for_leveling_quiesce()

        # Assert that extents are now well sized post leveling.
        assert self.topic
        self.assert_extents_well_sized(
            topic=self.topic,
            max_target_size=self.RECONCILIATION_MAX_OBJECT_SIZE,
            min_extent_ratio=self.MIN_EXTENT_RATIO,
        )

        # Read all records back to verify data integrity.
        self.consume()


class EndToEndCloudTopicsMaintenanceToggleTest(EndToEndCloudTopicsBase):
    """Rapidly flip the compaction and leveling configs
    (`cloud_topics_compaction_disabled` / `cloud_topics_leveling_disabled`)
    on and off while a rate-limited producer keeps a compacted topic busy.
    """

    topics = (
        TopicSpec(
            name=EndToEndCloudTopicsBase.s3_topic_name,
            partition_count=4,
            replication_factor=3,
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
            min_cleanable_dirty_ratio=0.0,
            delete_retention_ms=3000,
        ),
    )

    kgo_producer: KgoVerifierProducer
    kgo_consumer: KgoVerifierSeqConsumer

    COMPACTION_INTERVAL_MS = 2000
    LEVELING_INTERVAL_MS = 2000
    MAX_CONCURRENT = 4
    MIN_EXTENT_RATIO = 0.8
    RECONCILIATION_MAX_OBJECT_SIZE = 4 * 1024 * 1024
    TARGET_FILL_RATIO = 0.2

    def __init__(self, test_context):
        extra_rp_conf = {
            "cloud_topics_compaction_interval_ms": self.COMPACTION_INTERVAL_MS,
            "cloud_topics_compaction_key_map_memory": 128 * 1024 * 1024,
            "cloud_topics_leveling_interval_ms": self.LEVELING_INTERVAL_MS,
            "cloud_topics_max_concurrent_leveling_jobs_per_shard": self.MAX_CONCURRENT,
            "cloud_topics_leveling_min_extent_size_ratio": self.MIN_EXTENT_RATIO,
            "cloud_topics_reconciliation_max_object_size": self.RECONCILIATION_MAX_OBJECT_SIZE,
            "cloud_topics_reconciliation_target_fill_ratio": self.TARGET_FILL_RATIO,
        }
        environment = {"__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"}
        super(EndToEndCloudTopicsMaintenanceToggleTest, self).__init__(
            test_context,
            extra_rp_conf,
            environment,
        )
        self.msg_size = 4096
        # Size the workload (with the rate limit) so the producer is still
        # sending throughout the toggle window, while keeping the key count
        # low enough that the topic can fully compact afterwards.
        self.msg_count = 20_000
        self.key_set_cardinality = 100
        self.tombstone_probability = 0.5
        self.rate_limit_bps = 1024 * 1024  # 1 MB/s (~256 msg/s)
        self.toggle_duration_sec = 75
        self.toggle_interval_sec = 2

    def set_maintenance_configs(
        self, compaction_disabled: bool, leveling_disabled: bool
    ):
        assert self.redpanda
        self.redpanda.set_cluster_config(
            {
                "cloud_topics_compaction_disabled": compaction_disabled,
                "cloud_topics_leveling_disabled": leveling_disabled,
            }
        )

    def consume(self, traffic_node):
        assert self.redpanda
        assert self.topic
        self.kgo_consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic,
            self.msg_size,
            loop=False,
            compacted=True,
            validate_latest_values=True,
            nodes=[traffic_node],
        )
        try:
            self.kgo_consumer.start(clean=False)
            self.kgo_consumer.wait()
        finally:
            self.kgo_consumer.stop()

    @cluster(num_nodes=4)
    def test_toggle_maintenance(self):
        assert self.redpanda is not None
        assert self.topic is not None

        self.kgo_producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic,
            msg_size=self.msg_size,
            msg_count=self.msg_count,
            key_set_cardinality=self.key_set_cardinality,
            tombstone_probability=self.tombstone_probability,
            rate_limit_bps=self.rate_limit_bps,
            validate_latest_values=True,
            tolerate_failed_produce=True,
        )
        producer = self.kgo_producer
        try:
            producer.start()
            producer.wait_for_latest_value_map()

            # Flip the two kill switches on independent cadences (every tick
            # for compaction, every third tick for leveling) so every
            # combination of enabled/disabled is hit while work is inflight.
            start = time.time()
            i = 0
            while time.time() - start < self.toggle_duration_sec:
                time.sleep(self.toggle_interval_sec)
                compaction_disabled = (i % 2) == 0
                leveling_disabled = (i % 3) == 0
                self.set_maintenance_configs(compaction_disabled, leveling_disabled)
                self.logger.info(
                    f"toggled kill switches: compaction_disabled="
                    f"{compaction_disabled}, leveling_disabled={leveling_disabled} "
                    f"(acked={producer.produce_status.acked})"
                )
                i += 1

            # Re-enable both kinds so maintenance can drain, then let the
            # producer run to completion. Stop it before consuming so the
            # consumer can reuse the (single) traffic node.
            self.set_maintenance_configs(
                compaction_disabled=False, leveling_disabled=False
            )
            producer.wait(timeout_sec=10 * 60)
            self.logger.info(
                f"producer finished with acked={producer.produce_status.acked}, "
                f"bad_offsets={producer.produce_status.bad_offsets}"
            )
            traffic_node = producer.nodes[0]
        finally:
            producer.stop()

        # Both kinds had enabled phases during the toggle window, so the
        # cumulative counters must be nonzero: the toggling never wedged
        # maintenance outright.
        assert self.get_log_compactions() > 0, "no compaction rounds ran"
        assert self.get_leveling_completed() > 0, "no leveling ranges completed"

        # With both kinds re-enabled and the workload finished, maintenance
        # must converge: drain whatever eligible work remains and stop making
        # progress. The producer may finish well before the toggle window does
        # (tombstones halve the average message size under the byte rate
        # limit), so demanding progress beyond a post-re-enable baseline would
        # race with maintenance having already drained all eligible work
        # during the enabled phases of the window; quiescence is the property
        # the final re-enable actually guarantees.
        self.wait_for_compaction_quiesce()
        self.wait_for_leveling_quiesce()

        # Reading the whole compacted log back with latest-value validation
        # only succeeds once compaction has fully de-duplicated each key, so
        # retry until the post-chaos topic converges. This both proves data
        # integrity and that compaction recovers after the toggling.
        def consumed_latest_values():
            try:
                self.consume(traffic_node)
                return True
            except Exception:
                return False

        wait_until(
            consumed_latest_values,
            timeout_sec=360,
            backoff_sec=1,
            err_msg="Did not see a fully compacted CTP log after toggling",
        )
