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
    LoggingConfig,
    RESTART_LOG_ALLOW_LIST,
    SISettings,
    make_redpanda_service,
    MetricsEndpoint,
)
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import Scale, wait_until_result
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
        if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )
        for topic in self.topics:
            config = {
                **TopicSpec.storage_mode_config(storage_mode),
                "cleanup.policy": topic.cleanup_policy,
            }
            if topic.min_cleanable_dirty_ratio is not None:
                config["min.cleanable.dirty.ratio"] = topic.min_cleanable_dirty_ratio
            if topic.delete_retention_ms is not None:
                config["delete.retention.ms"] = topic.delete_retention_ms
            if topic.retention_bytes is not None:
                config["retention.bytes"] = topic.retention_bytes
            self.rpk.create_topic(
                topic=topic.name,
                partitions=topic.partition_count,
                replicas=topic.replication_factor,
                config=config,
            )

    def wait_until_reconciled(
        self,
        topic: str,
        partition: int,
        offset: int | None = None,
        timeout_sec: int = 60,
    ):
        """Wait for the partition's L1 reconciled frontier to catch up.

        By default this waits until every observable record has been reconciled
        into L1 (the metastore next offset reaches the log tail). When `offset`
        is given, instead wait until L1 has reconciled at least up to `offset`
        (the metastore next offset reaches `offset`), letting callers wait for a
        specific amount of fresh data to land in L1 without requiring the
        reconciler to fully catch up to the tail."""

        def get_next_offset() -> int:
            metastore = self.admin.metastore()
            req = metastore_pb.GetOffsetsRequest(
                partition=ntp_pb.TopicPartition(topic=topic, partition=partition)
            )
            return metastore.get_offsets(req=req).offsets.next_offset

        def get_last_record() -> int | None:
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
            return last_record

        def is_reconciled() -> bool:
            if offset is not None:
                # Wait until L1 has reconciled at least up to `offset`.
                return get_next_offset() >= offset
            # Check the last observable record's offset against the next offset
            # expected. For transactions, this could be much less than the HWM
            # if there are aborts.
            return (get_next_offset() - 1) == get_last_record()

        def message() -> str:
            try:
                next_offset = get_next_offset()
                if offset is not None:
                    return f"failed to reconcile all data: topic={topic}, partition={partition}, next_offset={next_offset}, target_next_offset={offset}"
                last_record = get_last_record()
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
        max_size_tolerance: float = 2.0,
    ):
        """Assert that, after leveling, `topic`'s L1 extents reflect the
        consolidation leveling is responsible for:
        * no extent is grossly larger than `max_target_size` (a soft cap).
        * no partition retains two *adjacent* extents that leveling could have
          folded into one, i.e. a consecutive pair (in `base_offset` order)
          that are both undersized (below `min_extent_ratio * max_target_size`,
          the leveling-eligibility threshold) yet whose combined size still
          fits under `max_target_size`. Such a pair is direct evidence leveling
          left work undone.

        Note we deliberately do *not* assert that every extent is well-sized:
        a workload that fragments faster than the target can fill (e.g. a
        trickle spread across many partitions) leaves isolated undersized
        extents that have no foldable neighbour — a lone small extent between
        two ~`max_target_size` extents cannot be merged without exceeding the
        cap. Those are an expected, irreducible outcome, not a leveling defect.

        The partition's tail run — the trailing consecutive undersized
        extents — is likewise exempt while its combined size is below the
        undersized threshold: leveling deliberately holds it back until its
        rewrite can produce a healthy extent, since folding it earlier would
        just emit another undersized extent that snowballs as new data lands
        behind it.
        """
        by_partition = ct_utils.get_l1_extent_lengths_by_partition(
            self.admin, topic=topic
        )
        assert by_partition, "expected at least one L1 extent to inspect"

        min_healthy = int(min_extent_ratio * max_target_size)
        max_allowed = int(max_target_size * max_size_tolerance)

        for partition, lengths in sorted(by_partition.items()):
            undersized = [length for length in lengths if length < min_healthy]
            self.logger.info(
                f"L1 extent sizes after leveling for partition {partition}: "
                f"count={len(lengths)}, total={sum(lengths)}, "
                f"undersized={len(undersized)}, lengths={lengths}"
            )

            oversized = [length for length in lengths if length > max_allowed]
            assert not oversized, (
                f"partition {partition}: found {len(oversized)} extents larger "
                f"than {max_allowed}B ({max_size_tolerance}x the "
                f"{max_target_size}B soft cap): {oversized}"
            )

            # Locate the tail run (trailing consecutive undersized extents).
            # If its combined size cannot yet fill a healthy extent, leveling
            # holds it back on purpose; exempt pairs within it.
            tail_start = len(lengths)
            while tail_start > 0 and lengths[tail_start - 1] < min_healthy:
                tail_start -= 1
            if sum(lengths[tail_start:]) >= min_healthy:
                # The tail run is big enough to level; no exemption.
                tail_start = len(lengths)

            for i in range(len(lengths) - 1):
                if i >= tail_start:
                    continue
                a, b = lengths[i], lengths[i + 1]
                foldable = (
                    a < min_healthy and b < min_healthy and a + b <= max_target_size
                )
                assert not foldable, (
                    f"partition {partition}: adjacent undersized extents at "
                    f"index {i} ({a}B) and {i + 1} ({b}B) sum to {a + b}B "
                    f"(<= the {max_target_size}B target) — leveling should have "
                    f"folded them into one well-sized extent"
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
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
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
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
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
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
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


L0_DATA_PREFIX = "level_zero/data/"

# When the read path tries to materialize a placeholder whose L0 object was
# garbage-collected there is no L1 fallback; the reader fails the fetch.
MODE_TOGGLE_LOG_ALLOW_LIST = RESTART_LOG_ALLOW_LIST + [
    "Failed to materialize batches from the cloud storage",
    # Downstream fallout of the failed materialization in the fetch path.
    "cloud_topics.*runtime_error",
    "kafka.*Failed to materialize",
]


class EndToEndCloudTopicsStorageModeToggleTest(EndToEndCloudTopicsBase):
    """Exercise toggling a topic between 'cloud' and 'tiered' storage modes
    (the latter resolves to tiered_v2 via the cluster default) while a
    rate-limited producer is running, then validate that the whole log stays
    readable once the L0 objects behind the already-reconciled region are
    garbage-collected.

    The final read exercises the mode-flip / L0-GC interaction: after a
    cloud -> tiered_v2 flip the tiered read branch serves everything at or
    above the local log's start offset from the local log, ignoring the
    last-reconciled offset. The local log still holds placeholders for the
    already-reconciled region, and L0 GC is allowed to delete the objects
    those placeholders reference (their epoch is inactive and they are past
    the minimum age). Reading that region then has to materialize
    placeholders against deleted objects, which fails with no L1 fallback,
    even though L1 holds a complete copy of the data. The flip also freezes
    local prefix-truncation (tiered_v2 honors full topic retention, which is
    effectively infinite here), so the log never self-heals by trimming the
    placeholder region.

    With the default 12h cloud_topics_short_term_gc_minimum_object_age the
    window is practically unreachable; the test shrinks it (plus the epoch
    rotation and housekeeping intervals that gate GC eligibility) to make
    the sequence take seconds.
    """

    topics = (
        TopicSpec(
            name=EndToEndCloudTopicsBase.s3_topic_name,
            partition_count=5,
            replication_factor=3,
        ),
    )

    def __init__(self, test_context, extra_rp_conf=None, env=None):
        extra_rp_conf = dict(extra_rp_conf or {})
        extra_rp_conf.update(
            {
                # L0 objects become GC-eligible after 30s instead of 12h.
                "cloud_topics_short_term_gc_minimum_object_age": 30_000,
                "cloud_topics_short_term_gc_interval": 5_000,
                "cloud_topics_short_term_gc_backoff_interval": 5_000,
                # GC eligibility requires the partition's inactive-epoch
                # estimate to advance past the epoch the data was written
                # under. That takes housekeeper epoch bumps, and each bump
                # needs the controller to have minted a fresh cluster epoch
                # (default mint interval: 10min) and the local cache to have
                # picked it up. Do not shrink max_same_epoch_duration below
                # the mint interval: epoch fetches fail outright while the
                # cached epoch is older than it.
                "cloud_storage_housekeeping_interval_ms": 5_000,
                "cloud_topics_epoch_service_epoch_increment_interval": 5_000,
                "cloud_topics_epoch_service_local_epoch_cache_duration": 5_000,
                # Reads must materialize from object storage rather than be
                # served by the write-through batch cache, which would mask
                # the deleted objects.
                "disable_batch_cache": True,
                # Many small L0 objects.
                "cloud_topics_produce_batching_size_threshold": 65536,
            }
        )
        super(EndToEndCloudTopicsStorageModeToggleTest, self).__init__(
            test_context, extra_rp_conf, env
        )
        self.msg_size = 16 * 1024
        # Size the workload so the producer is still sending when the
        # toggle window ends: at ~10 MB/s with 16 KiB messages this is
        # ~33s of traffic, vs. a 30-second toggle window.
        self.msg_count = 20_000
        self.rate_limit_bps = 10 * 1024 * 1024  # 10 MB/s
        self.toggle_duration_sec = 30
        self.toggle_interval_sec = 5
        # The GC gates (epoch eligibility, safety monitor, age) all log at
        # debug; without this the reason GC skips an object is invisible.
        assert self.redpanda
        self.redpanda._log_config = LoggingConfig("info", {"cloud_topics": "debug"})

    def _l0_object_count(self) -> int:
        assert self.redpanda
        return sum(
            1
            for _ in self.redpanda.cloud_storage_client.list_objects(
                self.s3_bucket_name, prefix=L0_DATA_PREFIX
            )
        )

    @cluster(num_nodes=4, log_allow_list=MODE_TOGGLE_LOG_ALLOW_LIST)
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
        # The toggle flips through the 'tiered' alias; point it at the
        # cloud-architecture variant (the default is tiered_v1, under which
        # cloud -> tiered is a forbidden transition).
        self.rpk.cluster_config_set(
            "default_redpanda_storage_mode_tiered_impl", "tiered_v2"
        )

        try:
            producer.start()

            start = time.time()
            mode = TopicSpec.STORAGE_MODE_CLOUD
            while time.time() - start < self.toggle_duration_sec:
                time.sleep(self.toggle_interval_sec)
                mode = (
                    TopicSpec.STORAGE_MODE_TIERED
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

            # Let the producer run to completion so the read-back has a
            # known record count.
            producer.wait(timeout_sec=10 * 60)
            acked = producer.produce_status.acked
            self.logger.info(
                f"producer finished with acked={acked}, "
                f"bad_offsets={producer.produce_status.bad_offsets}"
            )
        finally:
            producer.stop()
            producer.free()

        # End the toggling in tiered_v2 mode: the local log keeps the
        # placeholders for whatever was reconciled while in cloud mode.
        self.rpk.alter_topic_config(
            self.topic, TopicSpec.PROPERTY_STORAGE_MODE, TopicSpec.STORAGE_MODE_TIERED
        )
        self.wait_until_all_reconciled()

        l0_before = self._l0_object_count()
        self.logger.info(f"L0 objects after reconciliation: {l0_before}")
        assert l0_before > 0, "expected reconciled data to have L0 objects"

        # Trickle produce keeps the reconciler advancing the last-reconciled
        # offset past the housekeeper's epoch-bump command batches; the
        # inactive-epoch estimate (and with it GC eligibility) only moves
        # when the LRO does. The trickled records take the tiered (raft)
        # write path and land above the placeholder region under test.
        trickled = 0

        topic = self.topic

        def l0_gone() -> bool:
            nonlocal trickled
            count = self._l0_object_count()
            self.logger.info(f"L0 objects remaining: {count}")
            if count > 0:
                self.kafka_tools.produce(topic, 50, self.msg_size, acks=-1)
                trickled += 50
            return count == 0

        wait_until(
            l0_gone,
            timeout_sec=240,
            backoff_sec=10,
            err_msg="L0 GC did not delete the reconciled objects",
        )

        # The reconciler downloaded the L0 objects to build L1, leaving them
        # in the cloud-storage disk cache, which the fetch path consults
        # before object storage and which survives the objects' deletion.
        # Wipe it (and any in-memory state) so the read has to go to object
        # storage, as it would once the cache entries are evicted.
        for node in self.redpanda.nodes:
            self.redpanda.stop_node(node)
        for node in self.redpanda.nodes:
            node.account.ssh(f"rm -rf {self.redpanda.cache_dir}")
        for node in self.redpanda.nodes:
            self.redpanda.start_node(node)
        redpanda = self.redpanda
        wait_until(
            lambda: redpanda.healthy(),
            timeout_sec=60,
            backoff_sec=2,
            err_msg="cluster did not become healthy after cache wipe",
        )

        # Read the whole log back: every acked record (plus the trickled
        # ones) must be consumable; the reconciled region is expected to be
        # served from L1 once its L0 objects are gone.
        expected = acked + trickled
        out = self.rpk.consume(
            self.topic,
            offset="start",
            n=expected,
            format="%o\n",
            quiet=True,
            timeout=120,
        )
        consumed = len(out.splitlines())
        assert consumed == expected, (
            f"consumed {consumed}/{expected} records after storage-mode "
            f"toggling and L0 GC; the reconciled region is expected to be "
            f"readable from L1"
        )


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
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
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


class EndToEndCloudTopicsReconciliationToggleTest(EndToEndCloudTopicsBase):
    """Rapidly flip the reconciliation loop
    (`cloud_topics_disable_reconciliation_loop`) on and off while a
    rate-limited producer keeps a delete-policy topic busy under aggressive
    size-based retention.

    Retention (housekeeping) only acts on data that reconciliation has moved
    from L0 into the L1 metastore, so toggling reconciliation repeatedly
    stresses the interaction between the two: the reconciler adds extents in
    bursts while housekeeping trims them. The test verifies the toggling never
    wedges reconciliation or retention -- after both are allowed to drain, data
    still reconciles to L1 and retention advances the start offset -- and that
    the surviving tail of the log remains consumable.
    """

    topic_name = "cloud_topic_reconciliation_toggle_test"
    partition_count = 4

    RECONCILIATION_INTERVAL_MS = 1000
    HOUSEKEEPING_INTERVAL_MS = 5000
    # Per-partition size cap. The workload produces far more than this per
    # partition, so once reconciliation catches up retention must trim heavily
    # and advance the start offset well past 0.
    RETENTION_BYTES = 2 * 1024 * 1024

    topics = (
        TopicSpec(
            name=topic_name,
            partition_count=partition_count,
            replication_factor=3,
            cleanup_policy=TopicSpec.CLEANUP_DELETE,
            retention_bytes=RETENTION_BYTES,
        ),
    )

    kgo_producer: KgoVerifierProducer

    def __init__(self, test_context):
        extra_rp_conf = {
            # Fast reconciliation so each enabled window actually moves data
            # L0 -> L1.
            "cloud_topics_reconciliation_min_interval": self.RECONCILIATION_INTERVAL_MS,
            "cloud_topics_reconciliation_max_interval": self.RECONCILIATION_INTERVAL_MS,
            # Fast housekeeping so retention enforcement runs often.
            "cloud_storage_housekeeping_interval_ms": self.HOUSEKEEPING_INTERVAL_MS,
        }
        super(EndToEndCloudTopicsReconciliationToggleTest, self).__init__(
            test_context,
            extra_rp_conf,
        )
        self.msg_size = 4096
        # Rate-limit the produce so it spans the whole toggle window: at
        # 1 MiB/s this is ~160s of traffic, vs. a 120s toggle window.
        self.msg_count = 40_000
        self.rate_limit_bps = 1024 * 1024  # 1 MB/s
        # Each toggle iteration blocks until retention advances the start
        # offset, so the window is sized to fit several such gated flips.
        self.toggle_duration_sec = 120

    def set_reconciliation_disabled(self, disabled: bool):
        assert self.redpanda
        self.redpanda.set_cluster_config(
            {"cloud_topics_disable_reconciliation_loop": disabled}
        )

    def _start_offsets(self) -> dict[int, int]:
        """Current start offset of every partition, keyed by partition id."""
        return {
            part.id: part.start_offset
            for part in self.rpk.describe_topic(self.topic_name)
        }

    def _wait_for_retention_to_apply(
        self, min_offsets: dict[int, int], timeout_sec: int = 90
    ) -> dict[int, int]:
        """Wait until retention advances *every* partition's start offset
        strictly past its entry in `min_offsets`, then return the new start
        offsets. Threading the returned dict back in as the next `min_offsets`
        makes the wait itself the monotonic-advancement check across all
        partitions: it only returns once retention has moved every offset
        forward, gated on the slowest partition."""

        def retention_applied() -> tuple[bool, dict[int, int]]:
            current = self._start_offsets()
            applied = all(current.get(p, 0) > floor for p, floor in min_offsets.items())
            return applied, current

        return wait_until_result(
            retention_applied,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg=lambda: (
                f"retention did not advance every partition past "
                f"{min_offsets} (last={self._start_offsets()})"
            ),
        )

    @cluster(num_nodes=4)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
        ],
    )
    def test_toggle_reconciliation(self, storage_mode: str):
        assert self.redpanda is not None

        self.kgo_producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size=self.msg_size,
            msg_count=self.msg_count,
            rate_limit_bps=self.rate_limit_bps,
            tolerate_failed_produce=True,
        )
        producer = self.kgo_producer
        partition_offsets = {p: 0 for p in range(self.partition_count)}
        toggles = 0
        # Before each pause the reconciled frontier must move at least a full
        # retention window beyond each partition's current start offset, so
        # that once paused retention still has data above the cap to trim and
        # can advance the start offset. Records carry framing overhead, so
        # fewer than RETENTION_BYTES/msg_size records actually fit in the
        # window; requiring 2x the nominal count guarantees enough surplus.
        reconcile_ahead = 2 * (self.RETENTION_BYTES // self.msg_size)
        try:
            producer.start()

            start = time.time()
            while time.time() - start < self.toggle_duration_sec:
                # With the reconciler enabled, wait until a full retention
                # window of fresh data has landed in L1 beyond each partition's
                # current start offset, so that retention will be able to
                # advance every start offset once the reconciler is paused.
                self.set_reconciliation_disabled(False)
                for partition, start_offset in partition_offsets.items():
                    self.wait_until_reconciled(
                        topic=self.topic_name,
                        partition=partition,
                        offset=start_offset + reconcile_ahead,
                        timeout_sec=90,
                    )

                # Pause the reconciler and require retention to advance every
                # partition's start offset while it is paused: the housekeeper
                # must trim the freshly-reconciled backlog even though the
                # reconciler is stopped. Pausing reconciliation must not wedge
                # retention.
                self.set_reconciliation_disabled(True)
                prev = partition_offsets
                partition_offsets = self._wait_for_retention_to_apply(
                    prev, timeout_sec=90
                )
                self.logger.info(
                    f"retention advanced start offsets {prev} -> "
                    f"{partition_offsets} while reconciler paused "
                    f"(acked={producer.produce_status.acked})"
                )
                toggles += 1

            # Ensure reconciliation is enabled so it can drain, then let the producer
            # run to completion.
            self.set_reconciliation_disabled(False)
            producer.wait(timeout_sec=240)
            self.logger.info(
                f"producer finished with acked={producer.produce_status.acked}, "
                f"bad_offsets={producer.produce_status.bad_offsets}"
            )
        finally:
            producer.stop()

        assert producer.produce_status.acked > 0, "producer acked no records"
        assert toggles > 0, "expected toggling to have occurred"

        # Wait until all partitions are fully reconciled
        for partition in range(self.partition_count):
            self.wait_until_reconciled(topic=self.topic_name, partition=partition)

        # Retention must advance every partition even further now that the
        # backlog has drained.
        drained = self._wait_for_retention_to_apply(partition_offsets)
        assert all(drained[p] > partition_offsets[p] for p in partition_offsets), (
            f"retention did not advance every partition after draining: "
            f"{partition_offsets} -> {drained}"
        )

        # Finally, read the whole surviving log end-to-end.
        traffic_node = producer.nodes[0]
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            self.msg_size,
            loop=False,
            tolerate_data_loss=True,
            nodes=[traffic_node],
            producer=producer,
        )
        try:
            consumer.start(clean=False)
            consumer.wait(timeout_sec=240)
        finally:
            consumer.stop()

        status = consumer.consumer_status
        assert status.validator.valid_reads > 0, "consumer read no records"
        assert status.validator.invalid_reads == 0, (
            f"consumer saw invalid reads: {status.validator.invalid_reads}"
        )
