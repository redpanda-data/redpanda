# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
"""
End-to-end tests for the cutover-last tiered-storage -> cloud-topics migration.

Model (differs from the phase-1 cutover-first design these scenarios were first
written for):

  * A topic starts in 'tiered' storage mode and produces data that is archived
    to tiered storage (TS).
  * Altering redpanda.storage.mode to 'cloud'/'tiered_cloud' is the *trigger*.
    The partition stays *wholly TS-served* (reads route to the TS path) while a
    background archiver mirror copies the archival manifest into the cloud-topics
    L1 metastore as imported extents.
  * When the mirror has covered the whole manifest the partition *cuts over*:
    it advances partition_mode to cloud -- which flips routing to cloud topics
    (routing keys on partition_mode), serving the imported L1 extents -- empties
    the archival STM, and seeds the CT reconciliation baseline. Cutover is
    observable here as the archival manifest going empty.
  * Throughout, a consumer reading from offset 0 must see every record in order,
    and a read_committed consumer must see exactly the committed records.

These are observable-behavior tests; they validate the migration end to end and
are the intended validation vehicle for the C++ migration machinery (which is
otherwise only build-tested). The transaction scenarios are ported from the
phase-1 e2e because read_committed correctness across the migration boundary --
aborted-transaction ranges sourced from the per-segment tx_range_manifest in S3
once _rm_stm has dropped its state -- is exactly what the imported reader must
get right. The recovery and read-replica variants exercise the offline consume
paths (cluster recovery / read replica classify on the metastore migration
phase).
"""

import time

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from connectrpc.errors import ConnectError, ConnectErrorCode

from rptest.clients.admin.v2 import Admin as AdminV2, metastore_pb, ntp_pb
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.redpanda import (
    MetricsEndpoint,
    SISettings,
    make_redpanda_service,
)
from rptest.tests.redpanda_test import RedpandaTest


class TsToCtMigrationTest(RedpandaTest):
    NUM_PHASE1 = 500
    NUM_PHASE2 = 1000
    MSG_SIZE = 128
    TOPIC = "ts-ct-migration-test"

    TOPIC_CUTOVER = "ts-ct-migration-cutover-test"
    TOPIC_MULTI = "ts-ct-migration-multi-test"
    MULTI_PARTITION_COUNT = 4

    # Transactional parameters. msgs_per_transaction is intentionally large so
    # that when the migration trigger fires mid-stream at least one in-flight
    # transaction straddles the point where the mirror is running.
    TOPIC_TX = "ts-ct-migration-tx-test"
    NUM_TX_MSGS_PHASE1 = 10000
    NUM_TX_MSGS_PHASE2 = 1000
    TX_MSGS_PER_TXN = 200
    TX_ABORT_RATE = 0.3

    # Compaction parameters. A small key set so phase-2 keys supersede phase-1
    # keys (forcing cross-boundary compaction decisions), with tombstones.
    TOPIC_COMPACT = "ts-ct-migration-compact-test"
    NUM_COMPACT_PHASE1 = 4000
    NUM_COMPACT_PHASE2 = 4000
    COMPACT_KEY_CARDINALITY = 100
    COMPACT_TOMBSTONE_PROB = 0.3

    topics = ()

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(test_context, fast_uploads=True)
        super().__init__(
            test_context=test_context,
            num_brokers=1,
            si_settings=si_settings,
            extra_rp_conf={
                "cloud_topics_produce_batching_size_threshold": 65536,
                "enable_cluster_metadata_upload_loop": False,
                # Small segments + tight local retention so the archiver uploads
                # and prefix-truncates the local log quickly -- the precondition
                # for _rm_stm to drop abort state for the uploaded range.
                "log_segment_size_min": 1,
                "log_segment_ms_min": 1000,
                "log_segment_size": 1048576,
                "cloud_storage_housekeeping_interval_ms": 1000,
                "log_compaction_interval_ms": 1000,
                # Run cloud-topic (L1) compaction promptly so the compaction
                # test's quiesce wait converges quickly after cutover.
                "cloud_topics_compaction_interval_ms": 5000,
            },
        )
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    # ---- helpers ----------------------------------------------------------

    def _enable_migration(self):
        """Permit the tiered->cloud migration trigger. The storage_mode
        transition is gated on both the topic_mode_migration cluster feature and
        the enable_topic_mode_migration config; set both."""
        self.redpanda.set_feature_active("topic_mode_migration", True, timeout_sec=30)
        self.redpanda.set_cluster_config({"enable_topic_mode_migration": True})

    def _wait_for_ts_segment(self, topic: str):
        """Wait until at least one TS segment has reached object storage, so the
        partition has tiered data to migrate."""

        def has_ts_segment() -> bool:
            manifest = self.admin.get_partition_manifest(topic, 0)
            return len(manifest.get("segments", {})) >= 1

        wait_until(
            has_ts_segment,
            timeout_sec=120,
            backoff_sec=2,
            err_msg=f"No TS segment uploaded for {topic} within 120s",
            retry_on_exc=True,
        )

    def _trigger_migration(self, topic: str, storage_mode: str):
        """Flip the storage mode -- the migration trigger. The TSv2
        (tiered_cloud) destination is spelled storage.mode=tiered with the
        cluster default impl set to tiered_v2, since redpanda.storage.mode.impl
        is read-only after creation and tiered_cloud is not a settable
        storage.mode value."""
        if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2:
            self.redpanda.set_cluster_config(
                {"default_redpanda_storage_mode_tiered_impl": "tiered_v2"}
            )
            storage_mode = TopicSpec.STORAGE_MODE_TIERED
        self.rpk.alter_topic_config(
            topic, TopicSpec.PROPERTY_STORAGE_MODE, storage_mode
        )

    def _wait_for_cutover(self, topic: str):
        """Cutover advances partition_mode to cloud -- which flips routing to
        the cloud-topic path, since routing keys on partition_mode (the
        partition_properties field) -- and empties the archival STM manifest
        (reset_metadata). Observe it as the archival partition manifest
        becoming empty."""

        def cut_over() -> bool:
            manifest = self.admin.get_partition_manifest(topic, 0)
            return len(manifest.get("segments", {})) == 0

        wait_until(
            cut_over,
            timeout_sec=240,
            backoff_sec=5,
            err_msg=f"{topic} did not cut over to cloud topics within 240s",
            retry_on_exc=True,
        )

    def _l1_next_offset(self, topic: str, partition: int = 0) -> int | None:
        """The L1 metastore's next_offset for the partition -- the offset up to
        which the cloud-topic reconciler has materialized data into L1. Returns
        None if the partition is absent from the metastore. This is the signal
        that post-cutover writes actually reached the cloud-topic path: native
        CT writes (L0 -> reconciler -> L1) advance it; writes that go to the raft
        log + tiered storage instead do not."""
        metastore = AdminV2(self.redpanda).metastore()
        req = metastore_pb.GetOffsetsRequest(
            partition=ntp_pb.TopicPartition(topic=topic, partition=partition)
        )
        try:
            resp = metastore.get_offsets(req=req)
            return resp.offsets.next_offset
        except ConnectError as e:
            if e.code == ConnectErrorCode.NOT_FOUND:
                return None
            raise

    def _wait_for_cutover_all(self, topic: str, num_partitions: int):
        """Wait until every partition of the topic has cut over (its archival
        manifest is empty). Each partition migrates and cuts over
        independently."""

        def all_cut_over() -> bool:
            for p in range(num_partitions):
                m = self.admin.get_partition_manifest(topic, p)
                if len(m.get("segments", {})) != 0:
                    return False
            return True

        wait_until(
            all_cut_over,
            timeout_sec=240,
            backoff_sec=5,
            err_msg=f"not all partitions of {topic} cut over within 240s",
            retry_on_exc=True,
        )

    def _partition_start_offset(self, topic: str) -> int:
        parts = list(self.rpk.describe_topic(topic))
        return parts[0].start_offset if parts else 0

    def _records_removed(self) -> float:
        return self.redpanda.metric_sum(
            metric_name="vectorized_cloud_topics_compaction_worker_records_removed",
            metrics_endpoint=MetricsEndpoint.METRICS,
            expect_metric=True,
        )

    def _wait_for_compaction_quiesce(
        self, stable_sec: int = 20, timeout_sec: int = 240
    ):
        """Wait until CT (L1) compaction has converged -- the records-removed
        metric is unchanged for `stable_sec`. latest-value validation is only
        meaningful once the log is fully compacted to one value per key."""
        wait_until(
            lambda: self._records_removed() > 0,
            timeout_sec=120,
            backoff_sec=2,
            err_msg="CT compaction never removed any records",
            retry_on_exc=True,
        )
        state = {"prev": -1.0, "since": time.time()}

        def stable() -> bool:
            now = self._records_removed()
            if now != state["prev"]:
                state["prev"] = now
                state["since"] = time.time()
            return time.time() - state["since"] >= stable_sec

        wait_until(
            stable,
            timeout_sec=timeout_sec,
            backoff_sec=5,
            err_msg="CT compaction did not quiesce",
            retry_on_exc=True,
        )

    # ---- tests ------------------------------------------------------------

    @cluster(num_nodes=2)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
        ]
    )
    def test_ts_to_ct_migration(self, storage_mode: str):
        """Produce in tiered mode, trigger the migration mid-stream, and verify
        that a consumer reading from offset 0 sees every record in order both
        before and after cutover (TS passthrough then cloud-topic imported
        extents)."""
        self._enable_migration()
        if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        self.rpk.create_topic(
            self.TOPIC,
            partitions=1,
            replicas=1,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                "segment.bytes": str(32 * 1024),
                "retention.local.target.bytes": str(64 * 1024),
            },
        )

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC,
            msg_size=self.MSG_SIZE,
            msg_count=self.NUM_PHASE1 + self.NUM_PHASE2,
        )
        try:
            producer.start()
            # Once tiered data exists, trigger the migration while the producer
            # is still running (records keep flowing via the normal path --
            # TS-routed until cutover, cloud-topic after).
            self._wait_for_ts_segment(self.TOPIC)
            self._trigger_migration(self.TOPIC, storage_mode)
            producer.wait(timeout_sec=120)
        finally:
            producer.stop()
            producer.free()

        total = producer.produce_status.acked

        # The partition cuts over once the mirror converges.
        self._wait_for_cutover(self.TOPIC)

        # Read from offset 0 with the partition now cloud-topic-served and verify
        # all records are present and in order (imported extents for the
        # pre-cutover range, native CT for the residual).
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.TOPIC,
            loop=False,
        )
        consumer.start()
        consumer.wait(timeout_sec=120)
        status = consumer.consumer_status.validator
        assert status.invalid_reads == 0, f"invalid_reads={status.invalid_reads}"
        assert status.valid_reads >= total, (
            f"valid_reads={status.valid_reads} < produced {total}"
        )
        consumer.stop()
        consumer.free()

    @cluster(num_nodes=2)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
        ]
    )
    def test_ts_to_ct_migration_transactions(self, storage_mode: str):
        """read_committed correctness across the migration boundary.

        Scenarios exercised in one run:
          1. Aborted transactions fully within the TS range: resolved before the
             trigger, and after prefix truncation _rm_stm has dropped its
             abort-index state -- so reads of those offsets must source aborted
             ranges from the per-segment tx_range_manifest in S3 (the imported
             reader's tx-strip), not from _rm_stm.
          2. A transaction straddling the trigger: data records land in the TS
             range while its commit/abort control batch lands later.
          3. Post-cutover transactions on the cloud-topic write path.

        A read_committed consumer from offset 0 must see exactly the committed
        records and none of the aborted ones (invalid_reads > 0 is the failure
        signal).
        """
        self._enable_migration()
        if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        self.rpk.create_topic(
            self.TOPIC_TX,
            partitions=1,
            replicas=1,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                "segment.bytes": str(32 * 1024),
                "segment.ms": "1000",
                "retention.local.target.bytes": str(64 * 1024),
            },
        )

        phase1 = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_TX,
            msg_size=self.MSG_SIZE,
            msg_count=self.NUM_TX_MSGS_PHASE1,
            use_transactions=True,
            transaction_abort_rate=self.TX_ABORT_RATE,
            msgs_per_transaction=self.TX_MSGS_PER_TXN,
        )
        try:
            phase1.start()
            self._wait_for_ts_segment(self.TOPIC_TX)

            # Wait for prefix truncation so _rm_stm drops abort-index entries
            # below the local start offset; from here the per-segment
            # tx_range_manifest objects in S3 are the only source for aborted
            # ranges in the uploaded portion of the log.
            def is_log_truncated() -> bool:
                status = self.admin.get_partition_cloud_storage_status(self.TOPIC_TX, 0)
                return status.get("local_log_start_offset", 0) > 0

            wait_until(
                is_log_truncated,
                timeout_sec=120,
                backoff_sec=5,
                err_msg="Local log not prefix-truncated within 120s",
                retry_on_exc=True,
            )

            # Trigger mid-stream so a transaction straddles the boundary.
            self._trigger_migration(self.TOPIC_TX, storage_mode)
            phase1.wait(timeout_sec=120)
        finally:
            phase1.stop()
            phase1.free()

        # Post-cutover transactional records on the cloud-topic write path.
        self._wait_for_cutover(self.TOPIC_TX)
        phase2 = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_TX,
            msg_size=self.MSG_SIZE,
            msg_count=self.NUM_TX_MSGS_PHASE2,
            use_transactions=True,
            transaction_abort_rate=self.TX_ABORT_RATE,
            msgs_per_transaction=self.TX_MSGS_PER_TXN,
        )
        try:
            phase2.start()
            phase2.wait(timeout_sec=120)
        finally:
            phase2.stop()
            phase2.free()

        # A non-transactional sentinel after the last transaction, so the tail
        # of the log is never a run composed entirely of aborted records (which
        # a read_committed reader returns nothing for).
        self.rpk.produce(self.TOPIC_TX, "sentinel", "sentinel")

        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.TOPIC_TX,
            loop=False,
            # read_committed isolation: the verifier filters aborted-transaction
            # records and fails (invalid_reads) if any are observed.
            use_transactions=True,
        )
        consumer.start()
        consumer.wait(timeout_sec=180)
        status = consumer.consumer_status.validator
        assert status.invalid_reads == 0, (
            f"read_committed consumer saw aborted records: "
            f"invalid_reads={status.invalid_reads}"
        )
        consumer.stop()
        consumer.free()

    @cluster(num_nodes=2)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
        ]
    )
    def test_ts_to_ct_migration_compaction(self, storage_mode: str):
        """Compaction correctness across the migration boundary.

        A compacted (compact,delete) topic is migrated mid-stream. Phase-1 keyed
        records + tombstones are archived to tiered storage (and compacted there);
        phase-2 keyed records + tombstones over the SAME key set are written
        after the trigger. After cutover everything lives in L1 (imported extents
        for the pre-cutover range + native CT for the residual), and CT
        compaction runs over the unified log.

        A read from offset 0 with latest-value validation must see, for every
        key, exactly the last value the producer wrote for it -- no key
        resurrected by a stale pre-cutover value surviving a later tombstone, and
        no tombstone resurrecting a key. invalid_reads > 0 is the failure signal.
        """
        self._enable_migration()
        if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        self.rpk.create_topic(
            self.TOPIC_COMPACT,
            partitions=1,
            replicas=1,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                TopicSpec.PROPERTY_CLEANUP_POLICY: "compact,delete",
                "segment.bytes": str(32 * 1024),
                "segment.ms": "1000",
                "retention.local.target.bytes": str(64 * 1024),
                # Force compaction to run aggressively; keep data from aging out
                # of the log via time/size retention during the test.
                "min.cleanable.dirty.ratio": "0.0",
                "retention.ms": str(24 * 3600 * 1000),
            },
        )

        # A single producer over both phases so it owns one latest-value map
        # (phase-2 keys supersede phase-1 keys); the verifying consumer reads
        # that map. validate_latest_values requires the producer to outlive the
        # consumer, so it is stopped (not freed) before the consume.
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_COMPACT,
            msg_size=self.MSG_SIZE,
            msg_count=self.NUM_COMPACT_PHASE1 + self.NUM_COMPACT_PHASE2,
            key_set_cardinality=self.COMPACT_KEY_CARDINALITY,
            tombstone_probability=self.COMPACT_TOMBSTONE_PROB,
            validate_latest_values=True,
        )
        producer.start()
        try:
            # Trigger mid-stream, once tiered data (compacted on TS) exists.
            self._wait_for_ts_segment(self.TOPIC_COMPACT)
            self._trigger_migration(self.TOPIC_COMPACT, storage_mode)
            producer.wait_for_latest_value_map()
            producer.wait(timeout_sec=180)
        finally:
            producer.stop()

        self._wait_for_cutover(self.TOPIC_COMPACT)

        # CT (L1) compaction runs over the unified log (imported + native) after
        # cutover; latest-value validation only holds once it has converged to
        # one value per key, so wait for it to quiesce first.
        self._wait_for_compaction_quiesce()

        # Read the now-cloud-topic-served compacted log and validate that every
        # key resolves to the producer's last value for it.
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.TOPIC_COMPACT,
            msg_size=0,
            loop=False,
            compacted=True,
            validate_latest_values=True,
            nodes=[producer.nodes[0]],
        )
        consumer.start(clean=False)
        try:
            consumer.wait(timeout_sec=180)
            status = consumer.consumer_status.validator
            assert status.invalid_reads == 0, (
                f"compaction across migration resurrected/corrupted a key: "
                f"invalid_reads={status.invalid_reads}"
            )
        finally:
            consumer.stop()
            consumer.free()
            producer.free()

    @cluster(num_nodes=2)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
        ]
    )
    def test_ts_to_ct_migration_cutover_transition(self, storage_mode: str):
        """Migration liveness + correctness under concurrent writes.

        A producer runs continuously *through* the trigger and does not stop.
        The partition must cut over to cloud topics WHILE the producer is still
        writing -- migration does not require the workload to quiesce. No record
        is lost across the mid-stream cutover: a consumer from offset 0 sees
        every acked record, contiguous (data above the boundary is reconciled
        into L1 from the raft log, and writes arriving after cutover go to the
        cloud-topic path).

        Then -- the part that proves cutover is terminal -- post-cutover produce
        must go to the cloud-topic write path: the cloud-topic reconciler must
        materialize it into L1 (the L1 metastore next_offset advances to cover
        it), and the archival manifest must stay empty (the archiver is dormant,
        not re-uploading raft-log writes to tiered storage and re-triggering the
        migration). If post-cutover writes went to tiered storage instead, the
        L1 next_offset would not advance and the manifest would re-populate.
        """
        self._enable_migration()
        if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        self.rpk.create_topic(
            self.TOPIC_CUTOVER,
            partitions=1,
            replicas=1,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                "segment.bytes": str(32 * 1024),
                "retention.local.target.bytes": str(64 * 1024),
            },
        )

        # Rate-limited so archival keeps pace and the manifest stays bounded;
        # still high enough that segments seal and upload steadily. msg_count is
        # effectively unbounded so the producer never stops on its own.
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_CUTOVER,
            msg_size=self.MSG_SIZE,
            msg_count=100_000_000,
            rate_limit_bps=1024 * 1024,
        )
        producer.start()
        try:
            self._wait_for_ts_segment(self.TOPIC_CUTOVER)
            self._trigger_migration(self.TOPIC_CUTOVER, storage_mode)
            # Liveness: the partition must cut over while the producer is still
            # actively writing -- it does not require the producer to stop.
            acked_before = producer.produce_status.acked
            self._wait_for_cutover(self.TOPIC_CUTOVER)
            acked_after = producer.produce_status.acked
            assert acked_after > acked_before, (
                "producer made no progress across cutover -- the cutover did "
                "not happen under concurrent writes"
            )
            # Let a bit more land on the post-cutover (cloud-topic) path before
            # stopping, so the consume covers writes from both sides of cutover.
            time.sleep(5)
            acked = producer.produce_status.acked
            assert acked > self.NUM_PHASE1, "producer made too little progress"
        finally:
            producer.stop()
            producer.free()

        # No wipe: every acked record must be served from offset 0, contiguous.
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.TOPIC_CUTOVER,
            loop=False,
        )
        consumer.start()
        consumer.wait(timeout_sec=180)
        status = consumer.consumer_status.validator
        assert status.invalid_reads == 0, f"invalid_reads={status.invalid_reads}"
        assert status.offset_gaps == 0, (
            f"data stranded across cutover: offset_gaps={status.offset_gaps}"
        )
        assert status.valid_reads >= acked, (
            f"records lost across cutover: read {status.valid_reads} < acked {acked}"
        )
        consumer.stop()
        consumer.free()

        # Post-cutover writes must go to the cloud-topic path. Produce more and
        # require the reconciler to materialize it into L1 (next_offset advances)
        # while the archival manifest stays empty (archiver dormant, no TS
        # re-upload / re-migration).
        pre = self._l1_next_offset(self.TOPIC_CUTOVER)
        assert pre is not None, "partition absent from L1 metastore after cutover"
        post_count = 2000
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.TOPIC_CUTOVER,
            msg_size=self.MSG_SIZE,
            msg_count=post_count,
            timeout_sec=120,
        )

        def reconciled_into_l1() -> bool:
            nxt = self._l1_next_offset(self.TOPIC_CUTOVER)
            return nxt is not None and nxt >= pre + post_count

        wait_until(
            reconciled_into_l1,
            timeout_sec=120,
            backoff_sec=3,
            err_msg="post-cutover writes were not reconciled into L1 -- they "
            "did not go to the cloud-topic path (L1 next_offset did not "
            "advance to cover them)",
            retry_on_exc=True,
        )
        # And the archiver must not have re-uploaded them to tiered storage.
        manifest = self.admin.get_partition_manifest(self.TOPIC_CUTOVER, 0)
        assert len(manifest.get("segments", {})) == 0, (
            "archiver re-populated the TS manifest after cutover -- post-cutover "
            "writes went to tiered storage instead of the cloud-topic path"
        )

    @cluster(num_nodes=2)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
        ]
    )
    @cluster(num_nodes=2)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
        ]
    )
    def test_ts_to_ct_migration_multi_partition(self, storage_mode: str):
        """A multi-partition topic migrates: each partition runs its own mirror
        and cuts over independently (one trigger, fanned out). Exercises the
        per-partition cutover path and the sink's ntp -> topic_id_partition
        resolution under fan-out. A consumer from offset 0 must see every record
        across all partitions, contiguous per partition."""
        self._enable_migration()
        if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        self.rpk.create_topic(
            self.TOPIC_MULTI,
            partitions=self.MULTI_PARTITION_COUNT,
            replicas=1,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                "segment.bytes": str(32 * 1024),
                "retention.local.target.bytes": str(64 * 1024),
            },
        )

        # Enough records that every partition accrues tiered data to migrate.
        msg_count = self.MULTI_PARTITION_COUNT * 4000
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_MULTI,
            msg_size=self.MSG_SIZE,
            msg_count=msg_count,
        )
        try:
            producer.start()
            # Wait until every partition has a TS segment, then trigger.
            for p in range(self.MULTI_PARTITION_COUNT):
                wait_until(
                    lambda p=p: len(
                        self.admin.get_partition_manifest(self.TOPIC_MULTI, p).get(
                            "segments", {}
                        )
                    )
                    >= 1,
                    timeout_sec=120,
                    backoff_sec=2,
                    err_msg=f"partition {p} got no TS segment within 120s",
                    retry_on_exc=True,
                )
            self._trigger_migration(self.TOPIC_MULTI, storage_mode)
            producer.wait(timeout_sec=180)
        finally:
            producer.stop()
            producer.free()

        total = producer.produce_status.acked
        self._wait_for_cutover_all(self.TOPIC_MULTI, self.MULTI_PARTITION_COUNT)

        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.TOPIC_MULTI,
            loop=False,
        )
        consumer.start()
        consumer.wait(timeout_sec=180)
        status = consumer.consumer_status.validator
        assert status.invalid_reads == 0, f"invalid_reads={status.invalid_reads}"
        assert status.offset_gaps == 0, f"offset_gaps={status.offset_gaps}"
        assert status.valid_reads >= total, (
            f"records lost: read {status.valid_reads} < acked {total}"
        )
        consumer.stop()
        consumer.free()


class TsToCtMigrationRecoveryTest(RedpandaTest):
    """Whole-cluster metadata recovery of a partition that is mid tiered->cloud
    migration (E1).

    A partition still migrating is authoritative in tiered storage, not in the
    (incomplete) L1 mirror. The recovery backend must therefore recover it as
    tiered storage -- rebuilding its archival STM from the remote manifest --
    rather than bootstrapping it from L1. The live migration then resumes and
    cuts over.

    The flow keeps a producer running across the snapshot+wipe so the partition
    stays mid-migration (the cutover convergence rule requires a stable manifest
    tail, which cannot happen while the producer is appending), captures the
    migrating state in object storage (metastore manifest flush + controller
    snapshot), abruptly stops + wipes + recovers the cluster, and then verifies a
    consumer reading from offset 0 sees a correct, gap-free prefix of the data
    that reached tiered storage.
    """

    TOPIC = "ts-ct-recovery-test"
    MSG_SIZE = 128

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(test_context, fast_uploads=True)
        super().__init__(
            test_context=test_context,
            num_brokers=1,
            si_settings=si_settings,
            extra_rp_conf={
                "enable_topic_mode_migration": True,
                "cloud_topics_produce_batching_size_threshold": 65536,
                "log_segment_size_min": 1,
                "log_segment_ms_min": 1000,
                "log_segment_size": 1048576,
                "cloud_storage_housekeeping_interval_ms": 1000,
                "log_compaction_interval_ms": 1000,
                # Whole-cluster recovery: upload controller snapshots and the L1
                # metastore manifest frequently so the migrating state reaches
                # object storage quickly.
                "enable_cluster_metadata_upload_loop": True,
                "cloud_storage_cluster_metadata_upload_interval_ms": 1000,
                "controller_snapshot_max_age_sec": 1,
                "cloud_topics_long_term_flush_interval": 2000,
            },
        )
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    def _has_ts_segments(self, topic: str) -> bool:
        manifest = self.admin.get_partition_manifest(topic, 0)
        return len(manifest.get("segments", {})) >= 1

    @cluster(num_nodes=2)
    def test_ts_to_ct_migration_recovery(self):
        self.redpanda.set_feature_active("topic_mode_migration", True, timeout_sec=30)
        # Hold the partition in the migrating phase (the knob is a cluster config,
        # so it is captured in the controller snapshot and restored on recovery)
        # so the partition is recovered via the migrating branch -- as tiered
        # storage -- rather than racing a cutover.
        self.redpanda.set_cluster_config(
            {"cloud_topics_disable_migration_cutover_for_tests": True}
        )

        self.rpk.create_topic(
            self.TOPIC,
            partitions=1,
            replicas=1,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                "segment.bytes": str(32 * 1024),
                "retention.local.target.bytes": str(64 * 1024),
            },
        )

        # A long-running producer so there is substantial migrating state in
        # tiered storage to recover. Rate-limited so the recovered prefix the
        # consumer must drain below stays bounded on a fast or contended host
        # (the gating waits are time-based, so an unthrottled producer can build
        # an arbitrarily large prefix); 128 KiB/s still fills the 32 KiB segments
        # quickly enough for the upload waits.
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC,
            msg_size=self.MSG_SIZE,
            msg_count=100_000_000,
            rate_limit_bps=128 * 1024,
        )
        producer.start()
        try:
            wait_until(
                lambda: self._has_ts_segments(self.TOPIC),
                timeout_sec=120,
                backoff_sec=2,
                err_msg="No TS segment uploaded within 120s",
                retry_on_exc=True,
            )
            # Trigger the migration; the partition stays TS-served and the mirror
            # imports into L1 (marking the metastore phase migrating).
            self.rpk.alter_topic_config(
                self.TOPIC,
                TopicSpec.PROPERTY_STORAGE_MODE,
                TopicSpec.STORAGE_MODE_CLOUD,
            )
            # The manifest stays non-empty (still TS-served / migrating) because
            # the producer is running. Let the migrating state reach object
            # storage: controller snapshot + a couple of metastore flushes.
            wait_until(
                lambda: self._has_ts_segments(self.TOPIC),
                timeout_sec=60,
                backoff_sec=2,
                err_msg="partition cut over before recovery snapshot",
                retry_on_exc=True,
            )
            self.redpanda.wait_for_controller_snapshot(self.redpanda.nodes[0])
            time.sleep(8)
            acked_before = producer.produce_status.acked
            self.logger.info(f"acked before recovery: {acked_before}")
            assert acked_before > 0, "producer made no progress"
        finally:
            # Abruptly stop the broker while the producer is still running so the
            # partition is captured mid-migration (no cutover).
            self.redpanda.stop()
            producer.stop()
            producer.free()

        # Wipe local data and restart, then drive whole-cluster recovery.
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)
        self.redpanda.restart_nodes(
            self.redpanda.nodes,
            auto_assign_node_id=True,
            omit_seeds_on_idx_one=False,
        )
        self.redpanda._admin.await_stable_leader(
            "controller", partition=0, namespace="redpanda", timeout_s=60, backoff_s=2
        )
        self.redpanda._admin.initialize_cluster_recovery()

        def recovery_done() -> bool:
            state = self.redpanda._admin.get_cluster_recovery_status().json()["state"]
            if "failed" in state:
                raise RuntimeError(f"cluster recovery failed: {state}")
            return "inactive" in state

        wait_until(
            recovery_done,
            timeout_sec=120,
            backoff_sec=2,
            err_msg="cluster recovery did not complete within 120s",
            retry_on_exc=True,
        )

        # The migrating partition was recovered as tiered storage (the metastore
        # phase is migrating and cutover is held off): partition_mode is tiered,
        # so it is served as TS from the rebuilt archival STM.
        assert self.TOPIC in set(self.rpk.list_topics()), "topic was not recovered"

        # The migrating partition must come back AS TIERED STORAGE -- a non-empty
        # archival manifest -- not as an empty/native cloud topic. A recovery
        # phase misclassification recovers it empty (no manifest), so assert the
        # manifest is present here with a clear message rather than leaving it to
        # the consumer to hang on an empty partition.
        def recovered_as_tiered_storage() -> bool:
            try:
                m = self.admin.get_partition_manifest(self.TOPIC, 0)
            except Exception as e:
                self.logger.debug(f"manifest poll transient error: {e}")
                return False
            return len(m.get("segments", {})) > 0

        wait_until(
            recovered_as_tiered_storage,
            timeout_sec=120,
            backoff_sec=2,
            err_msg="recovered partition has no archival manifest -- it was not "
            "recovered as tiered storage (recovery phase misclassified)",
            retry_on_exc=True,
        )

        # The migration mirror must RESUME after recovery: the archival STM
        # restores its manifest asynchronously (after start()/leadership), so the
        # archiver is (re-)constructed off the archived-data-available edge. A
        # non-resuming mirror would still serve the static recovered prefix and
        # pass the consume check below, so assert forward progress explicitly --
        # the resumed mirror re-imports the recovered extents into L1, so L1's
        # next offset climbs from absent (None) to > 0.
        def mirror_resumed() -> bool:
            nxt = self._l1_next_offset(self.TOPIC)
            self.logger.info(f"post-recovery L1 next_offset: {nxt}")
            return nxt is not None and nxt > 0

        wait_until(
            mirror_resumed,
            timeout_sec=120,
            backoff_sec=2,
            err_msg="migration mirror did not resume after recovery (L1 import "
            "did not progress) -- the recovered archiver was not reconstructed",
            retry_on_exc=True,
        )

        # Cluster recovery reaching 'inactive' only means the controller
        # finished. The migrating partition is served from tiered storage via the
        # rebuilt archival STM, whose manifest is applied asynchronously -- until
        # it lands the partition reports high_watermark 0. A single-pass
        # (loop=False) consumer started against an empty partition latches onto
        # end offset 0 and blocks forever waiting for records that never arrive,
        # so wait for the recovered prefix to be served first. The threshold
        # matches the valid_reads assertion below; if the prefix never appears
        # this fails here with a clear message instead of an opaque consumer
        # timeout.
        def recovered_prefix_served() -> bool:
            hwm = list(self.rpk.describe_topic(self.TOPIC))[0].high_watermark or 0
            return hwm > 1000

        wait_until(
            recovered_prefix_served,
            timeout_sec=120,
            backoff_sec=2,
            err_msg="recovered partition did not expose its tiered-storage "
            "prefix (high_watermark stayed <= 1000)",
            retry_on_exc=True,
        )

        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.TOPIC,
            loop=False,
        )
        consumer.start()
        # Generous timeout: the recovered prefix is drained in one pass and the
        # consumer can be starved on a contended / low-resource host.
        consumer.wait(timeout_sec=300)
        status = consumer.consumer_status.validator
        # The recovered prefix (the data that reached tiered storage before the
        # wipe) must be correct and gap-free. The exact count is nondeterministic
        # -- records acked but not yet uploaded are legitimately lost on cluster
        # wipe -- so assert a substantial contiguous prefix rather than an exact
        # total.
        assert status.invalid_reads == 0, f"invalid_reads={status.invalid_reads}"
        assert status.offset_gaps == 0, (
            f"recovered prefix has gaps: offset_gaps={status.offset_gaps}"
        )
        assert status.valid_reads > 1000, (
            f"recovered partition served too few records: {status.valid_reads}"
        )
        consumer.stop()
        consumer.free()


class TsToCtMigrationReadReplicaTest(RedpandaTest):
    """Read replica of a source partition that is mid tiered->cloud migration
    (E2).

    The cutover-last mirror copies the migrating source's tiered-storage data
    into the source's L1 metastore as imported extents, which the source uploads
    for read-replica consumption. The cloud-topic read replica reads the source's
    migration_phase from the snapshot and serves the imported extents through the
    same L1 read path it uses for a native cloud topic -- so a replica of a
    still-migrating source returns a correct, gap-free prefix of the source's
    data (lagging the source tail until cutover).
    """

    TOPIC = "ts-ct-rr-test"
    MSG_SIZE = 128

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(test_context, fast_uploads=True)
        super().__init__(
            test_context=test_context,
            num_brokers=1,
            si_settings=si_settings,
            extra_rp_conf={
                "enable_topic_mode_migration": True,
                "cloud_topics_produce_batching_size_threshold": 65536,
                "log_segment_size_min": 1,
                "log_segment_ms_min": 1000,
                "log_segment_size": 1048576,
                "cloud_storage_housekeeping_interval_ms": 1000,
                "log_compaction_interval_ms": 1000,
                # Flush the L1 metastore manifest frequently so the migrating
                # source's imported extents are uploaded for the replica to read.
                "cloud_topics_long_term_flush_interval": 2000,
            },
        )
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)
        # The read replica must not own a bucket or write; it reads the source's
        # bucket. Disable the metastore flush / L0 GC loops on the replica.
        self.rr_settings = SISettings(
            test_context,
            bypass_bucket_creation=True,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )
        self.rr_settings.reset_cloud_storage_bucket(si_settings.cloud_storage_bucket)
        self.source_bucket = si_settings.cloud_storage_bucket
        self.second_cluster = None

    def _has_ts_segments(self, topic: str) -> bool:
        manifest = self.admin.get_partition_manifest(topic, 0)
        return len(manifest.get("segments", {})) >= 1

    @cluster(num_nodes=4)
    def test_ts_to_ct_migration_read_replica(self):
        self.redpanda.set_feature_active("topic_mode_migration", True, timeout_sec=30)
        # Hold the source in the migrating phase so the replica reads a
        # mid-migration source (rather than the source cutting over first).
        self.redpanda.set_cluster_config(
            {"cloud_topics_disable_migration_cutover_for_tests": True}
        )

        self.rpk.create_topic(
            self.TOPIC,
            partitions=1,
            replicas=1,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                "segment.bytes": str(32 * 1024),
                "retention.local.target.bytes": str(64 * 1024),
            },
        )

        # A producer so the source has migrating data for the replica to read.
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC,
            msg_size=self.MSG_SIZE,
            msg_count=100_000_000,
        )
        producer.start()
        try:
            wait_until(
                lambda: self._has_ts_segments(self.TOPIC),
                timeout_sec=120,
                backoff_sec=2,
                err_msg="No TS segment uploaded within 120s",
                retry_on_exc=True,
            )
            # Trigger the migration; the source stays TS-served and the mirror
            # imports into L1 (marking the metastore phase migrating).
            self.rpk.alter_topic_config(
                self.TOPIC,
                TopicSpec.PROPERTY_STORAGE_MODE,
                TopicSpec.STORAGE_MODE_CLOUD,
            )
            # Still migrating (manifest non-empty) -- give the mirror time to
            # import and the source to upload its L1 snapshot for the replica.
            wait_until(
                lambda: self._has_ts_segments(self.TOPIC),
                timeout_sec=60,
                backoff_sec=2,
                err_msg="source cut over before the replica could read it",
                retry_on_exc=True,
            )
            time.sleep(8)

            # Bring up the replica cluster (reads the source's bucket; no bucket
            # of its own) and create a read replica of the migrating source.
            self.second_cluster = make_redpanda_service(
                self.test_context,
                num_brokers=1,
                si_settings=self.rr_settings,
                extra_rp_conf={
                    "enable_cluster_metadata_upload_loop": False,
                    "cloud_topics_disable_metastore_flush_loop_for_tests": True,
                    "cloud_topics_disable_level_zero_gc_for_tests": True,
                },
            )
            self.second_cluster.start(start_si=False)
            rr_rpk = RpkTool(self.second_cluster)
            rr_rpk.create_topic(
                self.TOPIC,
                config={"redpanda.remote.readreplica": self.source_bucket},
            )

            def rr_has_leader() -> bool:
                parts = list(rr_rpk.describe_topic(self.TOPIC, tolerant=True))
                return len(parts) > 0 and all(p.leader != -1 for p in parts)

            wait_until(
                rr_has_leader,
                timeout_sec=90,
                backoff_sec=3,
                err_msg="read replica never got a leader",
                retry_on_exc=True,
            )

            # The replica reads the migrating source via its L1 imported extents.
            # Consume the available prefix from offset 0 and verify correctness.
            consumer = KgoVerifierSeqConsumer(
                self.test_context,
                self.second_cluster,
                self.TOPIC,
                loop=False,
            )
            consumer.start()
            consumer.wait(timeout_sec=120)
            status = consumer.consumer_status.validator
            assert status.invalid_reads == 0, (
                f"replica served incorrect records: "
                f"invalid_reads={status.invalid_reads}"
            )
            assert status.offset_gaps == 0, (
                f"replica prefix has gaps: offset_gaps={status.offset_gaps}"
            )
            assert status.valid_reads > 1000, (
                f"replica served too few records: {status.valid_reads}"
            )
            consumer.stop()
            consumer.free()
        finally:
            producer.stop()
            producer.free()
            if self.second_cluster is not None:
                self.second_cluster.stop()
