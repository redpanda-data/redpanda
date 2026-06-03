# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.admin.v2 import Admin as AdminV2, metastore_pb, ntp_pb
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.redpanda import MetricsEndpoint, SISettings
from rptest.tests.redpanda_test import RedpandaTest


class TsMigrationTest(RedpandaTest):
    """
    End-to-end test for the tiered-storage → cloud-topics migration path.

    A topic is first created in 'tiered' storage mode so that phase-1 records
    are archived via tiered storage (TS).  After at least one TS segment is
    uploaded the topic is promoted to 'cloud' or 'tiered_cloud' (parametrized),
    which records a migration boundary in the CTP STM.  Phase-2 records are
    then produced via the CT write path.  After the CT reconciler catches up,
    a consume from offset 0 must return all records in order — phase-1 records
    served via the TS passthrough reader and phase-2 records via CT.
    """

    NUM_PHASE1 = 500
    NUM_PHASE2 = 1000
    MSG_SIZE = 128
    TOPIC = "ts-migration-test"

    # Parameters for the transactional migration test.
    # msgs_per_transaction is intentionally large so that when migration fires
    # mid-stream, at least one in-flight transaction straddles the boundary.
    #
    # NUM_TX_MSGS_PHASE1 must be large enough that the total log volume for
    # phase 1 exceeds log_segment_size (1 MB, see extra_rp_conf below).
    # disk_space_manager::manage_data_disk() skips reclaim when
    # real_target_excess <= log_segment_size().  With TX_MSGS_PER_TXN=200 and
    # MSG_SIZE=128 each transactional batch is ~44 KB, so 50 transactions
    # (~10 000 messages) produce ~2.2 MB — well above the 1 MB threshold.
    TOPIC_GC = "ts-migration-gc-test"

    TOPIC_NO_UPLOAD = "ts-migration-no-upload-test"
    TOPIC_TX = "ts-migration-tx-test"
    NUM_TX_MSGS_PHASE1 = 10000
    NUM_TX_MSGS_PHASE2 = 1000
    TX_MSGS_PER_TXN = 200
    TX_ABORT_RATE = 0.3

    TOPIC_COMPACT = "ts-migration-compact-test"

    TOPIC_RETENTION = "ts-migration-retention-test"

    # Topics are created inside the test body after selecting storage mode.
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
                # Remove cluster-level minimums that would clamp topic-level
                # segment.bytes and segment.ms to 1 MB / 10 min respectively,
                # preventing segment rolling in the transactions test.
                "log_segment_size_min": 1,
                "log_segment_ms_min": 1000,
                # Set log_segment_size to its minimum (1 MB).
                # disk_space_manager::manage_data_disk() skips reclaim when
                # real_target_excess <= log_segment_size().  The default is
                # 128 MB which would never be exceeded by a small test log.
                # Setting it to 1 MB (the minimum allowed by bounded_property)
                # reduces the threshold to 1 MB; NUM_TX_MSGS_PHASE1 is sized
                # so that the phase-1 log (~2.2 MB) comfortably exceeds it.
                "log_segment_size": 1048576,
                # Cloud storage housekeeping defaults to 5 minutes; reduce to
                # 1 second so that local-retention enforcement (which evicts
                # uploaded segments) runs promptly and the is_log_truncated
                # wait completes well within the 120-second timeout.
                "cloud_storage_housekeeping_interval_ms": 1000,
                # Run log GC every second (default: 10s) so that uploaded
                # segments are evicted promptly once mark_clean is applied.
                "log_compaction_interval_ms": 1000,
                # Enable and run the disk space manager every second.
                # disk_space_manager::run_loop() skips all work when
                # _target_size == 0 (i.e. no capacity target is set), so
                # retention_local_trim_interval alone does nothing.  Setting a
                # tiny cluster-level capacity target activates the manager,
                # which calls get_reclaimable_offsets() → set_cloud_gc_offset()
                # on each cloud/tiered partition — the path that respects
                # retention.local.target.bytes regardless of strict-mode flags.
                "retention_local_target_capacity_bytes": 1024,
                "retention_local_trim_interval": 1000,
                # Run the CT (L1) compaction scheduler frequently so the
                # compaction test observes several rounds quickly.
                "cloud_topics_compaction_interval_ms": 5000,
            },
        )
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)
        self.admin_v2 = AdminV2(self.redpanda)

    def _metric_sum(self, metric_name: str) -> float:
        return self.redpanda.metric_sum(
            metric_name=metric_name,
            metrics_endpoint=MetricsEndpoint.METRICS,
            expect_metric=True,
        )

    def get_records_removed(self) -> float:
        # Superseded records dropped by key dedup (does not count tombstone
        # removals).
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_worker_records_removed"
        )

    def get_tombstones_removed(self) -> float:
        return self._metric_sum(
            "vectorized_cloud_topics_compaction_worker_tombstones_removed"
        )

    @cluster(num_nodes=2)
    @matrix(storage_mode=[
        TopicSpec.STORAGE_MODE_CLOUD,
        TopicSpec.STORAGE_MODE_TIERED_CLOUD,
    ])
    def test_ts_to_ct_migration(self, storage_mode: str):
        if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        self.rpk.create_topic(
            self.TOPIC,
            partitions=1,
            replicas=1,
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED},
        )

        # Phase 1: produce into the tiered topic
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.TOPIC,
            msg_size=self.MSG_SIZE,
            msg_count=self.NUM_PHASE1,
        )

        # Wait for TS archival to upload at least one segment
        def has_ts_segment() -> bool:
            manifest = self.admin.get_partition_manifest(self.TOPIC, 0)
            return len(manifest.get("segments", {})) >= 1

        wait_until(
            has_ts_segment,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="No TS segment uploaded within 120s",
            retry_on_exc=True,
        )

        # Start phase-2 producer before promoting the topic so that produces are
        # in-flight when the config change lands.
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC,
            msg_size=self.MSG_SIZE,
            msg_count=self.NUM_PHASE2,
        )
        try:
            producer.start()

            # Wait until the producer has acknowledged at least 50 records so
            # the config change is guaranteed to race with active writes.
            producer.wait_for_acks(50, timeout_sec=30, backoff_sec=0.5)

            # Promote to cloud/tiered_cloud — records a migration boundary in
            # the CTP STM.  Some phase-2 records will have been written as
            # tiered-storage records before this lands; they fall after the
            # boundary and are served via CT once reconciled.
            self.rpk.alter_topic_config(
                self.TOPIC, TopicSpec.PROPERTY_STORAGE_MODE, storage_mode
            )

            producer.wait(timeout_sec=120)
        finally:
            producer.stop()
            producer.free()

        total = self.NUM_PHASE1 + self.NUM_PHASE2

        # Wait for the CT reconciler to process all records
        def is_reconciled() -> bool:
            metastore = self.admin_v2.metastore()
            req = metastore_pb.GetOffsetsRequest(
                partition=ntp_pb.TopicPartition(topic=self.TOPIC, partition=0)
            )
            return metastore.get_offsets(req=req).offsets.next_offset >= total

        wait_until(
            is_reconciled,
            timeout_sec=180,
            backoff_sec=5,
            err_msg=f"CT reconciler did not process all {total} records within 180s",
            retry_on_exc=True,
        )

        # Consume from offset 0 and verify all records arrive in order.
        # Phase-1 records are served via the TS passthrough reader; phase-2
        # records via the CT L1/L0 reader.
        out = self.rpk.consume(
            self.TOPIC,
            n=total,
            partition=0,
            offset=0,
            format="%o\n",
            timeout=180,
            read_committed=True,
        )

        offsets = [int(line) for line in out.splitlines() if line.strip()]
        assert len(offsets) == total, (
            f"Expected {total} records, got {len(offsets)}"
        )
        assert offsets == list(range(total)), (
            f"Offset sequence broken: first={offsets[0]}, "
            f"last={offsets[-1]}, len={len(offsets)}"
        )

    @cluster(num_nodes=2)
    @matrix(storage_mode=[
        TopicSpec.STORAGE_MODE_CLOUD,
        TopicSpec.STORAGE_MODE_TIERED_CLOUD,
    ])
    def test_ts_to_ct_migration_transactions(self, storage_mode: str):
        """
        Verify that read_committed semantics are correct for Kafka transactions
        that touch the TS→CT migration boundary.  Two distinct scenarios are
        exercised in a single producer run:

        1. Aborted transactions fully within the TS range.  The producer runs
           and resolves some transactions (committed and aborted) before
           migration.  The test waits for prefix truncation so that _rm_stm
           discards its abort-index state for the S3 range.  After migration,
           reads of those pre-migration offsets must source aborted-transaction
           ranges from the per-segment tx_range_manifest in S3, not from
           _rm_stm, which no longer holds that state.

        2. Transactions straddling the migration boundary.  The producer is
           still running when the storage-mode change lands, so at least one
           transaction has data records in the TS (S3) range while its
           commit/abort control batch lands in the post-migration CT raft log.

        In both cases a read_committed consumer must see exactly the committed
        records and none of the aborted ones.  invalid_reads > 0 from the seq
        consumer is the failure signal.
        """
        if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        # Small segments and tight local retention force the archiver to upload
        # quickly and prefix-truncate the local log, which is the precondition
        # for _rm_stm to drop its abort state for the S3 range.
        #
        # segment.ms is used instead of (or in addition to) segment.bytes
        # because transactional produce batches are large enough (~44 KB each
        # for msgs_per_transaction=200) to prevent reliable byte-based rolling.
        # A 1-second time limit guarantees multiple segments even when the
        # producer finishes before any byte threshold is crossed.
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

        phase1_producer = KgoVerifierProducer(
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
            phase1_producer.start()

            # Wait for at least one TS segment to reach S3.
            def has_ts_segment() -> bool:
                manifest = self.admin.get_partition_manifest(self.TOPIC_TX, 0)
                return len(manifest.get("segments", {})) >= 1

            wait_until(
                has_ts_segment,
                timeout_sec=120,
                backoff_sec=5,
                err_msg="No TS segment uploaded within 120s",
                retry_on_exc=True,
            )

            # Wait for prefix truncation.  Once local_log_start_offset > 0,
            # _rm_stm has taken a snapshot that drops abort-index entries fully
            # below start_offset.  From this point the per-segment
            # tx_range_manifest objects in S3 are the only correct source for
            # aborted-transaction ranges in the S3 portion of the log.
            def is_log_truncated() -> bool:
                status = self.admin.get_partition_cloud_storage_status(
                    self.TOPIC_TX, 0
                )
                return status.get("local_log_start_offset", 0) > 0

            wait_until(
                is_log_truncated,
                timeout_sec=120,
                backoff_sec=5,
                err_msg="Local log not prefix-truncated within 120s",
                retry_on_exc=True,
            )

            # Promote to CT while the producer is still running so that at
            # least one in-flight transaction straddles the boundary: its data
            # records land in the TS range while its commit/abort control batch
            # lands in the post-migration CT raft log.
            self.rpk.alter_topic_config(
                self.TOPIC_TX, TopicSpec.PROPERTY_STORAGE_MODE, storage_mode
            )

            phase1_producer.wait(timeout_sec=120)
        finally:
            phase1_producer.stop()
            phase1_producer.free()

        total_committed = (
            phase1_producer.produce_status.acked
            - phase1_producer.produce_status.aborted_transaction_messages
        )

        # Phase 2: produce post-migration transactional records via the CT
        # write path.  This is required for two reasons:
        #   1. The CT reconciler only registers the NTP in the metastore when
        #      it processes its first L0 batch.  Without phase-2 data there is
        #      nothing to reconcile and is_reconciled would time out with
        #      metastore::errc::not_found.
        #   2. Phase-2 records exercise read_committed semantics for
        #      transactions written entirely in CT mode alongside phase-1
        #      records served via the TS passthrough reader.
        phase2_producer = KgoVerifierProducer(
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
            phase2_producer.start()
            phase2_producer.wait(timeout_sec=120)
        finally:
            phase2_producer.stop()
            phase2_producer.free()

        # Sentinel: one non-transactional record appended after the phase-2
        # transactional producer.  The CT reconciler uses read_committed_reader
        # internally and cannot advance LRO past a trailing all-aborted range
        # (read_committed_reader returns no batches for a range composed
        # entirely of aborted-transaction records).  By guaranteeing a
        # committed record at the very end of the log the sentinel ensures LRO
        # can always reach HWM, making the is_reconciled check reliable.
        sentinel_producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_TX,
            msg_size=self.MSG_SIZE,
            msg_count=1,
        )
        try:
            sentinel_producer.start()
            sentinel_producer.wait(timeout_sec=30)
        finally:
            sentinel_producer.stop()
            sentinel_producer.free()

        # Committed record count: acked includes both committed and aborted
        # data records; subtract the aborted ones.  Add 1 for the sentinel.
        total_committed += (
            phase2_producer.produce_status.acked
            - phase2_producer.produce_status.aborted_transaction_messages
            + 1
        )

        # Wait for the CT reconciler to process all post-migration records.
        # The HWM covers the full Kafka offset space (data + aborted records +
        # control batches), so next_offset >= HWM confirms the reconciler has
        # caught up to the end of the log.  The sentinel guarantees at least
        # one committed record at the tail so LRO can advance to HWM even when
        # all phase-2 transactions are aborted.
        def is_reconciled() -> bool:
            partitions = list(self.rpk.describe_topic(self.TOPIC_TX))
            if not partitions:
                return False
            hwm = partitions[0].high_watermark
            metastore = self.admin_v2.metastore()
            req = metastore_pb.GetOffsetsRequest(
                partition=ntp_pb.TopicPartition(
                    topic=self.TOPIC_TX, partition=0
                )
            )
            return metastore.get_offsets(req=req).offsets.next_offset >= hwm

        wait_until(
            is_reconciled,
            timeout_sec=180,
            backoff_sec=5,
            err_msg="CT reconciler did not catch up to HWM within 180s",
            retry_on_exc=True,
        )

        # Consume from offset 0 with read_committed isolation and validate the
        # full sequence.  The seq consumer checks that every committed record
        # appears in key order and that no records from aborted transactions are
        # present.
        #
        # loop=False causes kgo-verifier to read to the current LSO once and
        # exit rather than looping indefinitely.  This avoids the deadlock
        # where max_offsets_consumed can never reach max_offsets_produced when
        # the last produced transaction was aborted (aborted records are
        # filtered by read_committed, so the consumer never consumes their
        # offsets).
        #
        # The expected failure mode for the known bug: invalid_reads > 0
        # because frontend::aborted_transactions returns empty for pre-migration
        # S3 offsets (it queries _rm_stm, which has already discarded abort
        # state for those offsets during prefix truncation).  The read_committed
        # consumer then receives aborted records as if they were committed.
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.TOPIC_TX,
            msg_size=self.MSG_SIZE,
            use_transactions=True,
            loop=False,
        )

        try:
            consumer.start()
            consumer.wait(timeout_sec=180)
        finally:
            consumer.stop()
            consumer.free()

        status = consumer.consumer_status
        assert status.validator.invalid_reads == 0, (
            f"invalid_reads={status.validator.invalid_reads}: records from aborted "
            f"transactions appeared under read_committed isolation. "
            f"This indicates frontend::aborted_transactions returned empty for "
            f"pre-migration S3 offsets because _rm_stm discarded abort state "
            f"for those offsets during prefix truncation. "
            f"valid_reads={status.validator.valid_reads}, committed={total_committed}"
        )
        assert status.validator.valid_reads == total_committed, (
            f"valid_reads={status.validator.valid_reads} != committed={total_committed}: "
            f"some committed records were not returned under read_committed isolation"
        )

    @cluster(num_nodes=2)
    @matrix(storage_mode=[
        TopicSpec.STORAGE_MODE_CLOUD,
        TopicSpec.STORAGE_MODE_TIERED_CLOUD,
    ])
    def test_ts_to_ct_migration_gc(self, storage_mode: str):
        """
        Verify that the archival STM continues to GC pre-migration TS segments
        after a topic is promoted from tiered to cloud/tiered_cloud.

        After migration the ntp_archiver must continue running housekeeping so
        that apply_retention() + garbage_collect() advance start_offset and
        delete aged-out TS objects from S3.  The test sets a short retention.ms
        so that all uploaded segments are eligible for deletion immediately after
        the retention window elapses, then waits for start_offset to advance in
        the archival metadata STM manifest.

        NOTE: This test is expected to fail until the ntp_archiver (or an
        equivalent driver) is kept alive after the storage mode transition.
        """
        if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        # Short retention: all uploaded segments should be eligible for GC
        # well within the 60-second wait below.
        RETENTION_MS = 10_000  # 10 s

        self.rpk.create_topic(
            self.TOPIC_GC,
            partitions=1,
            replicas=1,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                "segment.bytes": str(32 * 1024),
                "segment.ms": "1000",
                "retention.ms": str(RETENTION_MS),
                "retention.local.target.bytes": str(64 * 1024),
            },
        )

        # Produce enough data that several segments are uploaded to S3.
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.TOPIC_GC,
            msg_size=self.MSG_SIZE,
            msg_count=2000,
        )

        # Wait for at least two TS segments to be uploaded so there is
        # something eligible for GC.
        def has_ts_segments() -> bool:
            manifest = self.admin.get_partition_manifest(self.TOPIC_GC, 0)
            return len(manifest.get("segments", {})) >= 2

        wait_until(
            has_ts_segments,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="Fewer than 2 TS segments uploaded within 120s",
            retry_on_exc=True,
        )

        manifest_before = self.admin.get_partition_manifest(self.TOPIC_GC, 0)
        segments_before = len(manifest_before.get("segments", {}))
        self.logger.info(
            f"Before migration: {segments_before} segments in manifest, "
            f"start_offset={manifest_before.get('start_offset', 0)}"
        )

        # Promote the topic to cloud/tiered_cloud.
        self.rpk.alter_topic_config(
            self.TOPIC_GC, TopicSpec.PROPERTY_STORAGE_MODE, storage_mode
        )

        # The archival STM should continue running housekeeping after the
        # storage mode change.  Once the retention window elapses,
        # apply_retention() advances start_offset and garbage_collect() deletes
        # the stale TS objects.  cloud_storage_housekeeping_interval_ms=1s
        # (set in __init__) means the loop runs frequently; the 60-second
        # timeout gives it ample time to fire several times after RETENTION_MS
        # has elapsed.
        def ts_segments_gc_d() -> bool:
            manifest = self.admin.get_partition_manifest(self.TOPIC_GC, 0)
            start_offset = manifest.get("start_offset", 0)
            segments = len(manifest.get("segments", {}))
            self.logger.info(
                f"GC check: start_offset={start_offset}, "
                f"segments={segments}/{segments_before}"
            )
            # start_offset is reset to 0 when no segments remain
            # (partition_manifest::truncate() clears it when empty), so check
            # segment count instead.
            return segments < segments_before

        wait_until(
            ts_segments_gc_d,
            timeout_sec=60,
            backoff_sec=5,
            err_msg=(
                "Archival STM did not GC pre-migration TS segments within 60s "
                "after storage mode change. Segment count did not decrease, "
                "indicating the ntp_archiver is not running housekeeping "
                "after the tiered->cloud storage mode transition."
            ),
            retry_on_exc=True,
        )

        manifest_after = self.admin.get_partition_manifest(self.TOPIC_GC, 0)
        segments_after = len(manifest_after.get("segments", {}))
        self.logger.info(
            f"After GC: {segments_after} segments remain "
            f"(was {segments_before}), "
            f"start_offset={manifest_after.get('start_offset', 0)}"
        )
        assert segments_after < segments_before, (
            f"Expected segments to decrease after GC: "
            f"before={segments_before}, after={segments_after}"
        )

    @cluster(num_nodes=2)
    @matrix(storage_mode=[
        TopicSpec.STORAGE_MODE_CLOUD,
        TopicSpec.STORAGE_MODE_TIERED_CLOUD,
    ])
    def test_no_ts_uploads_after_migration(self, storage_mode: str):
        """
        Verify that the TS archiver does NOT upload new segments after a topic
        is promoted from tiered to cloud/tiered_cloud.

        Once the CTP STM records a migration boundary, the ntp_archiver must
        stop uploading raft-log data as new TS segments.  All post-migration
        data flows through the CT write path and is reconciled by the CT
        reconciler, not the TS archiver.

        Failure mode without the fix: the archiver continues its upload loop
        after migration and new TS segment objects appear in the archival
        manifest for records that were written via the CT path.
        """
        if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        # cloud_storage_segment_max_upload_interval_sec=10 (set by fast_uploads).
        # One full upload cycle is the unit of time used throughout this test.
        UPLOAD_INTERVAL_SEC = 10

        self.rpk.create_topic(
            self.TOPIC_NO_UPLOAD,
            partitions=1,
            replicas=1,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                "segment.bytes": str(32 * 1024),
                "segment.ms": "1000",
            },
        )

        # Phase 1: produce enough data for several TS segments so the manifest
        # is non-trivial and any subsequent upload is unambiguously new.
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.TOPIC_NO_UPLOAD,
            msg_size=self.MSG_SIZE,
            msg_count=2000,
        )

        def has_ts_segments() -> bool:
            manifest = self.admin.get_partition_manifest(self.TOPIC_NO_UPLOAD, 0)
            return len(manifest.get("segments", {})) >= 2

        wait_until(
            has_ts_segments,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="Fewer than 2 TS segments uploaded within 120s",
            retry_on_exc=True,
        )

        # Promote to cloud/tiered_cloud — records the migration boundary.
        self.rpk.alter_topic_config(
            self.TOPIC_NO_UPLOAD, TopicSpec.PROPERTY_STORAGE_MODE, storage_mode
        )

        # Allow two upload cycles for any segment that was mid-upload at
        # promotion time to complete, then record the settled manifest state.
        # After this point no further TS uploads should occur.
        time.sleep(2 * UPLOAD_INTERVAL_SEC)

        def manifest_state() -> tuple[int, int]:
            m = self.admin.get_partition_manifest(self.TOPIC_NO_UPLOAD, 0)
            return (len(m.get("segments", {})), m.get("last_offset", 0))

        baseline_count, baseline_last_offset = manifest_state()
        self.logger.info(
            f"Settled manifest after migration: {baseline_count} segments, "
            f"last_offset={baseline_last_offset}"
        )

        # Phase 2: produce data via the CT write path.  This gives the
        # archiver new log content to upload if it were not suppressed.
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.TOPIC_NO_UPLOAD,
            msg_size=self.MSG_SIZE,
            msg_count=2000,
        )

        # Wait for CT reconciliation: confirms phase-2 records were written
        # and the reconciler has processed them, so the data definitely exists
        # in the raft log for the archiver to find.
        def is_reconciled() -> bool:
            partitions = list(self.rpk.describe_topic(self.TOPIC_NO_UPLOAD))
            if not partitions:
                return False
            hwm = partitions[0].high_watermark
            metastore = self.admin_v2.metastore()
            req = metastore_pb.GetOffsetsRequest(
                partition=ntp_pb.TopicPartition(
                    topic=self.TOPIC_NO_UPLOAD, partition=0
                )
            )
            return metastore.get_offsets(req=req).offsets.next_offset >= hwm

        wait_until(
            is_reconciled,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="CT reconciler did not process phase-2 records within 120s",
            retry_on_exc=True,
        )

        # Wait 3 upload cycles.  An unsuppressed archiver runs every
        # UPLOAD_INTERVAL_SEC and would have uploaded the phase-2 records as
        # new TS segments multiple times over by now.
        time.sleep(3 * UPLOAD_INTERVAL_SEC)

        final_count, final_last_offset = manifest_state()
        self.logger.info(
            f"Final manifest state: {final_count} segments, "
            f"last_offset={final_last_offset} "
            f"(baseline: {baseline_count}, {baseline_last_offset})"
        )

        assert final_count == baseline_count, (
            f"TS archiver uploaded {final_count - baseline_count} new segment(s) "
            f"after CT migration (storage_mode={storage_mode}). "
            f"baseline_count={baseline_count}, final_count={final_count}. "
            f"Post-migration TS uploads indicate the archiver was not suppressed "
            f"after the tiered→CT storage mode transition."
        )
        assert final_last_offset == baseline_last_offset, (
            f"manifest last_offset advanced from {baseline_last_offset} to "
            f"{final_last_offset} after CT migration, confirming post-migration "
            f"TS segment uploads occurred."
        )

    @cluster(num_nodes=2)
    @matrix(storage_mode=[
        TopicSpec.STORAGE_MODE_CLOUD,
        TopicSpec.STORAGE_MODE_TIERED_CLOUD,
    ])
    def test_ts_to_ct_migration_compaction(self, storage_mode: str):
        """
        Compaction on a topic mid TS->CT migration must behave like tiered
        storage: CT (L1) compaction dedups within its own (CT) range, but
        tombstones must NOT be removed while the pre-migration TS section is
        still present.  A CT tombstone can supersede a key whose last value is
        in the TS range (served via the passthrough reader); removing it would
        resurrect that key.  Once the TS section ages out and the migration
        completes (the seal clears), tombstones become removable again.

        delete.retention.ms is intentionally short so that the only thing
        preventing tombstone removal during migration is the seal; if the gate
        were missing, tombstones would be removed almost immediately.
        """
        if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        KEY_CARD = 100

        # retention.ms starts large so the pre-migration TS section persists
        # through the "migrating" assertions; it is shortened later to age the
        # TS section out and complete the migration.
        self.rpk.create_topic(
            self.TOPIC_COMPACT,
            partitions=1,
            replicas=1,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                TopicSpec.PROPERTY_CLEANUP_POLICY: "compact,delete",
                "segment.bytes": str(32 * 1024),
                "segment.ms": "1000",
                "delete.retention.ms": "1000",
                "retention.ms": str(24 * 3600 * 1000),
                "retention.local.target.bytes": str(64 * 1024),
                "min.cleanable.dirty.ratio": "0.0",
            },
        )

        # Phase 1: keyed records + tombstones archived to tiered storage.
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.TOPIC_COMPACT,
            msg_size=self.MSG_SIZE,
            msg_count=4000,
            key_set_cardinality=KEY_CARD,
            tombstone_probability=0.3,
        )

        def has_ts_segments() -> bool:
            m = self.admin.get_partition_manifest(self.TOPIC_COMPACT, 0)
            return len(m.get("segments", {})) >= 2

        wait_until(
            has_ts_segments,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="Fewer than 2 TS segments uploaded within 120s",
            retry_on_exc=True,
        )

        # Migrate -> records the migration seal.
        self.rpk.alter_topic_config(
            self.TOPIC_COMPACT, TopicSpec.PROPERTY_STORAGE_MODE, storage_mode
        )

        # Phase 2: keyed records + tombstones written via the CT path (these
        # land in the CT range above the migration boundary, in L1).
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.TOPIC_COMPACT,
            msg_size=self.MSG_SIZE,
            msg_count=4000,
            key_set_cardinality=KEY_CARD,
            tombstone_probability=0.3,
        )

        # CT compaction must dedup the CT range (records removed)...
        wait_until(
            lambda: self.get_records_removed() > 0,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="CT compaction removed no records while migrating",
        )

        # ...but must NOT remove tombstones while the seal is set, even though
        # delete.retention.ms is short. Give compaction several more rounds to
        # make the point robust, then assert no tombstones were removed.
        time.sleep(20)
        tombstones_removed_migrating = self.get_tombstones_removed()
        assert tombstones_removed_migrating == 0, (
            f"{tombstones_removed_migrating} tombstone(s) were removed by CT "
            f"compaction while the partition was mid-migration; tombstones must "
            f"be retained until the TS section ages out (storage_mode="
            f"{storage_mode})."
        )

        # Age out the TS section: shorten retention so the archiver GC drains
        # the TS manifest. An empty manifest triggers complete_migration, which
        # clears the seal and makes the partition a native cloud topic.
        self.rpk.alter_topic_config(
            self.TOPIC_COMPACT, "retention.ms", "1000"
        )

        def ts_drained() -> bool:
            m = self.admin.get_partition_manifest(self.TOPIC_COMPACT, 0)
            segments = len(m.get("segments", {}))
            self.logger.info(f"TS drain check: {segments} segments remain")
            return segments == 0

        wait_until(
            ts_drained,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="TS section did not fully drain after shortening retention",
            retry_on_exc=True,
        )

        # Restore a large retention before producing phase 3. The short
        # retention that drained the TS section also evicts CT data; if left in
        # place it would delete phase-3 (and its tombstones) before compaction
        # could remove them, so tombstone removal could never be observed.
        # delete.retention.ms stays short so phase-3 tombstones become removable
        # quickly once the seal has cleared.
        self.rpk.alter_topic_config(
            self.TOPIC_COMPACT, "retention.ms", str(24 * 3600 * 1000)
        )

        # Phase 3: produce more to drive a compaction round after the seal has
        # cleared.
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.TOPIC_COMPACT,
            msg_size=self.MSG_SIZE,
            msg_count=4000,
            key_set_cardinality=KEY_CARD,
            tombstone_probability=0.3,
        )

        # With the migration complete, tombstones past delete.retention.ms are
        # once again removable.
        wait_until(
            lambda: self.get_tombstones_removed() > 0,
            timeout_sec=180,
            backoff_sec=5,
            err_msg=(
                "no tombstones were removed after the migration completed; "
                "tombstone removal should resume once the TS section ages out "
                f"(storage_mode={storage_mode})."
            ),
        )

    @cluster(num_nodes=2)
    @matrix(storage_mode=[
        TopicSpec.STORAGE_MODE_CLOUD,
        TopicSpec.STORAGE_MODE_TIERED_CLOUD,
    ])
    def test_ts_to_ct_migration_retention_bytes(self, storage_mode: str):
        """
        retention.bytes is a whole-partition budget. During a TS->CT migration
        the post-migration data lives in cloud topics (L1), outside the
        tiered-storage manifest the archiver reclaims from, so the archiver must
        count those CT bytes when deciding how much TS to remove. Otherwise a
        partition whose TS section is under the byte budget on its own would
        never be trimmed no matter how much CT data accumulated.

        This drives the *archive/spillover* retention path:
        cloud_storage_spillover_manifest_max_segments is set low so the TS
        manifest spills over (archive_size_bytes > 0), and retention is computed
        against the whole partition there too.

        Flow:
          1. Produce TS data and let it spill over; the TS cloud log is under the
             byte budget on its own.
          2. Migrate. Set retention.bytes just above the observed TS size.
          3. Produce CT data that pushes the partition over the budget. The
             archiver must trim the (spilled) TS section -> cloud_log_start_offset
             advances even though TS alone is still under budget.
          4. Shorten retention.ms; the remaining TS ages out and the manifest
             drains to empty -- completion under time-based retention.
        """
        if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        # Force spillover so the archive-region retention path is exercised:
        # a small per-manifest segment cap, and no size-based spillover cap that
        # would otherwise keep the small test segments in the STM region.
        self.redpanda.set_cluster_config(
            {"cloud_storage_spillover_manifest_max_segments": 4}
        )
        self.redpanda.set_cluster_config_to_null(
            "cloud_storage_spillover_manifest_size"
        )

        # retention.ms starts large-but-finite: finite so the migration is
        # permitted (infinite retention is rejected), large so time retention
        # does not fire during the size-retention phase. retention.bytes is set
        # later, from the observed TS size, to make the test robust to the exact
        # on-disk segment overhead.
        self.rpk.create_topic(
            self.TOPIC_RETENTION,
            partitions=1,
            replicas=1,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                "segment.bytes": str(32 * 1024),
                "segment.ms": "1000",
                "retention.ms": str(24 * 3600 * 1000),
                "retention.local.target.bytes": str(64 * 1024),
            },
        )

        # Phase 1: TS data, uploaded and spilled into the archive region.
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.TOPIC_RETENTION,
            msg_size=self.MSG_SIZE,
            msg_count=6000,
        )

        # Wait until the manifest has spilled over (archive region non-empty)
        # and the cloud log size has settled.
        def spilled_over() -> bool:
            status = self.admin.get_partition_cloud_storage_status(
                self.TOPIC_RETENTION, 0
            )
            archive = status.get("archive_size_bytes", 0)
            cloud = status.get("cloud_log_size_bytes", 0)
            self.logger.info(
                f"spillover check: archive_size_bytes={archive}, "
                f"cloud_log_size_bytes={cloud}"
            )
            return archive > 0

        wait_until(
            spilled_over,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="TS manifest did not spill over within 120s",
            retry_on_exc=True,
        )

        # Pre-migration the cloud-storage status reads the archival manifest
        # directly, so it is a reliable source for the TS section's size here.
        # (Once migrating, the same admin endpoint routes through the composite
        # partition proxy and reports the CT side, so post-migration we observe
        # the TS manifest directly via get_partition_manifest instead.)
        status0 = self.admin.get_partition_cloud_storage_status(
            self.TOPIC_RETENTION, 0
        )
        ts_size = status0["cloud_log_size_bytes"]
        self.logger.info(
            f"Pre-migration TS cloud log: size={ts_size}, "
            f"archive={status0.get('archive_size_bytes')}"
        )

        # Migrate.
        self.rpk.alter_topic_config(
            self.TOPIC_RETENTION, TopicSpec.PROPERTY_STORAGE_MODE, storage_mode
        )

        # Snapshot the TS manifest right after migration, before any CT data:
        # the TS section is under the byte budget on its own, so it must not be
        # trimmed yet.
        manifest_post_migrate = self.admin.get_partition_manifest(
            self.TOPIC_RETENTION, 0
        )
        segments_post_migrate = len(manifest_post_migrate.get("segments", {}))
        self.logger.info(
            f"Post-migration TS manifest: {segments_post_migrate} segments, "
            f"start_offset={manifest_post_migrate.get('start_offset', 0)}"
        )

        # Whole-partition byte budget set just above the TS section's own size:
        # TS alone stays under budget, so any trimming must be driven by the CT
        # bytes counting toward the budget.
        MARGIN = 96 * 1024
        retention_bytes = ts_size + MARGIN
        self.rpk.alter_topic_config(
            self.TOPIC_RETENTION, "retention.bytes", str(retention_bytes)
        )

        # Phase 2: CT data well in excess of MARGIN, pushing TS+CT over budget.
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.TOPIC_RETENTION,
            msg_size=self.MSG_SIZE,
            msg_count=6000,
        )

        # The archiver must trim the (spilled) TS section to honor the
        # whole-partition budget -- the manifest's start offset advances and its
        # segment count drops -- even though the TS section is under the budget
        # on its own. Observed directly on the archival manifest.
        def ts_trimmed() -> bool:
            manifest = self.admin.get_partition_manifest(
                self.TOPIC_RETENTION, 0
            )
            start = manifest.get("start_offset", 0)
            segments = len(manifest.get("segments", {}))
            self.logger.info(
                f"trim check: manifest start_offset={start}, "
                f"segments={segments} (post-migration {segments_post_migrate}, "
                f"retention.bytes={retention_bytes}, ts_size={ts_size})"
            )
            return start > 0 or segments < segments_post_migrate

        wait_until(
            ts_trimmed,
            timeout_sec=120,
            backoff_sec=5,
            err_msg=(
                "TS section was not trimmed after CT data pushed the partition "
                "over retention.bytes; the archiver is not counting cloud-topics "
                f"bytes toward the size budget (storage_mode={storage_mode})."
            ),
            retry_on_exc=True,
        )

        # Part (b): completion under time-based retention. Shorten retention.ms
        # so the remaining TS ages out; the manifest drains to empty, which
        # triggers complete_migration (the partition becomes a native cloud
        # topic).
        self.rpk.alter_topic_config(
            self.TOPIC_RETENTION, "retention.ms", "5000"
        )

        def ts_drained() -> bool:
            manifest = self.admin.get_partition_manifest(
                self.TOPIC_RETENTION, 0
            )
            segments = len(manifest.get("segments", {}))
            self.logger.info(f"drain check: {segments} TS segments remain")
            return segments == 0

        wait_until(
            ts_drained,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="TS section did not fully drain under time retention",
            retry_on_exc=True,
        )
