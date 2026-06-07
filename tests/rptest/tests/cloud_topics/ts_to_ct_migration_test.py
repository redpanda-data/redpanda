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

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest


class TsToCtMigrationTest(RedpandaTest):
    NUM_PHASE1 = 500
    NUM_PHASE2 = 1000
    MSG_SIZE = 128
    TOPIC = "ts-ct-migration-test"

    # Transactional parameters. msgs_per_transaction is intentionally large so
    # that when the migration trigger fires mid-stream at least one in-flight
    # transaction straddles the point where the mirror is running.
    TOPIC_TX = "ts-ct-migration-tx-test"
    NUM_TX_MSGS_PHASE1 = 10000
    NUM_TX_MSGS_PHASE2 = 1000
    TX_MSGS_PER_TXN = 200
    TX_ABORT_RATE = 0.3

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
    def test_ts_to_ct_migration_recovery(self):
        """Cluster metadata recovery of a mid-migration partition: it is
        classified by the metastore migration_phase and recovered as a
        tiered-storage partition (re-ingesting the TS manifest), after which the
        live migration resumes and cuts over. A consume from offset 0 after
        recovery must return all records in order.

        NOTE: depends on E1 (migration-aware cluster recovery); included as the
        validation vehicle for that path.
        """
        # Full body to be completed alongside E1 (the recovery backend routing).
        # The structure: produce TS data, trigger migration, take a cluster
        # metadata snapshot mid-migration, wipe + recover the cluster, then
        # assert the recovered partition serves all records (TS while still
        # migrating, cloud-topic once it re-converges and cuts over).
        pass

    @cluster(num_nodes=2)
    def test_ts_to_ct_migration_read_replica(self):
        """A read replica of a migrating source serves it as tiered storage,
        and switches to the cloud-topic read path when the source's metastore
        migration_phase flips to complete at cutover. read_committed semantics
        hold across the switch.

        NOTE: depends on E2 (phase-aware read replica); included as the
        validation vehicle for that path.
        """
        # Full body to be completed alongside E2 (read-replica phase selection).
        pass
