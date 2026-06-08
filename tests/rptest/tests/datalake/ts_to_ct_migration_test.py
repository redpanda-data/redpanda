# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    PandaproxyConfig,
    SISettings,
    SchemaRegistryConfig,
)
from rptest.services.spark_service import QueryEngineType
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest


class DatalakeTsToCtMigrationTest(RedpandaTest):
    """Iceberg translation must stay continuous across a cutover-last TS->CT
    migration.

    During migration the partition is served as tiered storage and translation
    reads it from there; at cutover it becomes a native cloud topic (imported
    extents + native residual in L1) and translation reads it from there.
    Translation resumes from its persisted highest_translated_offset (carried in
    the partition raft log across the transition), so the Iceberg table must
    cover the full offset range with no gap or duplicate across the boundary.
    DatalakeVerifier consumes the Kafka topic and cross-checks every offset
    against the Iceberg table, catching any gap/duplicate introduced at cutover.
    """

    MSG_SIZE = 1024
    # Sized to run on a single dev box: the Iceberg verifier cross-checks every
    # offset through Trino, so the row count drives the bulk of the load. A few
    # hundred rows per phase is enough to span several TS segments before
    # migration and a residual after cutover while keeping Trino comfortable.
    PHASE1_ROWS = 500
    PHASE2_ROWS = 500

    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx, fast_uploads=True),
            extra_rp_conf={
                "enable_topic_mode_migration": True,
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 1000,
                # Small per-topic segments so the TS section uploads several
                # segments before migration.
                "log_segment_size_min": 1,
                "log_segment_ms_min": 1000,
                "cloud_storage_housekeeping_interval_ms": 1000,
                # Bound the uploaded (remote) segment size so the TS section
                # yields several segments even for the small dev-box data
                # volume -- otherwise the archiver coalesces it into a single
                # remote segment and the >=2-segments precondition never holds.
                "cloud_storage_segment_size_target": 64 * 1024,
                "cloud_storage_segment_size_min": 32 * 1024,
                # Reconcile the post-cutover residual into L1 promptly.
                "cloud_topics_long_term_flush_interval": 2000,
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            *args,
            **kwargs,
        )
        self.test_ctx = test_ctx
        self.topic_name = "ts-ct-iceberg"
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    def setUp(self):
        pass

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
        ],
    )
    def test_translation_continuous_across_migration(
        self, cloud_storage_type, storage_mode
    ):
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.TRINO],
        ) as dl:
            # DatalakeServices starts the cluster (setUp is a no-op), so the
            # features must be toggled here, once the cluster is up.
            self.redpanda.set_feature_active(
                "topic_mode_migration", True, timeout_sec=30
            )
            if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2:
                self.redpanda.set_feature_active(
                    "tiered_cloud_topics", True, timeout_sec=30
                )

            # Iceberg-enabled tiered-storage topic. A moderate target lag lets
            # translation run alongside the produce so the migration lands
            # mid-translation.
            dl.create_iceberg_enabled_topic(
                self.topic_name,
                iceberg_mode="key_value",
                target_lag_ms=10000,
                config={
                    TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                    "segment.bytes": str(32 * 1024),
                    "segment.ms": "1000",
                    # finite (so migration is permitted) but large (so time
                    # retention does not interfere during the test).
                    "retention.ms": str(24 * 3600 * 1000),
                    "retention.local.target.bytes": str(64 * 1024),
                },
            )

            # Phase 1: pre-migration data archived to tiered storage. Rate-limit
            # the produce so the small dev-box volume still spans several
            # segment.ms (1s) windows and rolls into multiple segments -- a fast
            # bulk produce of this little data would land in a single segment and
            # the >=2-segments precondition below would never hold.
            dl.produce_to_topic(
                self.topic_name,
                msg_size=self.MSG_SIZE,
                msg_count=self.PHASE1_ROWS,
                rate_limit_bps=64 * 1024,
            )

            # Wait for real TS segments so the migration splits the partition at
            # a non-trivial boundary.
            def has_ts_segments() -> bool:
                m = self.admin.get_partition_manifest(self.topic_name, 0)
                return len(m.get("segments", {})) >= 2

            wait_until(
                has_ts_segments,
                timeout_sec=120,
                backoff_sec=5,
                err_msg="Fewer than 2 TS segments uploaded within 120s",
                retry_on_exc=True,
            )

            # Migrate. The partition stays TS-served while the mirror imports,
            # then cuts over. The TSv2 (tiered_cloud) destination is spelled
            # storage.mode=tiered with the cluster default impl set to
            # tiered_v2, since redpanda.storage.mode.impl is read-only after
            # creation and tiered_cloud is not a settable storage.mode value.
            if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2:
                self.redpanda.set_cluster_config(
                    {"default_redpanda_storage_mode_tiered_impl": "tiered_v2"}
                )
                target_mode = TopicSpec.STORAGE_MODE_TIERED
            else:
                target_mode = storage_mode
            self.rpk.alter_topic_config(
                self.topic_name,
                TopicSpec.PROPERTY_STORAGE_MODE,
                target_mode,
            )

            # Phase 2: more data, written while migrating (TS path) and, once
            # cut over, via the cloud-topics path.
            dl.produce_to_topic(
                self.topic_name,
                msg_size=self.MSG_SIZE,
                msg_count=self.PHASE2_ROWS,
            )

            # Wait for the partition to cut over (archival manifest empties).
            wait_until(
                lambda: len(
                    self.admin.get_partition_manifest(self.topic_name, 0).get(
                        "segments", {}
                    )
                )
                == 0,
                timeout_sec=240,
                backoff_sec=5,
                err_msg="topic did not cut over within 240s",
                retry_on_exc=True,
            )

            # Trino may still be initializing after DatalakeServices started it
            # (it rejects queries with SERVER_STARTING_UP until ready); wait
            # before the first query below so the translation/verification
            # read-backs do not race its startup.
            dl.trino().wait_for_ready()

            # Translation must catch up to the partition high watermark across
            # the whole range (TS section + CT section), then the Iceberg table
            # must match the topic offset-for-offset.
            hwm = next(self.rpk.describe_topic(self.topic_name)).high_watermark
            dl.wait_for_translation_until_offset(
                self.topic_name, hwm - 1, timeout=120, backoff_sec=5
            )

            verifier = DatalakeVerifier(self.redpanda, self.topic_name, dl.trino())
            verifier.start()
            verifier.wait()
