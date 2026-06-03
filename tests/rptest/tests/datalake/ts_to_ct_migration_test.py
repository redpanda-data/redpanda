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
    """
    Iceberg translation must stay continuous across a TS->CT migration. The
    partition mid-migration is a composite (pre-migration data served from the
    tiered-storage path below the boundary, post-migration data from cloud
    topics above it); datalake translation reads through that same composite and
    resumes from its persisted highest_translated_offset, so it should cover the
    full offset range with no gap or duplicate across the boundary.

    DatalakeVerifier consumes the Kafka topic and cross-checks every offset
    against the Iceberg table, so any gap/duplicate introduced at the boundary
    is caught.
    """
    MSG_SIZE = 1024
    PHASE1_ROWS = 2000
    PHASE2_ROWS = 2000

    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx, fast_uploads=True),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 1000,
                # Allow the small per-topic segment sizes below so the TS
                # section uploads several segments before migration.
                "log_segment_size_min": 1,
                "log_segment_ms_min": 1000,
                "cloud_storage_housekeeping_interval_ms": 1000,
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
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
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
            # feature must be toggled here, once the cluster is up.
            if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
                self.redpanda.set_feature_active(
                    "tiered_cloud_topics", True, timeout_sec=30
                )

            # Create the topic as a tiered-storage topic with Iceberg enabled.
            # A moderate target lag lets translation run alongside the produce
            # so that the migration lands mid-translation.
            dl.create_iceberg_enabled_topic(
                self.topic_name,
                iceberg_mode="key_value",
                target_lag_ms=10000,
                config={
                    TopicSpec.PROPERTY_STORAGE_MODE:
                    TopicSpec.STORAGE_MODE_TIERED,
                    "segment.bytes": str(32 * 1024),
                    "segment.ms": "1000",
                    # finite (so migration is permitted) but large (so time
                    # retention does not interfere during the test).
                    "retention.ms": str(24 * 3600 * 1000),
                    "retention.local.target.bytes": str(64 * 1024),
                },
            )

            # Phase 1: pre-migration data written as tiered storage.
            dl.produce_to_topic(
                self.topic_name,
                msg_size=self.MSG_SIZE,
                msg_count=self.PHASE1_ROWS,
            )

            # The migration seal's boundary is the TS manifest's last uploaded
            # offset; if nothing has been uploaded the transition is a no-op
            # (treated as a fresh cloud topic). Wait for real TS segments so the
            # migration actually splits the partition at a non-trivial boundary.
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

            # Migrate to cloud topics. Translation must continue across the
            # boundary without losing or duplicating any record.
            self.rpk.alter_topic_config(
                self.topic_name,
                TopicSpec.PROPERTY_STORAGE_MODE,
                storage_mode,
            )

            # Phase 2: post-migration data written via the cloud-topics path.
            dl.produce_to_topic(
                self.topic_name,
                msg_size=self.MSG_SIZE,
                msg_count=self.PHASE2_ROWS,
            )

            # Wait for translation to catch up to the partition high watermark,
            # then verify the Iceberg table matches the topic offset-for-offset
            # across the whole range (TS section + CT section).
            hwm = next(self.rpk.describe_topic(self.topic_name)).high_watermark
            dl.wait_for_translation_until_offset(
                self.topic_name, hwm - 1, timeout=120, backoff_sec=5
            )

            verifier = DatalakeVerifier(
                self.redpanda, self.topic_name, dl.trino()
            )
            verifier.start()
            verifier.wait()
