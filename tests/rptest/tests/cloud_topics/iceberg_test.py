# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from typing import Any
from ducktape.tests.test import TestContext
from rptest.clients.admin.v2 import metastore_pb, ntp_pb
from rptest.clients.types import TopicSpec
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import MetricsEndpoint
from rptest.context.cloud_storage import CloudStorageType
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineBase, QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.cloud_topics.e2e_test import EndToEndCloudTopicsBase


class EndToEndCloudTopicsIcebergTestBase(EndToEndCloudTopicsBase):
    """
    Base class for cloud topics + Iceberg integration tests.
    Provides common setup, configuration, and helper methods.
    """

    num_brokers = 1
    topics = ()  # Topics created via DatalakeServices

    # Subclasses should override these
    topic_name: str = "iceberg_test"
    msg_size: int = 1024
    msg_count: int = 1000

    def __init__(
        self, test_ctx: TestContext, extra_rp_conf: dict[str, Any] | None = None
    ) -> None:
        iceberg_conf = {"iceberg_enabled": True}
        if extra_rp_conf:
            iceberg_conf.update(extra_rp_conf)

        super().__init__(test_ctx, extra_rp_conf=iceberg_conf)
        self.test_ctx = test_ctx

    def setUp(self):
        # DatalakeServices will start redpanda, not us
        pass

    def get_hwm(self) -> int:
        """Get the high watermark for the topic."""
        for part in self.rpk.describe_topic(self.topic_name):
            return part.high_watermark
        raise RuntimeError(f"Failed to get_hwm() for topic {self.topic_name}")

    def get_cloud_start_offset(self) -> int:
        """Get the start offset from the cloud topic's L1 metastore."""
        metastore = self.admin.metastore()
        req = metastore_pb.GetOffsetsRequest(
            partition=ntp_pb.TopicPartition(topic=self.topic_name, partition=0)
        )
        result = metastore.get_offsets(req=req)
        return result.offsets.start_offset

    def check_all_offsets_in_iceberg(
        self, spark: QueryEngineBase, expected_hwm: int
    ) -> None:
        """
        Verify that every offset from 0 to expected_hwm-1 exists in the
        Iceberg table.
        """
        assert self.redpanda
        result = spark.run_query_fetch_one(
            f"SELECT COUNT(DISTINCT redpanda.offset) FROM redpanda.{self.topic_name}"
        )
        distinct_count = result[0]
        self.redpanda.logger.info(
            f"Iceberg table has {distinct_count} distinct offsets, expected {expected_hwm}"
        )
        assert distinct_count == expected_hwm, (
            f"Expected {expected_hwm} distinct offsets in Iceberg, got {distinct_count}"
        )

        result = spark.run_query_fetch_one(
            f"SELECT MIN(redpanda.offset), MAX(redpanda.offset) FROM redpanda.{self.topic_name}"
        )
        min_offset, max_offset = result[0], result[1]
        assert min_offset == 0, f"Expected min offset 0, got {min_offset}"
        assert max_offset == expected_hwm - 1, (
            f"Expected max offset {expected_hwm - 1}, got {max_offset}"
        )


class EndToEndCloudTopicsIcebergCompactionTest(EndToEndCloudTopicsIcebergTestBase):
    """
    Tests that with cloud topics and Iceberg both enabled on a compacted topic,
    every offset produced ends up in the Iceberg table even though the Kafka
    log may be compacted.
    """

    topic_name = "compaction_iceberg_test"
    msg_size = 1024
    msg_count = 5000
    key_set_cardinality = 50

    def __init__(self, test_ctx: TestContext):
        super().__init__(
            test_ctx,
            extra_rp_conf={
                "iceberg_catalog_commit_interval_ms": 5000,
                "iceberg_target_lag_ms": 5000,
                "cloud_topics_compaction_interval_ms": 5000,
                "min_cleanable_dirty_ratio": 0.0,
            },
        )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_compaction_preserves_all_offsets_in_iceberg(
        self, cloud_storage_type: CloudStorageType, storage_mode: str
    ):
        """
        Produce messages with a small key set (causing many duplicates),
        trigger compaction, and verify that all offsets still exist in
        the Iceberg table.
        """
        assert self.redpanda
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=CatalogType.REST_JDBC,
        ) as dl:
            if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
                self.redpanda.set_feature_active(
                    "tiered_cloud_topics", True, timeout_sec=30
                )
            dl.create_iceberg_enabled_topic(
                self.topic_name,
                iceberg_mode="key_value",
                config={
                    TopicSpec.PROPERTY_STORAGE_MODE: storage_mode,
                    "cleanup.policy": TopicSpec.CLEANUP_COMPACT,
                },
            )

            spark = dl.spark()

            producer = KgoVerifierProducer(
                self.test_ctx,
                self.redpanda,
                self.topic_name,
                msg_size=self.msg_size,
                msg_count=self.msg_count,
                key_set_cardinality=self.key_set_cardinality,
            )
            try:
                producer.start()
                producer.wait()
            finally:
                producer.stop()
                producer.free()

            hwm = self.get_hwm()
            assert hwm > 0, f"Expected HWM > 0, got {hwm}"
            self.redpanda.logger.info(f"High watermark after produce: {hwm}")

            dl.wait_for_translation_until_offset(
                self.topic_name, hwm - 1, timeout=120, backoff_sec=5
            )

            def compaction_occurred():
                assert self.redpanda
                return (
                    self.redpanda.metric_sum(
                        metric_name="vectorized_cloud_topics_compaction_scheduler_log_compactions_total",
                        metrics_endpoint=MetricsEndpoint.METRICS,
                        expect_metric=True,
                    )
                    > 0
                )

            wait_until(
                compaction_occurred,
                timeout_sec=120,
                backoff_sec=5,
                err_msg="Compaction did not occur",
            )

            self.check_all_offsets_in_iceberg(spark, hwm)


class EndToEndCloudTopicsIcebergDeletionTest(EndToEndCloudTopicsIcebergTestBase):
    """
    Tests that with cloud topics and Iceberg both enabled, we cannot
    prefix truncate (delete records) beyond data that has been translated
    to Iceberg. The lowest_pinned_data_offset should prevent premature
    deletion.
    """

    topic_name = "deletion_pinning_test"
    msg_size = 1024
    msg_count = 1000

    def __init__(self, test_ctx: TestContext) -> None:
        super().__init__(
            test_ctx,
            extra_rp_conf={
                # Frequent housekeeping
                "cloud_storage_housekeeping_interval_ms": 500,
                # Moderate translation speed
                "iceberg_catalog_commit_interval_ms": 5000,
                "iceberg_target_lag_ms": 5000,
            },
        )

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_deletion_blocked_until_translated(
        self, cloud_storage_type: CloudStorageType, storage_mode: str
    ):
        """
        Test that even when running with very low retention policies, we
        cannot advance the start offset beyond the last translated Iceberg
        offset.
        """
        assert self.redpanda
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=CatalogType.REST_JDBC,
        ) as dl:
            if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
                self.redpanda.set_feature_active(
                    "tiered_cloud_topics", True, timeout_sec=30
                )
            dl.create_iceberg_enabled_topic(
                self.topic_name,
                iceberg_mode="key_value",
                config={
                    TopicSpec.PROPERTY_STORAGE_MODE: storage_mode,
                    "retention.ms": 500,
                    "retention.bytes": 1024,
                },
            )

            spark = dl.spark()
            producer = KgoVerifierProducer(
                self.test_ctx,
                self.redpanda,
                self.topic_name,
                msg_size=self.msg_size,
                msg_count=self.msg_count,
            )
            try:
                producer.start()
                producer.wait()
            finally:
                producer.stop()
                producer.free()

            hwm = self.get_hwm()
            self.redpanda.logger.info(f"High watermark after produce: {hwm}")
            assert hwm > 0, f"Expected HWM > 0, got {hwm}"

            # Wait for full translation
            dl.wait_for_translation_until_offset(
                self.topic_name, hwm - 1, timeout=120, backoff_sec=5
            )

            # Verify all offsets made it to Iceberg
            self.check_all_offsets_in_iceberg(spark, hwm)

            # After translation completes, cloud topic should eventually be truncated to the HWM
            wait_until(
                lambda: self.get_cloud_start_offset() >= hwm,
                timeout_sec=60,
                backoff_sec=1,
                err_msg="Cloud topic data was not truncated after translation completed",
            )
