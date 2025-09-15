# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import time

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.services.spark_service import SparkService
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import firewall_blocked
from rptest.utils.rpcn_utils import counter_stream_config


class DatalakeBlockedCatalogTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeBlockedCatalogTest, self).__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx, fast_uploads=True),
            extra_rp_conf={
                # Write to Iceberg quickly.
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": 1000,
                "iceberg_target_lag_ms": 1000,
                # Trim local storage aggressively.
                "retention_local_target_capacity_bytes": 1024,
                "retention_local_trim_interval": 1000,
                "log_segment_ms": 1000,
                "log_segment_ms_min": 1000,
                # Aggressively GC in tiered storage.
                "cloud_storage_housekeeping_interval_ms": 1000,
            },
            *args,
            **kwargs,
        )
        self.rpcn = RedpandaConnectService(self.test_context, self.redpanda)
        self.topic_name: str = "tapioca"
        self.stream_name: str = "tapioca_stream"

    def setUp(self):
        # NOTE: defer cluster startup to DatalakeServices.
        self.rpcn.start()

    def make_rpcn_config(self):
        return counter_stream_config(
            self.redpanda,
            self.topic_name,
            "",  # subject
            cnt=0,  # indefinite count
            interval_ms=10,
        )

    def local_storage_trimmed(self) -> bool:
        """
        Returns true if the local log has been prefix truncated.
        """
        admin = Admin(self.redpanda)
        status = admin.get_partition_cloud_storage_status(self.topic_name, 0)
        return status["local_log_start_offset"] > 0

    def cloud_log_trimmed(self) -> bool:
        """
        Returns true if the cloud log has been prefix truncated.
        """
        _, lso = self.get_hwm_lso()
        return lso > 0

    def has_spilled_over(self) -> bool:
        """
        Returns true if the cloud log spilled over.
        """
        admin = Admin(self.redpanda)
        status = admin.get_partition_cloud_storage_status(self.topic_name, 0)
        return status["archive_size_bytes"] > 0

    def iceberg_writes_quiesced(self, spark: SparkService) -> bool:
        """
        Returns true if the Iceberg table is no longer being written to.
        """
        initial_o = spark.max_translated_offset("redpanda", self.topic_name, 0)
        time.sleep(5)
        final_o = spark.max_translated_offset("redpanda", self.topic_name, 0)
        return initial_o == final_o

    def log_start_quiesced(self) -> bool:
        """
        Returns true if the log start offset is not changing.
        """
        _, initial_log_start = self.get_hwm_lso()
        time.sleep(5)
        _, final_log_start = self.get_hwm_lso()
        return initial_log_start == final_log_start

    def get_hwm_lso(self) -> tuple[int, int]:
        """
        Returns the partitions high watermark and start offset.
        """
        rpk = RpkTool(self.redpanda)
        hwm_lsos = [
            (p.high_watermark, p.start_offset)
            for p in rpk.describe_topic(self.topic_name)
        ]
        assert len(hwm_lsos) == 1, f"Expected one partition: {hwm_lsos}"
        return hwm_lsos[0]

    def check_offsets(self, spark: SparkService, hwm):
        """
        Asserts that the Iceberg table has all of the offsets expected up to
        and including HWM - 1.
        NOTE: this can be used instead of DatalakeVerifier (which expects the
        topic to match the table) when low retention is set for a topic.
        """
        TARGET_STEP_SIZE = 500
        num_steps = hwm // TARGET_STEP_SIZE
        ranges = [
            (i * hwm // num_steps, (i + 1) * hwm // num_steps) for i in range(num_steps)
        ]
        for start, end_excl in ranges:
            actual = [
                r[0]
                for r in spark.run_query_fetch_all(
                    f"select redpanda.offset from redpanda.{self.topic_name} where redpanda.offset >= {start} and redpanda.offset < {end_excl} order by redpanda.offset"
                )
            ]
            expected = list(range(start, end_excl))
            assert actual == expected, f"Expected: {expected}, got: {actual}"

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_trim_local_before_first_translation(self, cloud_storage_type):
        """
        Test that blocks the catalog before the first translation and ensures
        that once unblocked, translation proceeds from the start of the log
        that resides in tiered storage.
        """
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=CatalogType.REST_JDBC,
        ) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name)
            spark = dl.spark()

            with firewall_blocked(
                self.redpanda.nodes, dl.catalog_service.iceberg_rest_port
            ):
                self.rpcn.start_stream(self.stream_name, self.make_rpcn_config())

                # Give some time for Redpanda to attempt some translation, but
                # ultimately not create the table because of the firewall.
                time.sleep(5)
                assert not dl.table_exists(self.topic_name)

                # Because we've set such an aggressive local target capacity,
                # we should trim local data, despite not translating anything.
                wait_until(self.local_storage_trimmed, timeout_sec=30, backoff_sec=1)
                assert not dl.table_exists(self.topic_name)

            dl.wait_for_translation_until_offset(self.topic_name, 100)
            self.rpcn.stop_stream(self.stream_name, should_finish=None)

            # Check that we have data, and that our log starts from 0.
            hwm, lso = self.get_hwm_lso()
            assert lso == 0, f"Expected {lso} = 0"
            assert hwm > 0, f"Expected {hwm} > 0"
            dl.wait_for_translation_until_offset(self.topic_name, hwm - 1)
            DatalakeVerifier.oneshot(self.redpanda, self.topic_name, spark)

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_trim_local_after_first_translation(self, cloud_storage_type):
        """
        Test that blocks the catalog after the first translation and ensures
        that once unblocked, translation proceeds from the start of the log
        that resides in tiered storage.
        """
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=CatalogType.REST_JDBC,
        ) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name)
            self.rpcn.start_stream(self.stream_name, self.make_rpcn_config())
            dl.wait_for_translation_until_offset(self.topic_name, 100)
            wait_until(self.local_storage_trimmed, timeout_sec=30, backoff_sec=1)

            spark = dl.spark()
            with firewall_blocked(
                self.redpanda.nodes, dl.catalog_service.iceberg_rest_port
            ):
                wait_until(
                    lambda: self.iceberg_writes_quiesced(spark),
                    timeout_sec=30,
                    backoff_sec=1,
                )

            self.rpcn.stop_stream(self.stream_name, should_finish=None)

            hwm, lso = self.get_hwm_lso()
            assert lso == 0, f"Expected {lso} = 0"
            assert hwm > 0, f"Expected {hwm} > 0"
            dl.wait_for_translation_until_offset(self.topic_name, hwm - 1)

            # Finally, verify correctness.
            DatalakeVerifier.oneshot(self.redpanda, self.topic_name, spark)

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types(), with_spillover=[True, False])
    def test_block_cloud_retention_before_translation(
        self, cloud_storage_type, with_spillover
    ):
        """
        Test that blocks the catalog before the first translation and ensures
        that cloud retention is blocked while local space management can make
        progress.
        """
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=CatalogType.REST_JDBC,
        ) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name, config=dict({"retention.ms": 1})
            )
            spark = dl.spark()
            rpk = RpkTool(self.redpanda)
            if with_spillover:
                rpk.cluster_config_set("cloud_storage_spillover_manifest_size", "null")
                rpk.cluster_config_set(
                    "cloud_storage_spillover_manifest_max_segments", "1"
                )

            with firewall_blocked(
                self.redpanda.nodes, dl.catalog_service.iceberg_rest_port
            ):
                self.rpcn.start_stream(self.stream_name, self.make_rpcn_config())
                time.sleep(5)
                assert not dl.table_exists(self.topic_name)
                wait_until(self.local_storage_trimmed, timeout_sec=30, backoff_sec=1)
                if with_spillover:
                    wait_until(self.has_spilled_over, timeout_sec=30, backoff_sec=1)
                assert not dl.table_exists(self.topic_name)

                # Despite our retention settings being so low, we shouldn't
                # trim anything because we haven't translated anything.
                _, lso = self.get_hwm_lso()
                assert lso == 0, f"Expected {lso} = 0"

            dl.wait_for_translation_until_offset(self.topic_name, 100)
            self.rpcn.stop_stream(self.stream_name, should_finish=None)

            # Now that we've translated, tiered storage is free to remove
            # historical data.
            wait_until(self.cloud_log_trimmed, timeout_sec=10, backoff_sec=1)
            hwm, _ = self.get_hwm_lso()
            dl.wait_for_translation_until_offset(self.topic_name, hwm - 1)
            self.check_offsets(spark, hwm)

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_block_cloud_retention_after_translation(self, cloud_storage_type):
        """
        Test that blocks the catalog after the first translation and ensures
        that cloud retention is blocked while local space management can make
        progress.
        """
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=CatalogType.REST_JDBC,
        ) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name, config=dict({"retention.ms": 1})
            )
            self.rpcn.start_stream(self.stream_name, self.make_rpcn_config())
            dl.wait_for_translation_until_offset(self.topic_name, 100)
            wait_until(self.local_storage_trimmed, timeout_sec=30, backoff_sec=1)

            spark = dl.spark()
            with firewall_blocked(
                self.redpanda.nodes, dl.catalog_service.iceberg_rest_port
            ):
                wait_until(
                    lambda: self.iceberg_writes_quiesced(spark),
                    timeout_sec=30,
                    backoff_sec=1,
                )
                wait_until(self.log_start_quiesced, timeout_sec=30, backoff_sec=1)

            self.rpcn.stop_stream(self.stream_name, should_finish=None)

            hwm, _ = self.get_hwm_lso()
            dl.wait_for_translation_until_offset(self.topic_name, hwm - 1)
            self.check_offsets(spark, hwm)

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_disable_reenable_after_blocking(self, cloud_storage_type):
        """
        Test that blocks the catalog, disables Iceberg, allows for retention to
        happen, reenables Iceberg, and then ensures that we can make progress.

        This is a regression test: in older versions of Redpanda we would be
        unable to translate after reenabling Iceberg.
        """
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
            catalog_type=CatalogType.REST_JDBC,
        ) as dl:

            def hwm_gt(target_o):
                hwm, _ = self.get_hwm_lso()
                return hwm > target_o

            rpk = RpkTool(self.redpanda)
            with firewall_blocked(
                self.redpanda.nodes, dl.catalog_service.iceberg_rest_port
            ):
                dl.create_iceberg_enabled_topic(
                    self.topic_name, config=dict({"retention.ms": 1})
                )
                self.rpcn.start_stream(self.stream_name, self.make_rpcn_config())

                # Wait for some amount of data to be produced.
                wait_until(lambda: hwm_gt(100), timeout_sec=10, backoff_sec=1)

                # Sanity check that the data is pinned and can't be trimmed.
                _, lso = self.get_hwm_lso()
                assert lso == 0, f"Expected {lso} = 0"

                # Now disable Iceberg and wait for data to be trimmed.
                rpk.cluster_config_set("iceberg_enabled", "false")
                wait_until(self.cloud_log_trimmed, timeout_sec=30, backoff_sec=1)

            # Now reenable Iceberg and check that we commit data.
            rpk.cluster_config_set("iceberg_enabled", "true")
            self.redpanda.restart_nodes(self.redpanda.nodes)

            hwm, _ = self.get_hwm_lso()
            dl.wait_for_translation_until_offset(self.topic_name, hwm - 1)
            self.rpcn.stop_stream(self.stream_name, should_finish=None)
