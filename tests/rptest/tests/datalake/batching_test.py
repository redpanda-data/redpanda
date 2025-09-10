# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import SISettings
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode


class DatalakeBatchingTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeBatchingTest, self).__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            extra_rp_conf={
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": 100,
            },
            *args,
            **kwargs,
        )
        self.test_ctx = test_ctx
        self.topic_name = "test"

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    @cluster(num_nodes=4)
    @skip_debug_mode
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK],
        catalog_type=[CatalogType.REST_JDBC],
        expect_large_files=[True, False],
    )
    def test_batching(
        self, cloud_storage_type, query_engine, catalog_type, expect_large_files
    ):
        """Test ensures that the broker produces sufficiently large parquet files on topics with large target lag."""

        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            include_query_engines=[query_engine],
            catalog_type=catalog_type,
        ) as dl:
            lag = 60 * 60 * 1000 if expect_large_files else 60 * 1000
            rate = 30 * 2**20 if expect_large_files else 2**20
            dl.create_iceberg_enabled_topic(
                self.topic_name,
                partitions=1,
                target_lag_ms=lag,
                config={"redpanda.iceberg.partition.spec": "()"},
            )

            producer = KgoVerifierProducer(
                self.test_ctx,
                self.redpanda,
                self.topic_name,
                msg_size=4096,
                msg_count=2**30,
                rate_limit_bps=rate,
            )
            producer.start()

            table_name = f"redpanda.{self.topic_name}"
            spark = dl.spark()

            min_file_count = 2
            batch_file_size = 32 * 1024 * 1024

            def wait_for_large_files():
                try:
                    file_count = spark.run_query_fetch_one(
                        f"select count(*) from {table_name}.files"
                    )[0]
                    file_size = spark.run_query_fetch_one(
                        f"select min(file_size_in_bytes) from {table_name}.files"
                    )[0]
                    self.redpanda.logger.debug(
                        f"count: {file_count}, size: {file_size}"
                    )
                    file_size_checks_out = (
                        file_size >= batch_file_size
                        if expect_large_files
                        else file_size < batch_file_size
                    )
                    return file_count >= min_file_count and file_size_checks_out
                except Exception:
                    return False

            wait_until(
                wait_for_large_files,
                timeout_sec=180,
                backoff_sec=10,
                err_msg="Timed out waiting for batched parquet files to be created",
            )
