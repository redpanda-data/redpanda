# Copyright 2024 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from ducktape.mark import matrix

from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings
from rptest.tests.datalake.catalog_service_factory import supported_catalog_types
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest


class Datalake3rdPartyMaintenanceTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_ctx),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
            },
            *args,
            **kwargs,
        )

        self.test_ctx = test_ctx
        self.topic_name = "test"
        self.num_partitions = 10

        self.produced_messages = 0

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    @cluster(num_nodes=4)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        query_engine=[QueryEngineType.SPARK, QueryEngineType.TRINO],
        catalog_type=supported_catalog_types(),
    )
    def test_e2e_basic(self, cloud_storage_type, query_engine, catalog_type):
        """
        This test verifies that Redpanda can continue to work with Iceberg
        metadata written by third-party query engines. We use an optimize operation
        with a third-party query engine to trigger a rewrite of the data files
        and metadata.
        """
        with DatalakeServices(
            self.test_ctx,
            redpanda=self.redpanda,
            catalog_type=catalog_type,
            include_query_engines=[query_engine],
        ) as dl:
            dl.create_iceberg_enabled_topic(
                self.topic_name, partitions=self.num_partitions
            )
            # Write some data to the topic.
            self._translate_sample_data(dl)

            # Run maintenance to rewrite the data.
            num_parquet_files_before = dl.query_engine(
                query_engine
            ).count_parquet_files("redpanda", self.topic_name)

            # Want at least 2 files to be able to assert that optimization did something.
            assert num_parquet_files_before >= 2, (
                f"Expecting at least 2 files, got {num_parquet_files_before}"
            )

            dl.query_engine(query_engine).optimize_parquet_files(
                "redpanda", self.topic_name
            )

            # Ensure that some data and metadata mutation actually happened.
            num_parquet_files_after = dl.query_engine(query_engine).count_parquet_files(
                "redpanda", self.topic_name
            )
            assert num_parquet_files_after < num_parquet_files_before, (
                f"Expecting fewer files after optimize, got {num_parquet_files_after}"
            )

            # Verify consistency post rewrite.
            DatalakeVerifier.oneshot(
                self.redpanda, self.topic_name, dl.query_engine(query_engine)
            )

            # Produce additional messages to the topic to make sure we correctly
            # interoperate with the metadata written by Trino.
            self._translate_sample_data(dl)

            # Verify consistency with the additional messages.
            DatalakeVerifier.oneshot(
                self.redpanda, self.topic_name, dl.query_engine(query_engine)
            )

    def _translate_sample_data(self, dl):
        NUM_MSG_PER_SAMPLE = 100
        self.produced_messages += NUM_MSG_PER_SAMPLE

        dl.produce_to_topic(self.topic_name, 1024, NUM_MSG_PER_SAMPLE)
        # Wait for all messages (including the ones we just wrote) to be translated.
        dl.wait_for_translation(self.topic_name, msg_count=self.produced_messages)
