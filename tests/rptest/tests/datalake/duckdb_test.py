# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix

from rptest.context.cloud_storage import CloudStorageType
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings, get_cloud_provider
from rptest.tests.datalake.catalog_service_factory import filesystem_catalog_type
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.redpanda_test import RedpandaTest


def duckdb_supported_storage_types():
    """
    Run only in docker environment with S3 storage type.
    TODO: extend support.
    """
    if get_cloud_provider() == "docker":
        return [CloudStorageType.S3]
    else:
        return []


class DuckDBTest(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(
            test_context,
            num_brokers=1,
            si_settings=SISettings(test_context),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
            },
            *args,
            **kwargs,
        )
        self.test_context = test_context
        self.topic_name = "test"

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=duckdb_supported_storage_types())
    def test_e2e_basic(self, cloud_storage_type):
        count = 100
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.DUCKDB_PY],
            catalog_type=filesystem_catalog_type(),
        ) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=1)
            dl.produce_to_topic(self.topic_name, 1024, count)

            dl.wait_for_translation(
                self.topic_name, msg_count=count, timeout=120, progress_sec=30
            )

            # TODO add test with WHERE clause which currently is broken.
            #   Even `WHERE value IS NOT NULL` fails with:
            #   `duckdb.duckdb.Error: unordered_map::at`
            #   - https://github.com/duckdb/duckdb-iceberg/issues/328
            #   - https://redpandadata.atlassian.net/browse/CORE-11982?focusedCommentId=76394
            with dl.query_engine(QueryEngineType.DUCKDB_PY).run_query(f"""
                    SELECT sum(redpanda.offset % 2)
                    FROM redpanda.{self.topic_name}
                    """) as cursor:
                odd_offsets_result = cursor.fetchone()
                assert odd_offsets_result is not None and odd_offsets_result[0] == 50, (
                    f"Expected 50 odd offsets, got {odd_offsets_result}"
                )
