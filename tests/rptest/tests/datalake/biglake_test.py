# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from ducktape.mark._mark import Mark

from rptest.context.gcp import GCPContext
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    PandaproxyConfig,
    SISettings,
    SchemaRegistryConfig,
)
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until


class GCPOnlyTestMark(Mark):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def apply(self, seed_context, context_list):
        """
        Apply the mark to the test context list.
        This will skip the test if the GCP context is not available.
        """
        assert len(context_list) > 0, (
            "ignore annotation is not being applied to any test cases"
        )

        should_ignore_test = False
        if not GCPContext.available(seed_context):
            seed_context.logger.debug(
                f"Skipping {seed_context} test because GCP context is not available"
            )
            should_ignore_test = True

        for ctx in context_list:
            ctx.ignore = should_ignore_test

        return context_list


def gcp_only_test(func, /):
    """
    Decorator to mark a test as a Google Cloud Platform test.
    Such tests will only run if the GCP context is available. I.e. we run
    in a GCP environment.
    """

    Mark.mark(func, GCPOnlyTestMark())
    return func


class BiglakeTest(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(
            test_context,
            num_brokers=1,
            si_settings=SISettings(test_context),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000,
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            *args,
            **kwargs,
        )
        self.test_context = test_context
        self.topic_name = "test"

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    @gcp_only_test
    @cluster(num_nodes=2)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_e2e_basic(self, cloud_storage_type):
        count = 100
        with DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[],
            catalog_type=CatalogType.BIGLAKE,
        ) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)
            dl.produce_to_topic(self.topic_name, 1024, count)

            wait_until(
                lambda: dl.catalog_client().table_exists(f"redpanda.{self.topic_name}"),
                timeout_sec=30,
                backoff_sec=1,
            )

            def count_rows():
                t = dl.catalog_client().load_table(f"redpanda.{self.topic_name}")
                df = t.scan().to_duckdb("data")
                r = df.sql("SELECT count(*) FROM data").fetchone()
                self.logger.info(f"Row count for {self.topic_name}: {r[0]}")
                return r[0]

            wait_until(lambda: count_rows() == count, timeout_sec=60, backoff_sec=1)
