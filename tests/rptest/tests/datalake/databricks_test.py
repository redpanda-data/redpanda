# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix

from rptest.context.databricks import DatabricksContext as DatabricksContext
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    PandaproxyConfig,
    SchemaRegistryConfig,
    SISettings,
)
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_cloud_provider


class DatabricksTest(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(
            test_context,
            num_brokers=1,
            si_settings=SISettings(
                test_context,
                # Temporary workaround:
                # Skip because we don't always run redpanda/setup tests at all and it fails
                # to cleanup. Will be fixed once we will avoid entering the tests at all if
                # they shouldn't run.
                skip_end_of_test_scrubbing=True),
            extra_rp_conf={
                "iceberg_enabled": "true",
                "iceberg_catalog_commit_interval_ms": 5000
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
            *args,
            **kwargs)
        self.test_context = test_context
        self.topic_name = "test"

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    @cluster(num_nodes=2)
    @skip_cloud_provider(["gcp", "azure"])
    @matrix(cloud_storage_type=supported_storage_types())
    def test_e2e_basic(self, cloud_storage_type):
        count = 100
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              include_query_engines=[
                                  QueryEngineType.DATABRICKS_SQL,
                              ],
                              catalog_type=CatalogType.DATABRICKS_UNITY) as dl:
            dl.create_iceberg_enabled_topic(self.topic_name, partitions=10)
            dl.produce_to_topic(self.topic_name, 1024, count)

            dl.wait_for_translation(self.topic_name,
                                    msg_count=count,
                                    timeout=60)

    # This test does not work because Iceberg tables in the managed catalog
    # w/ their databricks sql engine are read only. I.e. there is no support
    # for DELETE statements. They fail with Read Iceberg with Delta Uniform
    # has failed. Operation is not supported. Only CREATE and REFRESH are
    # supported on Uniform Iceberg Ingress Table.
    #
    # @cluster(num_nodes=4)
    # @matrix(cloud_storage_type=supported_storage_types())
    # def test_upload_after_external_update(self, cloud_storage_type):
    #     # TODO: Move this in the matrix decorator. Somehow.
    #     if not DatabricksContext.available(self.test_context):
    #         self.logger.warning(
    #             "Skipping test because Databricks context is not available")
    #         cleanup_on_early_exit(self)
    #         return

    #     table_name = f"redpanda.{self.topic_name}"
    #     with DatalakeServices(self.test_context,
    #                           redpanda=self.redpanda,
    #                           include_query_engines=[
    #                               QueryEngineType.DATABRICKS_SQL,
    #                           ],
    #                           catalog_type=CatalogType.DATABRICKS_UNITY) as dl:
    #         count = 100
    #         dl.create_iceberg_enabled_topic(self.topic_name, partitions=1)
    #         dl.produce_to_topic(self.topic_name, 1024, count)
    #         dl.wait_for_translation(self.topic_name, count)

    #         query_engine = dl.query_engine(QueryEngineType.DATABRICKS_SQL)
    #         query_engine.make_client().cursor().execute(
    #             f"delete from {table_name}")

    #         count_after_del = query_engine.count_table("redpanda",
    #                                                    self.topic_name)
    #         assert count_after_del == 0, f"{count_after_del} rows, expected 0"

    #         dl.produce_to_topic(self.topic_name, 1024, count)
    #         dl.wait_for_translation_until_offset(self.topic_name,
    #                                              2 * count - 1)
    #         count_after_produce = query_engine.count_table(
    #             "redpanda", self.topic_name)
    #         assert count_after_produce == count, f"{count_after_produce} rows, expected {count}"
