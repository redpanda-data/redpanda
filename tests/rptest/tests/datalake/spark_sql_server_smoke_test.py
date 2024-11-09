# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Optional
from rptest.services.cluster import cluster
from rptest.services.spark_service import SparkService
from rptest.tests.datalake.iceberg_rest_catalog import IcebergRESTCatalogTest
from rptest.tests.datalake.utils import supported_storage_types
from ducktape.mark import matrix


class SparkSmokeTest(IcebergRESTCatalogTest):
    """Tests basic Spark functionality with iceberg REST server"""
    def __init__(self, test_ctx, *args, **kwargs):
        self.test_ctx = test_ctx
        super(SparkSmokeTest, self).__init__(test_ctx,
                                             num_brokers=1,
                                             *args,
                                             **kwargs)
        self.spark: Optional[SparkService] = None

    def setUp(self):
        super().setUp()
        si = self.redpanda.si_settings
        self.spark = SparkService(self.test_ctx,
                                  self.catalog_service.catalog_url, si)
        self.spark.start()

    def tearDown(self):
        if self.spark:
            self.spark.stop()
        return super().tearDown()

    def execute_query(self, query_str):
        assert self.spark
        return self.spark.execute(query=query_str)

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_spark_smoke(self, cloud_storage_type):
        assert self.spark
        client = self.spark.make_client()
        try:
            cursor = client.cursor()
            try:
                cursor.execute("CREATE SCHEMA redpanda")
                # Create an iceberg table
                cursor.execute(
                    "CREATE TABLE redpanda.test(id bigint NOT NULL, data string) USING iceberg"
                )
                cursor.execute(
                    "INSERT into redpanda.test values(2024, 'Wohn Jick')")
                cursor.execute("SELECT count(*) from redpanda.test")
                count = cursor.fetchone()[0]
                assert count == 1, count
            finally:
                cursor.close()
        finally:
            client.close()
