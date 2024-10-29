# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Optional
from rptest.services.cluster import cluster
from rptest.services.trino_service import TrinoService
from rptest.tests.datalake.iceberg_rest_catalog import IcebergRESTCatalogTest


class TrinoSmokeTest(IcebergRESTCatalogTest):
    """Tests basic trino functionality with iceberg connector"""
    def __init__(self, test_ctx, *args, **kwargs):
        self.test_ctx = test_ctx
        super(TrinoSmokeTest, self).__init__(test_ctx,
                                             num_brokers=1,
                                             *args,
                                             **kwargs)
        self.trino: Optional[TrinoService] = None

    def setUp(self):
        super().setUp()
        si = self.redpanda.si_settings
        self.trino = TrinoService(
            self.test_ctx,
            iceberg_catalog_rest_uri=str(self.catalog_service.catalog_url),
            cloud_storage_access_key=str(si.cloud_storage_access_key),
            cloud_storage_secret_key=str(si.cloud_storage_secret_key),
            cloud_storage_region=si.cloud_storage_region,
            cloud_storage_api_endpoint=str(si.endpoint_url))
        self.trino.start()

    def tearDown(self):
        if self.trino:
            self.trino.stop()
        return super().tearDown()

    def execute_query(self, query_str):
        assert self.trino
        return self.trino.execute(query=query_str)

    @cluster(num_nodes=3)
    def test_trino_smoke(self):
        assert self.trino
        client = self.trino.make_client()
        try:
            cursor = client.cursor()
            try:
                cursor.execute("CREATE SCHEMA redpanda")
                cursor.fetchall()
                # Create an iceberg table
                cursor.execute(
                    "CREATE TABLE redpanda.test (year INTEGER NOT NULL, name VARCHAR NOT NULL, age INTEGER, address VARCHAR)"
                )
                cursor.fetchall()
                cursor.execute(
                    "INSERT into redpanda.test values(2024, 'John', 60, 'Wick')"
                )
                cursor.fetchall()
                cursor.execute("SELECT count(*) from redpanda.test")
                count = cursor.fetchone()[0]
                assert count == 1, count
            finally:
                cursor.close()
        finally:
            client.close()
