# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    BinaryType,
    DoubleType,
    FloatType,
    IntegerType,
    ListType,
    LongType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)

from rptest.services.cluster import cluster
from rptest.services.nessie_catalog import NessieCatalog
from rptest.services.redpanda import SISettings
from rptest.services.spark_service import SparkService
from rptest.services.trino_service import TrinoService
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest


class NessieCatalogSmokeTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self.test_ctx = test_ctx
        si_settings = SISettings(test_context=test_ctx)
        super(NessieCatalogSmokeTest, self).__init__(
            test_ctx, si_settings=si_settings, *args, **kwargs
        )
        self.catalog_service = NessieCatalog(
            test_ctx, cloud_storage_bucket=si_settings.cloud_storage_bucket
        )

    def setUp(self):
        self.catalog_service.start()
        return super().setUp()

    def tearDown(self):
        self.catalog_service.stop()
        return super().tearDown()

    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_nessie_with_trino(self, cloud_storage_type):
        self.trino = TrinoService(
            self.test_ctx,
            self.catalog_service.vendor_api_url,
            self.catalog_service.cloud_storage_warehouse,
            self.catalog_service.catalog_type(),
        )
        self.trino.start()
        client = self.trino.make_client()

        try:
            cursor = client.cursor()
            try:
                cursor.execute("CREATE SCHEMA redpanda")
                cursor.fetchall()
                cursor.execute(
                    "CREATE TABLE redpanda.test (year INTEGER NOT NULL, name VARCHAR NOT NULL, age INTEGER, address VARCHAR)"
                )
                cursor.fetchall()

                cursor.execute(
                    "INSERT into redpanda.test values(2024, 'John', 60, 'Wick')"
                )
                cursor.fetchall()

                cursor.execute("SELECT * from redpanda.test")
                row = cursor.fetchall()
                assert len(row) == 1
                assert row == [(2024, "John", 60, "Wick")]
            finally:
                cursor.close()
        finally:
            client.close()
            self.trino.stop()

    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_nessie_with_spark(self, cloud_storage_type):
        self.spark = SparkService(
            self.test_ctx,
            self.catalog_service.vendor_api_url,
            self.catalog_service.cloud_storage_warehouse,
            self.catalog_service.catalog_type(),
        )
        self.spark.start()
        client = self.spark.make_client()
        try:
            cursor = client.cursor()
            try:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS `spark-catalog`.redpanda")
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS `spark-catalog`.redpanda.test(id bigint, data string) USING iceberg"
                )
                cursor.execute(
                    "INSERT INTO `spark-catalog`.redpanda.test VALUES (1, 'Alice'), (2, 'Bob')"
                )

                main_branch = "main"
                dev_branch = "dev"

                cursor.execute(f"CREATE BRANCH IF NOT EXISTS {dev_branch}")

                cursor.execute(
                    f"INSERT INTO `spark-catalog`.redpanda.`test@{dev_branch}` VALUES (3, 'Carol'), (4, 'Doris')"
                )

                cursor.execute(
                    f"SELECT * from `spark-catalog`.redpanda.`test@{dev_branch}` ORDER BY id"
                )
                row = cursor.fetchall()
                assert len(row) == 4
                assert row == [(1, "Alice"), (2, "Bob"), (3, "Carol"), (4, "Doris")]

                cursor.execute(
                    f"SELECT * from `spark-catalog`.redpanda.`test@{main_branch}` ORDER BY id"
                )
                row = cursor.fetchall()
                assert len(row) == 2
                assert row == [(1, "Alice"), (2, "Bob")]

            finally:
                cursor.close()
        finally:
            client.close()
            self.spark.stop()

    @cluster(num_nodes=2)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_basic(self, cloud_storage_type):
        catalog = self.catalog_service.client()
        namespace = "test_ns"
        catalog.create_namespace(namespace)
        catalog.list_tables(namespace)
        schema = Schema(
            NestedField(
                field_id=1, name="datetime", field_type=TimestampType(), required=True
            ),
            NestedField(
                field_id=2, name="symbol", field_type=StringType(), required=True
            ),
            NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
            NestedField(
                field_id=4, name="ask", field_type=DoubleType(), required=False
            ),
            NestedField(
                field_id=5,
                name="details",
                field_type=StructType(
                    NestedField(
                        field_id=4,
                        name="created_by",
                        field_type=StringType(),
                        required=False,
                    ),
                ),
                required=False,
            ),
        )
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=1,
                field_id=1000,
                transform=DayTransform(),
                name="datetime_day",
            )
        )
        table = catalog.create_table(
            identifier=f"{namespace}.bids", schema=schema, partition_spec=partition_spec
        )
        self.logger.info(f">>> {table}")

        assert "bids" in [t[1] for t in catalog.list_tables(namespace)]

    @cluster(num_nodes=2)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_redpanda_schema(self, cloud_storage_type):
        catalog = self.catalog_service.client()
        namespace = "test_ns"
        catalog.create_namespace(namespace)
        catalog.list_tables(namespace)

        headers_kv = StructType(
            NestedField(
                field_id=7, name="key", field_type=BinaryType(), required=False
            ),
            NestedField(
                field_id=8, name="value", field_type=BinaryType(), required=False
            ),
        )

        system_fields = StructType(
            NestedField(
                field_id=2, name="partition", field_type=IntegerType(), required=True
            ),
            NestedField(
                field_id=3, name="offset", field_type=LongType(), required=True
            ),
            NestedField(
                field_id=4, name="timestamp", field_type=TimestampType(), required=True
            ),
            NestedField(
                field_id=5,
                name="headers",
                field_type=ListType(
                    element_id=6, element=headers_kv, element_required=True
                ),
                required=False,
            ),
            NestedField(
                field_id=9, name="key", field_type=BinaryType(), required=False
            ),
        )

        schema = Schema(
            NestedField(
                field_id=1, name="test_schema", field_type=system_fields, required=True
            )
        )
        partition_spec = PartitionSpec(
            PartitionField(
                source_id=4,
                field_id=1000,
                transform=DayTransform(),
                name="datetime_day",
            )
        )
        table = catalog.create_table(
            identifier=f"{namespace}.key", schema=schema, partition_spec=partition_spec
        )
        self.logger.info(f">>> {table}")
