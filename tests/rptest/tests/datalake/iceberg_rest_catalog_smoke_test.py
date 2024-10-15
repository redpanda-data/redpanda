from rptest.services.cluster import cluster

from rptest.tests.datalake.iceberg_rest_catalog import IcebergRESTCatalogTest
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    StructType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform


class IcebergRESTCatalogSmokeTest(IcebergRESTCatalogTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(IcebergRESTCatalogSmokeTest, self).__init__(test_ctx,
                                                          num_brokers=1,
                                                          *args,
                                                          extra_rp_conf={},
                                                          **kwargs)

    @cluster(num_nodes=2)
    def test_basic(self):
        warehouse = self.catalog_service.cloud_storage_warehouse
        catalog = self.catalog_service.client()
        catalog.create_namespace("ducktape123")
        catalog.list_tables("ducktape123")

        schema = Schema(
            NestedField(field_id=1,
                        name="datetime",
                        field_type=TimestampType(),
                        required=True),
            NestedField(field_id=2,
                        name="symbol",
                        field_type=StringType(),
                        required=True),
            NestedField(field_id=3,
                        name="bid",
                        field_type=FloatType(),
                        required=False),
            NestedField(field_id=4,
                        name="ask",
                        field_type=DoubleType(),
                        required=False),
            NestedField(
                field_id=5,
                name="details",
                field_type=StructType(
                    NestedField(field_id=4,
                                name="created_by",
                                field_type=StringType(),
                                required=False), ),
                required=False,
            ),
        )
        catalog.create_table(identifier="ducktape.bids",
                             schema=schema,
                             location=f"s3a://{warehouse}/bids")

        assert catalog.table_exists("ducktape.bids")
