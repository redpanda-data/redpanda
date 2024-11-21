from rptest.services.cluster import cluster

from ducktape.mark import matrix
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
from rptest.tests.datalake.utils import supported_storage_types


class IcebergRESTCatalogSmokeTest(IcebergRESTCatalogTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(IcebergRESTCatalogSmokeTest, self).__init__(test_ctx,
                                                          num_brokers=1,
                                                          *args,
                                                          extra_rp_conf={},
                                                          **kwargs)

    def setUp(self):
        # custom startup logic below
        pass

    @cluster(num_nodes=2)
    @matrix(cloud_storage_type=supported_storage_types(),
            filesystem_catalog_mode=[True, False])
    def test_basic(self, cloud_storage_type, filesystem_catalog_mode):

        self.catalog_service.set_filesystem_wrapper_mode(
            filesystem_catalog_mode)
        super().setUp()

        warehouse = self.catalog_service.cloud_storage_warehouse
        catalog = self.catalog_service.client()
        namespace = "test_ns"
        catalog.create_namespace(namespace)
        catalog.list_tables(namespace)

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
        table = catalog.create_table(identifier=f"{namespace}.bids",
                                     schema=schema)
        self.logger.info(f">>> {table}")

        assert "bids" in [t[1] for t in catalog.list_tables(namespace)]
