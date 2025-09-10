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
from rptest.tests.datalake.iceberg_rest_catalog_test import IcebergRESTCatalogTest
from rptest.tests.datalake.utils import supported_storage_types


class IcebergRESTCatalogSmokeTest(IcebergRESTCatalogTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(IcebergRESTCatalogSmokeTest, self).__init__(
            test_ctx, num_brokers=1, *args, extra_rp_conf={}, **kwargs
        )

    def setUp(self):
        # custom startup logic below
        pass

    @cluster(num_nodes=2)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        filesystem_catalog_mode=[True, False],
    )
    def test_basic(self, cloud_storage_type, filesystem_catalog_mode):
        self.catalog_service.set_filesystem_wrapper_mode(filesystem_catalog_mode)
        super().setUp()

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
        table = catalog.create_table(identifier=f"{namespace}.bids", schema=schema)
        self.logger.info(f">>> {table}")

        assert "bids" in [t[1] for t in catalog.list_tables(namespace)]

    @cluster(num_nodes=2)
    @matrix(
        cloud_storage_type=supported_storage_types(),
        filesystem_catalog_mode=[True, False],
    )
    def test_redpanda_schema(self, cloud_storage_type, filesystem_catalog_mode):
        self.catalog_service.set_filesystem_wrapper_mode(filesystem_catalog_mode)
        super().setUp()

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
