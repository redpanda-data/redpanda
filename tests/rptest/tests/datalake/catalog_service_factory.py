# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.catalog_service import CatalogType, CatalogService
from rptest.services.nessie_catalog import NessieCatalog
from rptest.services.apache_iceberg_catalog import IcebergRESTCatalog

from typing import List

SUPPORTED_CATALOG_TYPES = [
    CatalogType.REST_JDBC,
    CatalogType.REST_HADOOP,
    CatalogType.NESSIE,
]


def filesystem_catalog_type() -> CatalogType:
    return CatalogType.REST_HADOOP


def supported_catalog_types() -> List[CatalogType]:
    return SUPPORTED_CATALOG_TYPES


def make_catalog_service_for_type(
    catalog_type: CatalogType, test_ctx, cloud_storage_bucket: str, warehouse_name: str
) -> CatalogService:
    if catalog_type == CatalogType.REST_JDBC:
        return IcebergRESTCatalog(
            test_ctx,
            cloud_storage_bucket=cloud_storage_bucket,
            warehouse_name=warehouse_name,
        )
    elif catalog_type == CatalogType.REST_HADOOP:
        return IcebergRESTCatalog(
            test_ctx,
            cloud_storage_bucket=cloud_storage_bucket,
            warehouse_name=warehouse_name,
            filesystem_wrapper_mode=True,
        )
    elif catalog_type == CatalogType.NESSIE:
        return NessieCatalog(
            test_ctx,
            cloud_storage_bucket=cloud_storage_bucket,
            warehouse_name=warehouse_name,
        )
    else:
        raise NotImplementedError(f"No catalog of type {catalog_type}")
