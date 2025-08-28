# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import List

from rptest.services.catalog_service import CatalogType

SUPPORTED_CATALOG_TYPES = [
    CatalogType.REST_JDBC,
    CatalogType.REST_HADOOP,
    CatalogType.NESSIE,
]


def filesystem_catalog_type() -> CatalogType:
    return CatalogType.REST_HADOOP


def supported_catalog_types() -> List[CatalogType]:
    return SUPPORTED_CATALOG_TYPES
