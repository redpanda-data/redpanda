# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import databricks
import databricks.sql
from ducktape.tests.test import TestContext
from rptest.context.databricks import DatabricksContext as DatabricksContext
from rptest.services.catalog_service import CatalogType
from rptest.tests.datalake.query_engine_base import QueryEngineBase, QueryEngineType


class DatabricksSQL(QueryEngineBase):
    def __init__(
        self,
        ctx: TestContext,
        iceberg_catalog_uri: str,
        default_warehouse_dir: str,
        catalog_type: CatalogType,
        catalog_name: str,
    ):
        assert catalog_type == CatalogType.DATABRICKS_UNITY, (
            "Only DATABRICKS_UNITY catalog type is supported"
        )

        super().__init__(ctx, num_nodes=0)

        self._catalog_name = catalog_name
        self._databricks_context = DatabricksContext.from_context(ctx)

    @staticmethod
    def engine_name() -> QueryEngineType:
        return QueryEngineType.DATABRICKS_SQL

    def make_client(self):
        """
        See https://docs.databricks.com/aws/en/dev-tools/python-sql-connector
        """
        return databricks.sql.connect(
            server_hostname=self._databricks_context.server_hostname,
            http_path=self._databricks_context.sql_warehouse_path,
            catalog=self._catalog_name,
            credentials_provider=self._databricks_context.credentials_provider,
        )

    def escape_identifier(self, table: str) -> str:
        return f"`{table}`"

    def count_parquet_files(self, namespace: str, table: str) -> int:
        raise NotImplementedError(
            "DatabricksSQL count_parquet_files is not implemented yet"
        )

    def optimize_parquet_files(self, namespace: str, table: str) -> None:
        result = self.run_query_fetch_one(
            f"OPTIMIZE {self.escape_identifier(namespace)}.{self.escape_identifier(table)}"
        )
        self.logger.debug(f"OPTIMIZE result: {result}")
