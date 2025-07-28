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
from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from rptest.context.databricks import DatabricksContext as DatabricksContext
from rptest.services.catalog_service import CatalogType
from rptest.tests.datalake.query_engine_base import QueryEngineBase, QueryEngineType
from rptest.services.datalake.util.table_verifier import verify_rows


class DatabricksSQL(Service, QueryEngineBase):
    def __init__(
        self,
        ctx: TestContext,
        iceberg_catalog_uri: str,
        default_warehouse_dir: str,
        catalog_type: CatalogType,
        catalog_name: str,
    ):
        assert catalog_type == CatalogType.DATABRICKS_UNITY, "Only DATABRICKS_UNITY catalog type is supported"

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
            "DatabricksSQL count_parquet_files is not implemented yet")

    def optimize_parquet_files(self, namespace: str, table: str) -> None:
        raise NotImplementedError(
            "DatabricksSQL optimize_parquet_files is not implemented yet")

    def verify_table_contents(
            self, namespace: str, table: str,
            expected_records: list[tuple[str, str | dict, dict]]) -> bool:
        self.logger.debug("Starting verify_table_contents")
        client = self.make_client()
        cursor = client.cursor()
        query = (f"SELECT redpanda.key, value, redpanda.headers\n"
                 f"FROM `{self._catalog_name}`.`{namespace}`.`{table}`\n"
                 f"WHERE value IS NOT NULL\n"
                 f"ORDER BY redpanda.offset DESC\n"
                 f"LIMIT 100")

        self.logger.debug(f"Running verification query:\n{query}")
        cursor.execute(query)

        try:
            rows = cursor.fetchall()
            self.logger.debug(f"Fetched {len(rows)} rows")
        except Exception as e:
            self.logger.error("Failed to fetch rows from cursor",
                              exc_info=True)
            raise

        self.logger.debug("Calling verify_rows with fetched rows...")

        success, errors = verify_rows(rows, expected_records, self.logger)
        if not success:
            for err in errors:
                self.logger.error(err)
            raise AssertionError("Verification failed for table contents.")

        self.logger.info("All expected records found and verified.")
        return True
