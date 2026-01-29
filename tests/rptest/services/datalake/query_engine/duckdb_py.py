# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from urllib.parse import urlparse

import duckdb
from ducktape.tests.test import TestContext
from rptest.context import cloud_storage
from rptest.services.catalog_service import CatalogType
from rptest.tests.datalake.query_engine_base import QueryEngineBase, QueryEngineType


class DuckDBPy(QueryEngineBase):
    """
    DuckDBPy query engine service for Iceberg tables.

    Might not be ready for widespread use yet, but it is enough for testing
    our Iceberg implementation compatibility with DuckDB.
    """

    def __init__(
        self,
        ctx: TestContext,
        iceberg_catalog_uri: str,
        default_warehouse_dir: str,
        catalog_type: CatalogType,
        catalog_name: str,
    ):
        super().__init__(ctx, num_nodes=0)

        self._catalog_name = catalog_name
        self._iceberg_catalog_uri = iceberg_catalog_uri
        self._default_warehouse_dir = default_warehouse_dir
        self._catalog_type = catalog_type

        # Currently only supporting S3 storage, can be extended in the future.
        # Note that DuckDB v1.3 supports only S3 backed iceberg rest catalogs.
        #
        # Quote: Reading from Iceberg REST Catalogs backed by remote storage that is
        #   not S3 or S3Tables is not yet supported.
        #   —https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs#limitations
        self._storage_credentials = cloud_storage.Credentials.from_context(ctx)
        assert isinstance(self._storage_credentials, cloud_storage.S3Credentials), (
            "DuckDBPy query service is implemented only for S3 credentials"
        )

    @staticmethod
    def engine_name() -> QueryEngineType:
        return QueryEngineType.DUCKDB_PY

    def make_client(self):
        """
        Creates and configures a DuckDB connection with Iceberg support
        """
        conn = duckdb.connect()
        conn.install_extension("iceberg")
        conn.load_extension("iceberg")

        # DuckDB has some problems here interpolating query parameters, so we use f-string
        # to construct the query. This is safe here because we control the input.
        conn.execute(f"""
            ATTACH '{self._catalog_name}' AS iceberg_catalog (
            TYPE iceberg,
            AUTHORIZATION_TYPE 'none',
            ENDPOINT '{self._iceberg_catalog_uri}'
            )""")

        # Currently only supporting S3 storage
        assert isinstance(self._storage_credentials, cloud_storage.S3Credentials)
        # DuckDB requires endpoint to be of the form "hostname:port"
        assert self._storage_credentials.endpoint is not None, (
            "S3 endpoint must not be None"
        )
        parsed = urlparse(self._storage_credentials.endpoint)
        endpoint = parsed.hostname
        if parsed.port:
            endpoint = f"{endpoint}:{parsed.port}"

            # Create S3 secret for authentication
        create_secret_res = conn.execute(
            """
            CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER config,
                KEY_ID ?,
                SECRET ?,
                REGION ?,
                ENDPOINT ?,
                USE_SSL false,
                URL_STYLE 'path'
            );""",
            (
                self._storage_credentials.access_key,
                self._storage_credentials.secret_key,
                self._storage_credentials.region,
                endpoint,
            ),
        ).fetchone()

        if create_secret_res is None or not create_secret_res[0]:
            raise RuntimeError(
                f"Failed to create secret in DuckDB. Result: {create_secret_res}"
            )

        conn.execute("USE iceberg_catalog.redpanda")

        return Connection(conn)

    def escape_identifier(self, table: str) -> str:
        return f'"{table}"'

    def count_parquet_files(self, namespace: str, table: str) -> int:
        raise NotImplementedError("count_parquet_files is not implemented yet")

    def optimize_parquet_files(self, namespace: str, table: str) -> None:
        raise NotImplementedError("optimize_parquet_files is not implemented yet")


class Connection:
    """
    PEP 249 compliant client connection wrapper for DuckDBPyConnection
    so that we can pre-execute some setup commands and provide a custom cursor method.
    See https://peps.python.org/pep-0249/#connection-objects
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def close(self):
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def cursor(self):
        return self.conn.cursor().execute("USE iceberg_catalog.redpanda")

    def execute(self, query, params=None):
        return self.conn.execute(query, params)
