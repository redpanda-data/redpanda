# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from abc import ABC, abstractmethod
from contextlib import contextmanager
from enum import Enum
from typing import Any
from rptest.tests.datalake.iceberg import Identifier

from ducktape.services.service import Service


class QueryEngineType(str, Enum):
    SPARK = "spark"
    TRINO = "trino"
    DATABRICKS_SQL = "databricks_sql"
    DUCKDB_PY = "duckdb_py"


class QueryEngineBase(Service, ABC):
    """Captures all the common operations across registered query engines"""

    @staticmethod
    @abstractmethod
    def engine_name() -> QueryEngineType:
        raise NotImplementedError

    @abstractmethod
    def make_client(self):
        """
        A PEP 249 compliant client connection object.
        See https://peps.python.org/pep-0249/#connection-objects
        """
        raise NotImplementedError

    @contextmanager
    def run_query(self, query):
        """
        A PEP 249 compliant cursor object.
        See https://peps.python.org/pep-0249/#cursor-objects
        """
        client = self.make_client()
        assert client
        self.logger.debug(f"running query: {query}")
        try:
            try:
                cursor = client.cursor()
                cursor.execute(query)
                yield cursor
            finally:
                cursor.close()
        finally:
            client.close()

    @abstractmethod
    def escape_identifier(self, table: str) -> str:
        raise NotImplementedError

    def run_query_fetch_all(self, query):
        with self.run_query(query) as cursor:
            result = cursor.fetchall()
            self.logger.debug(f"query result: {result}")
            return result

    def run_query_fetch_one(self, query: Any) -> Any:
        with self.run_query(query) as cursor:
            return cursor.fetchone()

    def count_table(self, namespace: str | Identifier, table) -> int:
        if isinstance(namespace, tuple):
            namespace = ".".join(namespace)

        query = f"select count(*) from {namespace}.{self.escape_identifier(table)}"
        with self.run_query(query) as cursor:
            return cursor.fetchone()[0]

    def max_translated_offset(self, namespace, table, partition) -> int:
        query = f"select max(redpanda.offset) from {namespace}.{self.escape_identifier(table)} where redpanda.partition={partition}"
        with self.run_query(query) as cursor:
            return cursor.fetchone()[0]

    @abstractmethod
    def count_parquet_files(self, namespace, table) -> int: ...

    @abstractmethod
    def optimize_parquet_files(self, namespace, table) -> None: ...
