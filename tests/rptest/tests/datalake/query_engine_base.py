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


class QueryEngineType(str, Enum):
    SPARK = 'spark'
    TRINO = 'trino'


class QueryEngineBase(ABC):
    """Captures all the common operations across registered query engines"""
    @staticmethod
    @abstractmethod
    def engine_name() -> QueryEngineType:
        raise NotImplementedError

    @abstractmethod
    def make_client(self):
        raise NotImplementedError

    @contextmanager
    def run_query(self, query):
        client = self.make_client()
        assert client
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
            return cursor.fetchall()

    def count_table(self, namespace, table) -> int:
        query = f"select count(*) from {namespace}.{self.escape_identifier(table)}"
        with self.run_query(query) as cursor:
            return cursor.fetchone()[0]

    def max_translated_offset(self, namespace, table, partition) -> int:
        query = f"select max(redpanda.offset) from {namespace}.{self.escape_identifier(table)} where redpanda.partition={partition}"
        with self.run_query(query) as cursor:
            return cursor.fetchone()[0]
