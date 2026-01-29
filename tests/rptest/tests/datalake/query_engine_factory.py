# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.datalake.query_engine.databricks_sql import DatabricksSQL
from rptest.services.datalake.query_engine.duckdb_py import DuckDBPy
from rptest.services.spark_service import SparkService
from rptest.services.trino_service import TrinoService
from rptest.tests.datalake.query_engine_base import QueryEngineBase, QueryEngineType

SUPPORTED_QUERY_ENGINES: list[type[QueryEngineBase]] = [
    SparkService,
    TrinoService,
    DatabricksSQL,
    DuckDBPy,
]


def get_query_engine_by_type(
    engine_type: QueryEngineType,
) -> type[QueryEngineBase]:
    for svc in SUPPORTED_QUERY_ENGINES:
        if svc.engine_name() == engine_type:
            return svc
    raise NotImplementedError(f"No query engine of type {engine_type}")
