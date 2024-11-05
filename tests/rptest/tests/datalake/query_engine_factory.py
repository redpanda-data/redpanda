# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.spark_service import SparkService
from rptest.services.trino_service import TrinoService
from rptest.tests.datalake.query_engine_base import QueryEngineType

SUPPORTED_QUERY_ENGINES = [SparkService, TrinoService]


def get_query_engine_by_type(type: QueryEngineType):
    for svc in SUPPORTED_QUERY_ENGINES:
        if svc.engine_name() == type:
            return svc
    raise NotImplementedError(f"No query engine of type {type}")
