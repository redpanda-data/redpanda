# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0


class NoProgressError(Exception):
    pass


class ConsistencyCheckError(Exception):
    pass


class Result:
    PASSED = "PASSED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"
    NODATA = "NODATA"

    @staticmethod
    def more_severe(a, b):
        results = [Result.FAILED, Result.UNKNOWN, Result.PASSED, Result.NODATA]
        if a not in results:
            raise Exception(f"unknown result value: a={a}")
        if b not in results:
            raise Exception(f"unknown result value: b={b}")
        if a == Result.FAILED or b == Result.FAILED:
            return Result.FAILED
        if a == Result.UNKNOWN or b == Result.UNKNOWN:
            return Result.UNKNOWN
        if a == Result.NODATA or b == Result.NODATA:
            return Result.NODATA
        return Result.PASSED
