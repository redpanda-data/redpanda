# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import re
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class TimeRange:
    start: datetime
    end: datetime

    def __contains__(self, item: datetime):
        return self.start <= item <= self.end

    def __repr__(self):
        return f'{self.start} -> {self.end}'


@contextmanager
def bad_logs_allowed(self, expression):
    """
    self is the test instance using the decorator. The time range spanning the execution of
    the decorator is recorded within the test as a span where the expression is allowed.
    """
    start = datetime.now()
    if not hasattr(self, 'bad_log_allowed_time_ranges'):
        self.bad_log_allowed_time_ranges = defaultdict(list)
    try:
        yield
    finally:
        end = datetime.now()
        self.bad_log_allowed_time_ranges[expression].append(
            TimeRange(start=start, end=end))


class TimedLogAllowList:
    time_ranges: list[TimeRange]

    def __init__(self, expression):
        """
        The expression is used to match log lines. The time ranges are gathered by the
        bad_logs_allowed decorator during the test run.
        """
        if isinstance(expression, str):
            self.expression = re.compile(expression)
        elif isinstance(expression, re.Pattern):
            self.expression = expression
        else:
            raise ValueError(
                f'{expression} has invalid type {type(expression)}: expected str|re.Pattern'
            )
        self.time_ranges = []

    def search(self, line: str) -> Optional[TimeRange]:
        if not self.time_ranges or not self.expression.search(line):
            return None

        tokens = line.split()
        if len(tokens) < 3:
            return None

        # 2023-10-12 16:32:48,364
        time_str = f'{tokens[1]} {tokens[2]}'
        time_point = datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S,%f')
        return next((tr for tr in self.time_ranges if time_point in tr), None)
