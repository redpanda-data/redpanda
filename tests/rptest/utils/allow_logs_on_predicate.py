# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from typing import Callable, Optional, Protocol


class ConvertableToBool(Protocol):
    def __bool__(self) -> bool:
        ...


PredicateT = Callable[[str], ConvertableToBool]


class AllowLogsOnPredicate:
    """
    Calls the supplied test method to determine if ERROR logs should be allowed
    """
    def __init__(self, method: str):
        """
        predicate_method: The name of the method on the test which will accept a log entry,
        and return a truthy value to signal if the ERROR log is allowed.
        """
        self.method = method
        self.predicate: Optional[PredicateT] = None

    def initialize(self, test_case):
        """
        Called with the test case during the `cluster` decorator run, after this object
        has been constructed with the method name. Validates and sets the method to be
        called per log entry.
        """
        assert hasattr(
            test_case, self.method
        ), f"Test {test_case} does not have expected method {self.method}"
        self.predicate = getattr(test_case, self.method)
        assert callable(
            self.predicate), f"{self.predicate} is not a callable method"

    def search(self, log_line: str) -> ConvertableToBool:
        """
        Explicitly convert falsy values to None. This matches semantics of `re.search` which this method is
        expected to be called with, where None is returned for failed searches. This conversion allows
        the predicate to return falsy values to reject a log from being allowed.
        """
        assert self.predicate is not None, f"Log predicate method {self.method} is not initialized"
        return self.predicate(log_line) or None
