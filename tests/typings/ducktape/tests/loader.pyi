from typing import NamedTuple

from _typeshed import Incomplete
from ducktape.mark import parametrized as parametrized
from ducktape.mark.mark_expander import MarkedFunctionExpander as MarkedFunctionExpander
from ducktape.tests.test import Test as Test
from ducktape.tests.test import TestContext as TestContext

class LoaderException(Exception): ...

class ModuleAndFile(NamedTuple):
    module: Incomplete
    file: Incomplete

DEFAULT_TEST_FILE_PATTERN: str
DEFAULT_TEST_FUNCTION_PATTERN: str

class TestLoader:
    session_context: Incomplete
    cluster: Incomplete
    logger: Incomplete
    repeat: Incomplete
    subset: Incomplete
    subsets: Incomplete
    historical_report: Incomplete
    test_file_pattern: Incomplete
    test_function_pattern: Incomplete
    injected_args: Incomplete
    def __init__(
        self,
        session_context,
        logger,
        repeat: int = ...,
        injected_args: Incomplete | None = ...,
        cluster: Incomplete | None = ...,
        subset: int = ...,
        subsets: int = ...,
        historical_report: Incomplete | None = ...,
    ) -> None: ...
    def load(self, symbols, excluded_test_symbols: Incomplete | None = ...): ...
    def discover(
        self,
        directory,
        module_name,
        cls_name,
        method_name,
        injected_args: Incomplete | None = ...,
    ): ...
