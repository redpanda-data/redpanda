from _typeshed import Incomplete
from ducktape.json_serializable import DucktapeJSONEncoder as DucktapeJSONEncoder
from ducktape.tests.reporter import SingleResultFileReporter as SingleResultFileReporter
from ducktape.tests.status import (
    FAIL as FAIL,
)
from ducktape.tests.status import (
    FLAKY as FLAKY,
)
from ducktape.tests.status import (
    IGNORE as IGNORE,
)
from ducktape.tests.status import (
    OFAIL as OFAIL,
)
from ducktape.tests.status import (
    OPASS as OPASS,
)
from ducktape.tests.status import (
    PASS as PASS,
)
from ducktape.tests.test import TestContext as TestContext
from ducktape.utils.local_filesystem_utils import mkdir_p as mkdir_p
from ducktape.utils.util import ducktape_version as ducktape_version

class TestResult:
    nodes_allocated: Incomplete
    services: Incomplete
    nodes_used: Incomplete
    test_id: Incomplete
    module_name: Incomplete
    cls_name: Incomplete
    function_name: Incomplete
    injected_args: Incomplete
    description: Incomplete
    results_dir: Incomplete
    test_index: Incomplete
    session_context: Incomplete
    test_status: Incomplete
    summary: Incomplete
    data: Incomplete
    file_name: Incomplete
    base_results_dir: Incomplete
    relative_results_dir: Incomplete
    start_time: Incomplete
    stop_time: Incomplete
    def __init__(
        self,
        test_context,
        test_index,
        session_context,
        test_status=...,
        summary: str = ...,
        data: Incomplete | None = ...,
        start_time: int = ...,
        stop_time: int = ...,
    ) -> None: ...
    @property
    def run_time_seconds(self): ...
    def report(self) -> None: ...
    def dump_json(self) -> None: ...
    def to_json(self): ...

class TestResults:
    session_context: Incomplete
    cluster: Incomplete
    command_line: str
    start_time: int
    stop_time: int
    def __init__(self, session_context, cluster) -> None: ...
    def append(self, obj): ...
    def __len__(self) -> int: ...
    def __iter__(self): ...
    @property
    def num_passed(self): ...
    @property
    def num_failed(self): ...
    @property
    def num_ignored(self): ...
    @property
    def num_flaky(self): ...
    @property
    def num_opassed(self): ...
    @property
    def num_ofailed(self): ...
    @property
    def run_time_seconds(self): ...
    def get_aggregate_success(self): ...
    def to_json(self): ...
