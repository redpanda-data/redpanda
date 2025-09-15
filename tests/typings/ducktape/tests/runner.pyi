from typing import NamedTuple

from _typeshed import Incomplete
from ducktape.cluster.finite_subcluster import FiniteSubcluster as FiniteSubcluster
from ducktape.command_line.defaults import ConsoleDefaults as ConsoleDefaults
from ducktape.errors import TimeoutError as TimeoutError
from ducktape.tests.event import (
    ClientEventFactory as ClientEventFactory,
)
from ducktape.tests.event import (
    EventResponseFactory as EventResponseFactory,
)
from ducktape.tests.reporter import (
    HTMLSummaryReporter as HTMLSummaryReporter,
)
from ducktape.tests.reporter import (
    JSONReporter as JSONReporter,
)
from ducktape.tests.reporter import (
    SimpleFileSummaryReporter as SimpleFileSummaryReporter,
)
from ducktape.tests.result import (
    FAIL as FAIL,
)
from ducktape.tests.result import (
    TestResult as TestResult,
)
from ducktape.tests.result import (
    TestResults as TestResults,
)
from ducktape.tests.runner_client import run_client as run_client
from ducktape.tests.scheduler import TestScheduler as TestScheduler
from ducktape.tests.serde import SerDe as SerDe
from ducktape.tests.test import TestContext as TestContext
from ducktape.utils import persistence as persistence
from ducktape.utils.terminal_size import get_terminal_size as get_terminal_size

class Receiver:
    port: Incomplete
    min_port: Incomplete
    max_port: Incomplete
    serde: Incomplete
    zmq_context: Incomplete
    socket: Incomplete
    def __init__(self, min_port, max_port) -> None: ...
    def start(self) -> None: ...
    def recv(self, timeout: int = ...): ...
    def send(self, event) -> None: ...
    def close(self) -> None: ...

class TestKey(NamedTuple):
    test_id: Incomplete
    test_index: Incomplete

class TestRunner:
    stop_testing: bool
    session_logger: Incomplete
    cluster: Incomplete
    event_response: Incomplete
    hostname: str
    receiver: Incomplete
    deflake_num: Incomplete
    session_context: Incomplete
    max_parallel: Incomplete
    results: Incomplete
    exit_first: Incomplete
    main_process_pid: Incomplete
    scheduler: Incomplete
    test_counter: int
    total_tests: Incomplete
    active_tests: Incomplete
    finished_tests: Incomplete
    def __init__(
        self,
        cluster,
        session_context,
        session_logger,
        tests,
        deflake_num,
        min_port=...,
        max_port=...,
    ) -> None: ...
    def who_am_i(self): ...
    def run_all_tests(self): ...
    def active_tests_debug(self): ...
