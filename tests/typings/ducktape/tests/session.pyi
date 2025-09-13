from typing import Any

from _typeshed import Incomplete
from ducktape.command_line.defaults import ConsoleDefaults as ConsoleDefaults
from ducktape.tests.loggermaker import LoggerMaker as LoggerMaker

class SessionContext:
    session_id: Incomplete
    results_dir: Incomplete
    debug: Incomplete
    compress: Incomplete
    exit_first: Incomplete
    no_teardown: Incomplete
    max_parallel: Incomplete
    default_expected_num_nodes: Incomplete
    fail_bad_cluster_utilization: Incomplete
    test_runner_timeout: Incomplete

    def __init__(self, **kwargs: Any) -> None: ...
    @property
    def globals(self) -> dict[str, Any]: ...
    def to_json(self) -> dict[str, Any]: ...

class SessionLoggerMaker(LoggerMaker):
    log_dir: Incomplete
    debug: Incomplete

    def __init__(self, session_context) -> None: ...
    def configure_logger(self) -> None: ...

def generate_session_id(session_id_file): ...
def generate_results_dir(results_root, session_id): ...
