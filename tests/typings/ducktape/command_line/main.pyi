from ducktape.command_line.defaults import ConsoleDefaults as ConsoleDefaults
from ducktape.command_line.parse_args import parse_args as parse_args
from ducktape.tests.loader import (
    LoaderException as LoaderException,
)
from ducktape.tests.loader import (
    TestLoader as TestLoader,
)
from ducktape.tests.loggermaker import close_logger as close_logger
from ducktape.tests.reporter import (
    FailedTestSymbolReporter as FailedTestSymbolReporter,
)
from ducktape.tests.reporter import (
    HTMLSummaryReporter as HTMLSummaryReporter,
)
from ducktape.tests.reporter import (
    JSONReporter as JSONReporter,
)
from ducktape.tests.reporter import (
    JUnitReporter as JUnitReporter,
)
from ducktape.tests.reporter import (
    SimpleFileSummaryReporter as SimpleFileSummaryReporter,
)
from ducktape.tests.reporter import (
    SimpleStdoutSummaryReporter as SimpleStdoutSummaryReporter,
)
from ducktape.tests.runner import TestRunner as TestRunner
from ducktape.tests.session import (
    SessionContext as SessionContext,
)
from ducktape.tests.session import (
    SessionLoggerMaker as SessionLoggerMaker,
)
from ducktape.tests.session import (
    generate_results_dir as generate_results_dir,
)
from ducktape.tests.session import (
    generate_session_id as generate_session_id,
)
from ducktape.utils import persistence as persistence
from ducktape.utils.local_filesystem_utils import mkdir_p as mkdir_p
from ducktape.utils.util import load_function as load_function

def get_user_defined_globals(globals_str): ...
def setup_results_directory(new_results_dir) -> None: ...
def update_latest_symlink(results_root, new_results_dir) -> None: ...
def main() -> None: ...
