from _typeshed import Incomplete
from ducktape.services.service import (
    MultiRunServiceIdFactory as MultiRunServiceIdFactory,
)
from ducktape.services.service import (
    service_id_factory as service_id_factory,
)
from ducktape.services.service_registry import ServiceRegistry as ServiceRegistry
from ducktape.tests.event import ClientEventFactory as ClientEventFactory
from ducktape.tests.loader import TestLoader as TestLoader
from ducktape.tests.result import (
    FAIL as FAIL,
)
from ducktape.tests.result import (
    IGNORE as IGNORE,
)
from ducktape.tests.result import (
    OFAIL as OFAIL,
)
from ducktape.tests.result import (
    OPASS as OPASS,
)
from ducktape.tests.result import (
    PASS as PASS,
)
from ducktape.tests.result import (
    TestResult as TestResult,
)
from ducktape.tests.serde import SerDe as SerDe
from ducktape.tests.status import FLAKY as FLAKY
from ducktape.tests.test import TestContext as TestContext
from ducktape.tests.test import test_logger as test_logger
from ducktape.utils.local_filesystem_utils import mkdir_p as mkdir_p

def run_client(*args, **kwargs) -> None: ...

class RunnerClient:
    serde: Incomplete
    logger: Incomplete
    runner_port: Incomplete
    fail_bad_cluster_utilization: Incomplete
    test_id: Incomplete
    test_index: Incomplete
    id: Incomplete
    message: Incomplete
    sender: Incomplete
    session_context: Incomplete
    test_metadata: Incomplete
    cluster: Incomplete
    deflake_num: Incomplete
    test: Incomplete
    test_context: Incomplete
    all_services: Incomplete
    def __init__(
        self,
        server_hostname,
        server_port,
        test_id,
        test_index,
        logger_name,
        log_dir,
        debug,
        fail_bad_cluster_utilization,
        deflake_num,
    ) -> None: ...
    def send(self, event): ...
    def run(self): ...
    def setup_test(self) -> None: ...
    def run_test(self): ...
    def teardown_test(
        self, teardown_services: bool = ..., test_status: Incomplete | None = ...
    ): ...
    def log(self, log_level, msg, *args, **kwargs) -> None: ...

class Sender:
    REQUEST_TIMEOUT_MS: int
    NUM_RETRIES: int
    serde: Incomplete
    server_endpoint: Incomplete
    zmq_context: Incomplete
    socket: Incomplete
    poller: Incomplete
    message_supplier: Incomplete
    logger: Incomplete
    def __init__(self, server_host, server_port, message_supplier, logger) -> None: ...
    def send(self, event, blocking: bool = ...): ...
    def close(self) -> None: ...
