from _typeshed import Incomplete
from ducktape.services.service import Service as Service

class BackgroundThreadService(Service):
    worker_threads: Incomplete
    worker_errors: Incomplete
    errors: str
    lock: Incomplete
    def __init__(
        self,
        context,
        num_nodes: Incomplete | None = ...,
        cluster_spec: Incomplete | None = ...,
        *args,
        **kwargs,
    ) -> None: ...
    def start_node(self, node) -> None: ...
    def wait(self, timeout_sec: int = ...) -> None: ...
    def stop(self) -> None: ...
    def wait_node(self, node, timeout_sec: int = ...): ...
