from _typeshed import Incomplete
from ducktape.tests.test import TestContext as TestContext

from ._mark import Parametrize as Parametrize
from ._mark import parametrized as parametrized

class MarkedFunctionExpander:
    seed_context: Incomplete
    context_list: Incomplete
    def __init__(
        self,
        session_context: Incomplete | None = ...,
        module: Incomplete | None = ...,
        cls: Incomplete | None = ...,
        function: Incomplete | None = ...,
        file: Incomplete | None = ...,
        cluster: Incomplete | None = ...,
    ) -> None: ...
    def expand(self, test_parameters: Incomplete | None = ...): ...
