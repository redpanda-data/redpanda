from _typeshed import Incomplete
from typing import Any
from ducktape.mark._mark import Mark as Mark, P, T, PassDecorator

CLUSTER_SPEC_KEYWORD: str
CLUSTER_SIZE_KEYWORD: str

class ClusterUseMetadata(Mark):
    metadata: Incomplete
    def __init__(self, **kwargs) -> None: ...
    @property
    def name(self): ...
    def apply(self, seed_context, context_list): ...

def cluster(**kwargs: Any) -> PassDecorator[P, T]: ...
