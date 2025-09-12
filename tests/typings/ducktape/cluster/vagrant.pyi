from _typeshed import Incomplete
from ducktape.json_serializable import DucktapeJSONEncoder as DucktapeJSONEncoder

from .json import JsonCluster as JsonCluster
from .remoteaccount import RemoteAccountSSHConfig as RemoteAccountSSHConfig

class VagrantCluster(JsonCluster):
    ssh_exception_checks: Incomplete
    def __init__(self, *args, **kwargs) -> None: ...
