from _typeshed import Incomplete
from ducktape.cluster.cluster_spec import LINUX as LINUX
from ducktape.cluster.remoteaccount import (
    RemoteAccount as RemoteAccount,
)
from ducktape.cluster.remoteaccount import (
    RemoteAccountError as RemoteAccountError,
)

class LinuxRemoteAccount(RemoteAccount):
    os: Incomplete
    def __init__(self, *args, **kwargs) -> None: ...
    @property
    def local(self): ...
    def get_network_devices(self): ...
    def get_external_accessible_network_devices(self): ...
    def fetch_externally_routable_ip(self, is_aws: Incomplete | None = ...): ...
