from _typeshed import Incomplete
from ducktape.cluster.cluster_spec import WINDOWS as WINDOWS
from ducktape.cluster.remoteaccount import (
    RemoteAccount as RemoteAccount,
)
from ducktape.cluster.remoteaccount import (
    RemoteCommandError as RemoteCommandError,
)

class WindowsRemoteAccount(RemoteAccount):
    WINRM_USERNAME: str
    os: Incomplete
    def __init__(self, *args, **kwargs) -> None: ...
    @property
    def winrm_client(self): ...
    def fetch_externally_routable_ip(self, is_aws: Incomplete | None = ...): ...
    def run_winrm_command(self, cmd, allow_fail: bool = ...): ...
