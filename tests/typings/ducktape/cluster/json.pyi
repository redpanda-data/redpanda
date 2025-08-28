from .cluster import Cluster as Cluster, ClusterNode as ClusterNode
from .remoteaccount import RemoteAccountSSHConfig as RemoteAccountSSHConfig
from _typeshed import Incomplete
from ducktape.cluster.cluster_spec import ClusterSpec as ClusterSpec, WINDOWS as WINDOWS
from ducktape.cluster.linux_remoteaccount import (
    LinuxRemoteAccount as LinuxRemoteAccount,
)
from ducktape.cluster.node_container import NodeContainer as NodeContainer
from ducktape.cluster.windows_remoteaccount import (
    WindowsRemoteAccount as WindowsRemoteAccount,
)
from ducktape.command_line.defaults import ConsoleDefaults as ConsoleDefaults

class JsonCluster(Cluster):
    def __init__(
        self, cluster_json: Incomplete | None = ..., *args, **kwargs
    ) -> None: ...
    @staticmethod
    def make_remote_account(ssh_config, *args, **kwargs): ...
    def do_alloc(self, cluster_spec): ...
    def free_single(self, node) -> None: ...
    def available(self): ...
    def used(self): ...
