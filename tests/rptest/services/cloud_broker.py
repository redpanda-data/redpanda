import json
import os
import subprocess
from rptest.services.utils import KubeNodeShell

from dataclasses import dataclass


@dataclass
class DummyAccount():
    node_name: str
    pod_name: str

    @property
    def hostname(self) -> str:
        return f"{self.node_name}/{self.pod_name}"


class CloudBroker():
    # Mimic ducktape cluster node structure
    def __init__(self, pod, kubectl, logger) -> None:
        self.logger = logger
        # Validate
        if not isinstance(pod, dict) or pod['kind'] != 'Pod':
            self.logger.error("Invalid pod data provided")

        # save client
        self._kubeclient = kubectl

        # Metadata
        self.operating_system = 'k8s'
        self._meta = pod['metadata']
        self.name = self._meta['name']

        # Backward compatibility
        # Various classes will use this to hash and compare nodes
        self.account = DummyAccount(node_name=pod['spec']['nodeName'],
                                    pod_name=pod['metadata']['name'])

        # It appears that the node-id label will only be added if the cluster
        # is still being managed by the operator - if managed=false then this
        # label won't be updated/added
        # self.slot_id = int(
        #    self._meta['labels']['operator.redpanda.com/node-id'])

        # Other dirty way to get node-id
        self.slot_id = int(self.name.replace(self._meta['generateName'], ""))
        self.uuid = self._meta['uid']

        # Save other data
        self._spec = pod['spec']
        self._status = pod['status']

        # Mimic Ducktape cluster node hostname field
        self.hostname = f"{self._spec['nodeName']}/{self.name}"
        self.nodename = self._spec['nodeName']
        # Create node shell pod
        self.nodeshell = KubeNodeShell(self._kubeclient,
                                       self.nodename,
                                       clean=False)
        # Init node shell pod beforehand
        self.nodeshell.initialize_nodeshell()
        # Prepare log extraction script
        self.inject_script("pod_log_extract.sh")

    def inject_script(self, script_name):
        # Modified version of inject_script func
        self.logger.info(f"Injecting '{script_name}' to {self.nodename}")
        rptest_dir = os.path.dirname(os.path.realpath(__file__))
        scripts_dir = os.path.join(rptest_dir, os.path.pardir,
                                   "remote_scripts", "cloud")
        scripts_dir = os.path.abspath(scripts_dir)
        assert os.path.exists(scripts_dir)
        script_path = os.path.join(scripts_dir, script_name)
        assert os.path.exists(script_path)

        # not using paramiko due to complexity of routing to actual node
        # Copy ducktape -> agent
        _scp_cmd = self._kubeclient._scp_cmd(
            script_path, f"{self._kubeclient._remote_uri}:")
        self.logger.debug(_scp_cmd)
        res = subprocess.check_output(_scp_cmd)
        # Copy agent -> broker node
        remote_path = os.path.join("/tmp", script_name)
        _cp_cmd = self._kubeclient._ssh_prefix() + [
            'kubectl', 'cp', script_name,
            f"{self.nodeshell.pod_name}:{remote_path}"
        ]
        self.logger.debug(_cp_cmd)
        res = subprocess.check_output(_cp_cmd)
        return

    def _query_broker(self, path, port=None):
        """
            Function queries Proxy API via ssh/curl to pod
        """
        _baseurl = "http://localhost"
        _port = f":{port}" if port else ""
        _url = f"{_baseurl}{_port}{path}"
        _response = {}
        try:
            _r = self.ssh_output(f"curl -s {_url}")
            _response = json.loads(_r.decode())
        except Exception as e:
            self.logger.warning("Error getting data from broker "
                                f"'{self.name}': {e}")

        return _response

    def query(self, path):
        """
            GET calls to Proxy API via ssh/curl to pod
            https://docs.redpanda.com/api/pandaproxy-rest/
        """
        return self._query_broker(8082, path)

    def query_admin(self, path):
        """
            GET calls to RP Admin API inside pod
            Please, no v1 required in path
            https://docs.redpanda.com/api/admin-api/#tag/Clusters/operation/get_cluster_health_overview
        """
        _path = f"/v1{path}"
        return self._query_broker(9644, _path)

    def get_health_overview(self):
        return self.query_admin("/cluster/health_overview")

    def get_cluster_config_status(self):
        return self.query_admin("/cluster_config/status")

    # Backward compatibility
    def ssh_output(self, cmd):
        return self._kubeclient.exec(cmd)
