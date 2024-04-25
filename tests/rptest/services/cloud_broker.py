import json


class CloudBroker():
    # Mimic ducktape cluster node structure
    class account():
        def __init__(self, pod) -> None:
            self.hostname = f"{pod['spec']['nodeName']}/" \
                f"{pod['metadata']['name']}"

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
        self.account(pod)

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

        # Backward compatibility
        # Various classes will use this var to hash and compare nodes
        self.account = self._meta
        # Mimic Ducktape cluster node hostname field
        self.hostname = f"{self._spec['nodeName']}/{self.name}"

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
