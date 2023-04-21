# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import json
import requests
import time
from time import sleep
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from requests.packages.urllib3.util.retry import Retry
from ducktape.cluster.cluster import ClusterNode
from typing import Optional, Callable, NamedTuple
from rptest.util import wait_until_result
from requests.exceptions import HTTPError

DEFAULT_TIMEOUT = 30


class AuthPreservingSession(requests.Session):
    """
    Override `requests` default behaviour of dropping Authorization
    headers when redirecting.  This makes sense as a general default,
    but in the case of the redpanda admin API, we trust the server to
    only redirect us to other equally privileged servers within
    the same cluster.
    """
    def should_strip_auth(self, old_url, new_url):
        return False


class Replica:
    node_id: int
    core: int

    def __init__(self, replica_dict):
        self.node_id = replica_dict["node_id"]
        self.core = replica_dict["core"]

    def __getitem__(self, item_type):
        if item_type == "node_id":
            return self.node_id
        elif item_type == "core":
            return self.core
        return None


class PartitionDetails:
    replicas: list[Replica]

    def __init__(self):
        self.replicas = []
        self.leader = None
        self.status = None


class RedpandaNode(NamedTuple):
    # host or ip address
    ip: str
    # node id in redpanda.yaml
    id: int


class Admin:
    """
    Wrapper for Redpanda admin REST API.

    All methods on this class will raise on errors.  For GETs the return
    value is a decoded dict of the JSON payload, for other requests
    the successful HTTP response object is returned.
    """
    def __init__(self,
                 redpanda,
                 default_node=None,
                 retry_codes=None,
                 auth=None,
                 retries_amount=5):
        self.redpanda = redpanda

        self._session = AuthPreservingSession()
        if auth is not None:
            self._session.auth = auth

        self._default_node: ClusterNode = default_node

        # - We retry on 503s because at any time a POST to a leader-redirected
        # request will return 503 if the partition is leaderless -- this is common
        # at the very start of a test when working with the controller partition to
        # e.g. create users.
        # - We do not let urllib retry on connection errors, because we need to do our
        # own logic in _request for trying a different node in that case.
        # - If the caller wants to handle 503s directly, they can set retry_codes to []
        if retry_codes is None:
            retry_codes = [503]

        retries = Retry(status=retries_amount,
                        connect=0,
                        read=0,
                        backoff_factor=1,
                        status_forcelist=retry_codes,
                        method_whitelist=None,
                        remove_headers_on_redirect=[])

        self._session.mount("http://", HTTPAdapter(max_retries=retries))

    @staticmethod
    def ready(node):
        url = f"http://{node.account.hostname}:9644/v1/status/ready"
        return requests.get(url).json()

    @staticmethod
    def _url(node, path):
        return f"http://{node.account.hostname}:9644/v1/{path}"

    @staticmethod
    def _equal_assignments(r0, r1):
        def to_tuple(a):
            return a["node_id"], a["core"]

        r0 = [to_tuple(a) for a in r0]
        r1 = [to_tuple(a) for a in r1]
        return set(r0) == set(r1)

    def _get_configuration(self, host, namespace, topic, partition):
        url = f"http://{host}:9644/v1/partitions/{namespace}/{topic}/{partition}"
        self.redpanda.logger.debug(f"Dispatching GET {url}")
        r = self._session.request("GET", url)
        if r.status_code != 200:
            self.redpanda.logger.warn(f"Response {r.status_code}: {r.text}")
            return None
        else:
            try:
                json = r.json()
                self.redpanda.logger.debug(f"Response OK, JSON: {json}")
                return json
            except json.decoder.JSONDecodeError as e:
                self.redpanda.logger.debug(
                    f"Response OK, Malformed JSON: '{r.text}' ({e})")
                return None

    def _get_stable_configuration(
            self,
            hosts,
            topic,
            partition=0,
            namespace="kafka",
            replication: Optional[int] = None) -> Optional[PartitionDetails]:
        """
        Method iterates through hosts and checks that the configuration of
        namespace/topic/partition is the same on all hosts and that all
        hosts see the same leader.

        When replication is provided the method also checks that the confi-
        guration has exactly that amout of nodes.

        When the configuration isn't stable the method returns None
        """
        last_leader = -1
        replicas = None
        status = None
        for host in hosts:
            self.redpanda.logger.debug(
                f"requesting \"{namespace}/{topic}/{partition}\" details from {host})"
            )
            meta = self._get_configuration(host, namespace, topic, partition)
            if meta == None:
                return None
            if "replicas" not in meta:
                self.redpanda.logger.debug(f"replicas are missing")
                return None
            if "status" not in meta:
                self.redpanda.logger.debug(f"status is missing")
                return None
            if status == None:
                status = meta["status"]
                self.redpanda.logger.debug(f"get status:{status}")
            if status != meta["status"]:
                self.redpanda.logger.debug(
                    f"get status:{meta['status']} while already observed:{status} before"
                )
                return None
            read_replicas = meta["replicas"]
            if replicas is None:
                replicas = read_replicas
                self.redpanda.logger.debug(
                    f"get replicas:{read_replicas} from {host}")
            elif not self._equal_assignments(replicas, read_replicas):
                self.redpanda.logger.debug(
                    f"get conflicting replicas:{read_replicas} from {host}")
                return None
            if replication != None:
                if len(meta["replicas"]) != replication:
                    self.redpanda.logger.debug(
                        f"expected replication:{replication} got:{len(meta['replicas'])}"
                    )
                    return None
            if meta["leader_id"] < 0:
                self.redpanda.logger.debug(f"doesn't have leader")
                return None
            if last_leader < 0:
                last_leader = int(meta["leader_id"])
                self.redpanda.logger.debug(f"get leader:{last_leader}")
            if last_leader not in [n["node_id"] for n in replicas]:
                self.redpanda.logger.debug(
                    f"leader:{last_leader} isn't in the replica set")
                return None
            if last_leader != meta["leader_id"]:
                self.redpanda.logger.debug(
                    f"got leader:{meta['leader_id']} but observed {last_leader} before"
                )
                return None
        info = PartitionDetails()
        info.status = status
        info.leader = int(last_leader)
        info.replicas = [Replica(r) for r in replicas]
        return info

    def wait_stable_configuration(
            self,
            topic,
            *,
            partition=0,
            namespace="kafka",
            replication=None,
            timeout_s=10,
            backoff_s=1,
            hosts: Optional[list[str]] = None) -> PartitionDetails:
        """
        Method waits for timeout_s until the configuration is stable and returns it.

        When the timeout is exhaust it throws TimeoutException
        """
        if hosts == None:
            hosts = [n.account.hostname for n in self.redpanda.nodes]
        hosts = list(hosts)

        def get_stable_configuration():
            random.shuffle(hosts)
            msg = ",".join(hosts)
            self.redpanda.logger.debug(
                f"wait details for {namespace}/{topic}/{partition} from nodes: {msg}"
            )
            try:
                info = self._get_stable_configuration(hosts,
                                                      topic,
                                                      partition=partition,
                                                      namespace=namespace,
                                                      replication=replication)
                if info == None:
                    return False
                return True, info
            except RequestException:
                self.redpanda.logger.exception(
                    "an error on getting stable configuration, retrying")
                return False

        return wait_until_result(
            get_stable_configuration,
            timeout_sec=timeout_s,
            backoff_sec=backoff_s,
            err_msg=
            f"can't fetch stable replicas for {namespace}/{topic}/{partition} within {timeout_s} sec"
        )

    def await_stable_leader(self,
                            topic,
                            partition=0,
                            namespace="kafka",
                            replication=None,
                            timeout_s=10,
                            backoff_s=1,
                            hosts: Optional[list[str]] = None,
                            check: Callable[[int],
                                            bool] = lambda node_id: True):
        """
        Method waits for timeout_s until the configuration is stable and check
        predicate returns true when invoked on the configuration's leader.

        When the timeout is exhaust it throws TimeoutException
        """
        def is_leader_stable():
            info = self.wait_stable_configuration(topic,
                                                  partition=partition,
                                                  namespace=namespace,
                                                  replication=replication,
                                                  timeout_s=timeout_s,
                                                  hosts=hosts,
                                                  backoff_s=backoff_s)
            if check(info.leader):
                return True, info.leader
            return False

        return wait_until_result(
            is_leader_stable,
            timeout_sec=timeout_s,
            backoff_sec=backoff_s,
            err_msg=
            f"can't get stable leader of {namespace}/{topic}/{partition} within {timeout_s} sec"
        )

    def _request(self, verb, path, node=None, **kwargs):
        if node is None and self._default_node is not None:
            # We were constructed with an explicit default node: use that one
            # and do not retry on others.
            node = self._default_node
            retry_connection = False
        elif node is None:
            # Pick a random node to run this request on.  If that node gives
            # connection errors we will retry on other nodes.
            node = random.choice(self.redpanda.started_nodes())
            retry_connection = True
        else:
            # We were called with a specific node to run on -- do no retry on
            # other nodes.
            retry_connection = False

        if kwargs.get('timeout', None) is None:
            kwargs['timeout'] = DEFAULT_TIMEOUT

        fallback_nodes = self.redpanda.nodes
        fallback_nodes = list(filter(lambda n: n != node, fallback_nodes))

        # On connection errors, retry until we run out of alternative nodes to try
        # (fall through on first successful request)
        while True:
            url = self._url(node, path)
            self.redpanda.logger.debug(f"Dispatching {verb} {url}")
            try:
                r = self._session.request(verb, url, **kwargs)
            except requests.ConnectionError:
                if retry_connection and fallback_nodes:
                    node = random.choice(fallback_nodes)
                    fallback_nodes = list(
                        filter(lambda n: n != node, fallback_nodes))
                    self.redpanda.logger.info(
                        f"Connection error, retrying on node {node.account.hostname} (remaining {[n.account.hostname for n in fallback_nodes]})"
                    )
                else:
                    raise
            else:
                break

        # Log the response
        if r.status_code != 200:
            self.redpanda.logger.warn(f"Response {r.status_code}: {r.text}")
        else:
            if 'application/json' in r.headers.get('Content-Type') and len(
                    r.text):
                try:
                    self.redpanda.logger.debug(
                        f"Response OK, JSON: {r.json()}")
                except json.decoder.JSONDecodeError as e:
                    self.redpanda.logger.debug(
                        f"Response OK, Malformed JSON: '{r.text}' ({e})")
            else:
                self.redpanda.logger.debug("Response OK")

        r.raise_for_status()
        return r

    def get_status_ready(self, node=None):
        return self._request("GET", "status/ready", node=node).json()

    def get_cluster_config(self, node=None, include_defaults=None):
        if include_defaults is not None:
            kwargs = {"params": {"include_defaults": include_defaults}}
        else:
            kwargs = {}

        return self._request("GET", "cluster_config", node=node,
                             **kwargs).json()

    def get_cluster_config_schema(self, node=None):
        return self._request("GET", "cluster_config/schema", node=node).json()

    def patch_cluster_config(self,
                             upsert=None,
                             remove=None,
                             force=False,
                             dry_run=False,
                             node=None):
        if upsert is None:
            upsert = {}
        if remove is None:
            remove = []

        path = "cluster_config"
        params = {}
        if force:
            params['force'] = 'true'
        if dry_run:
            params['dry_run'] = 'true'

        if params:
            joined = "&".join([f"{k}={v}" for k, v in params.items()])
            path = path + f"?{joined}"

        return self._request("PUT",
                             path,
                             json={
                                 'upsert': upsert,
                                 'remove': remove
                             },
                             node=node).json()

    def get_cluster_config_status(self, node: ClusterNode = None):
        return self._request("GET", "cluster_config/status", node=node).json()

    def get_node_config(self, node=None):
        return self._request("GET", "node_config", node).json()

    def get_features(self, node=None):
        return self._request("GET", "features", node=node).json()

    def supports_feature(self, feature_name: str, nodes=None):
        """
        Returns true whether all nodes in 'nodes' support the given feature. If
        no nodes are supplied, uses all nodes in the cluster.
        """
        if not nodes:
            nodes = self.redpanda.nodes

        def node_supports_feature(node):
            features_resp = None
            try:
                features_resp = self.get_features(node=node)
            except RequestException as e:
                self.redpanda.logger.debug(
                    f"Could not get features on {node.account.hostname}: {e}")
                return False
            features_dict = dict(
                (f["name"], f) for f in features_resp["features"])
            return features_dict[feature_name]["state"] == "active"

        for node in nodes:
            if not node_supports_feature(node):
                return False
        return True

    def put_feature(self, feature_name, body):
        return self._request("PUT", f"features/{feature_name}", json=body)

    def get_license(self, node=None):
        return self._request("GET", "features/license", node=node).json()

    def put_license(self, license):
        return self._request("PUT", "features/license", data=license)

    def get_loggers(self, node):
        """
        Get the names of all loggers.
        """
        return [
            l["name"]
            for l in self._request("GET", "loggers", node=node).json()
        ]

    def set_log_level(self, name, level, expires=None):
        """
        Set broker log level
        """
        name = name.replace("/", "%2F")
        for node in self.redpanda.nodes:
            path = f"config/log_level/{name}?level={level}"
            if expires:
                path = f"{path}&expires={expires}"
            self._request('put', path, node=node)

    def get_brokers(self, node=None):
        """
        Return metadata about brokers.
        """
        return self._request('get', "brokers", node=node).json()

    def get_broker(self, id, node=None):
        """
        Return metadata about a broker.

        This differs from the `brokers/` endpoint in that it may contain
        additional context beyond what is reported for through `brokers/`.
        """
        return self._request('get', f"brokers/{id}", node=node).json()

    def get_cluster_view(self, node):
        """
        Return cluster_view.
        """
        return self._request('get', "cluster_view", node=node).json()

    def get_cluster_health_overview(self, node=None):

        return self._request('get', "cluster/health_overview",
                             node=node).json()

    def decommission_broker(self, id, node=None):
        """
        Decommission broker
        """
        path = f"brokers/{id}/decommission"
        self.redpanda.logger.debug(f"decommissioning {path}")
        return self._request('put', path, node=node)

    def get_decommission_status(self, id, node=None):
        """
        Get broker decommission status
        """
        path = f"brokers/{id}/decommission"
        return self._request('get', path, node=node).json()

    def recommission_broker(self, id, node=None):
        """
        Recommission broker i.e. abort ongoing decommissioning
        """
        path = f"brokers/{id}/recommission"
        self.redpanda.logger.debug(f"recommissioning {id}")
        return self._request('put', path, node=node)

    def trigger_rebalance(self, node=None):
        """
        Trigger on demand partitions rebalancing
        """
        path = f"partitions/rebalance"

        return self._request('post', path, node=node)

    def list_reconfigurations(self, node=None):
        """
        List pending reconfigurations
        """
        path = f"partitions/reconfigurations"

        return self._request('get', path, node=node).json()

    def cancel_all_reconfigurations(self, node=None):
        """
        Cancel all pending reconfigurations
        """
        path = "cluster/cancel_reconfigurations"

        return self._request('post', path, node=node).json()

    def cancel_all_node_reconfigurations(self, target_id, node=None):
        """
        Cancel all reconfigurations moving partition replicas from node
        """
        path = f"brokers/{target_id}/cancel_partition_moves"

        return self._request('post', path, node=node).json()

    def get_partitions(self,
                       topic=None,
                       partition=None,
                       *,
                       namespace=None,
                       node=None):
        """
        Return partition metadata from controller. This includes low-level
        information like replica set assignments with core affinities.
        """
        assert (topic is None and partition is None) or \
                (topic is not None)

        namespace = namespace or "kafka"
        path = "partitions"
        if topic:
            path = f"{path}/{namespace}/{topic}"

        if partition is not None:
            path = f"{path}/{partition}"

        return self._request('get', path, node=node).json()

    def get_transactions(self, topic, partition, namespace, node=None):
        """
        Get transaction for current partition
        """
        path = f"partitions/{namespace}/{topic}/{partition}/transactions"
        return self._request('get', path, node=node).json()

    def get_all_transactions(self, node=None):
        """
        Get all transactions
        """
        cluster_config = self.get_cluster_config(include_defaults=True)
        tm_partition_amount = cluster_config[
            "transaction_coordinator_partitions"]
        result = []
        for partition in range(tm_partition_amount):
            self.await_stable_leader(topic="tx",
                                     namespace="kafka_internal",
                                     partition=partition)
            path = f"transactions?coordinator_partition_id={partition}"
            partition_res = self._request('get', path, node=node).json()
            result.extend(partition_res)
        return result

    def mark_transaction_expired(self,
                                 topic,
                                 partition,
                                 pid,
                                 namespace,
                                 node=None):
        """
        Mark transaction expired for partition
        """
        path = f"partitions/{namespace}/{topic}/{partition}/mark_transaction_expired?id={pid['id']}&epoch={pid['epoch']}"
        return self._request("post", path, node=node)

    def delete_partition_from_transaction(self,
                                          tid,
                                          namespace,
                                          topic,
                                          partition_id,
                                          etag,
                                          node=None):
        """
        Delete partition from transaction
        """
        partition_info = {
            "namespace": namespace,
            "topic": topic,
            "partition_id": partition_id,
            "etag": etag
        }

        params = "&".join([f"{k}={v}" for k, v in partition_info.items()])
        path = f"transaction/{tid}/delete_partition/?{params}"
        return self._request('post', path, node=node)

    def set_partition_replicas(self,
                               topic,
                               partition,
                               replicas,
                               *,
                               namespace="kafka",
                               node=None):
        """
        [ {"node_id": 0, "core": 1}, ... ]
        """
        path = f"partitions/{namespace}/{topic}/{partition}/replicas"
        return self._request('post', path, node=node, json=replicas)

    def cancel_partition_move(self,
                              topic,
                              partition,
                              namespace="kafka",
                              node=None):

        path = f"partitions/{namespace}/{topic}/{partition}/cancel_reconfiguration"
        return self._request('post', path, node=node)

    def force_abort_partition_move(self,
                                   topic,
                                   partition,
                                   namespace="kafka",
                                   node=None):

        path = f"partitions/{namespace}/{topic}/{partition}/unclean_abort_reconfiguration"
        return self._request('post', path, node=node)

    def create_user(self, username, password, algorithm):
        self.redpanda.logger.debug(
            f"Creating user {username}:{password}:{algorithm}")

        path = f"security/users"

        self._request("POST",
                      path,
                      json=dict(
                          username=username,
                          password=password,
                          algorithm=algorithm,
                      ))

    def delete_user(self, username):
        self.redpanda.logger.info(f"Deleting user {username}")

        path = f"security/users/{username}"

        self._request("delete", path)

    def update_user(self, username, password, algorithm):
        self.redpanda.logger.info(
            f"Updating user {username}:{password}:{algorithm}")

        self._request("PUT",
                      f"security/users/{username}",
                      json=dict(
                          username=username,
                          password=password,
                          algorithm=algorithm,
                      ))

    def list_users(self, node=None, include_ephemeral: Optional[bool] = False):
        params = None if include_ephemeral is None else {
            "include_ephemeral": f"{include_ephemeral}".lower()
        }
        return self._request("get", "security/users", node=node,
                             params=params).json()

    def partition_transfer_leadership(self,
                                      namespace,
                                      topic,
                                      partition,
                                      target_id=None):
        path = f"partitions/{namespace}/{topic}/{partition}/transfer_leadership"
        if target_id:
            path += f"?target={target_id}"

        self._request("POST", path)

    def get_partition_leader(self, *, namespace, topic, partition, node=None):
        partition_info = self.get_partitions(topic=topic,
                                             partition=partition,
                                             namespace=namespace,
                                             node=node)

        return partition_info['leader_id']

    def transfer_leadership_to(self,
                               *,
                               namespace,
                               topic,
                               partition,
                               target_id=None,
                               leader_id=None):
        """
        Looks up current ntp leader and transfer leadership to target node,
        this operations is NOP when current leader is the same as target.
        If user pass None for target this function will choose next replica for new leader.
        Returns true if leadership was transferred to the target node.
        """
        def _get_details():
            p = self.get_partitions(topic=topic,
                                    partition=partition,
                                    namespace=namespace)
            self.redpanda.logger.debug(
                f"ntp {namespace}/{topic}/{partition} details: {p}")
            return p

        #  check which node is current leader

        if leader_id == None:
            leader_id = self.await_stable_leader(topic,
                                                 partition=partition,
                                                 namespace=namespace,
                                                 timeout_s=30,
                                                 backoff_s=2)

        details = _get_details()

        if target_id is not None:
            if leader_id == target_id:
                return True
            path = f"raft/{details['raft_group_id']}/transfer_leadership?target={target_id}"
        else:
            path = f"raft/{details['raft_group_id']}/transfer_leadership"

        leader = self.redpanda.get_node(leader_id)
        ret = self._request('post', path=path, node=leader)
        return ret.status_code == 200

    def maintenance_start(self, node, dst_node=None):
        """
        Start maintenanceing on 'node', sending the request to 'dst_node'.
        """
        id = self.redpanda.node_id(node)
        url = f"brokers/{id}/maintenance"
        self.redpanda.logger.info(
            f"Starting maintenance on node {node.name}/{id}")
        return self._request("put", url, node=dst_node)

    def maintenance_stop(self, node, dst_node=None):
        """
        Stop maintenanceing on 'node', sending the request to 'dst_node'.
        """
        id = self.redpanda.node_id(node)
        url = f"brokers/{id}/maintenance"
        self.redpanda.logger.info(
            f"Stopping maintenance on node {node.name}/{id}")
        return self._request("delete", url, node=dst_node)

    def maintenance_status(self, node):
        """
        Get maintenance status of a node.
        """
        id = self.redpanda.node_id(node)
        self.redpanda.logger.info(
            f"Getting maintenance status on node {node.name}/{id}")
        return self._request("get", "maintenance", node=node).json()

    def reset_leaders_info(self, node):
        """
        Reset info for leaders on node
        """
        id = self.redpanda.node_id(node)
        self.redpanda.logger.info(f"Reset leaders info on {node.name}/{id}")
        url = "debug/reset_leaders"
        return self._request("post", url, node=node)

    def get_leaders_info(self, node=None):
        """
        Get info for leaders on node
        """
        if node:
            id = self.redpanda.node_id(node)
            self.redpanda.logger.info(f"Get leaders info on {node.name}/{id}")
        else:
            self.redpanda.logger.info(f"Get leaders info on any node")

        url = "debug/partition_leaders_table"
        return self._request("get", url, node=node).json()

    def si_sync_local_state(self, topic, partition, node=None):
        """
        Check data in the S3 bucket and fix local index if needed
        """
        path = f"cloud_storage/sync_local_state/{topic}/{partition}"
        return self._request('post', path, node=node)

    def get_partition_balancer_status(self, node=None, **kwargs):
        return self._request("GET",
                             "cluster/partition_balancer/status",
                             node=node,
                             **kwargs).json()

    def get_peer_status(self, node, peer_id):
        return self._request("GET", f"debug/peer_status/{peer_id}",
                             node=node).json()

    def get_controller_status(self, node):
        return self._request("GET", f"debug/controller_status",
                             node=node).json()

    def get_cluster_uuid(self, node):
        try:
            r = self._request("GET", "cluster/uuid", node=node)
        except HTTPError as ex:
            if ex.response.status_code == 404:
                return
            raise
        if len(r.text) > 0:
            return r.json()["cluster_uuid"]

    def initiate_topic_scan_and_recovery(self,
                                         payload: Optional[dict] = None,
                                         force_acquire_lock: bool = False,
                                         node=None,
                                         **kwargs):
        request_args = {'node': node, **kwargs}

        if payload:
            request_args['json'] = payload
        return self._request('post', "cloud_storage/automated_recovery",
                             **request_args)

    def get_topic_recovery_status(self, node=None, **kwargs):
        request_args = {'node': node, **kwargs}
        return self._request('get',
                             "cloud_storage/automated_recovery?extended=true",
                             **request_args)

    def self_test_start(self, options):
        return self._request("POST", "debug/self_test/start", json=options)

    def self_test_stop(self):
        return self._request("POST", "debug/self_test/stop")

    def self_test_status(self):
        return self._request("GET", "debug/self_test/status").json()

    def restart_service(self,
                        rp_service: Optional[str] = None,
                        node: Optional[ClusterNode] = None):
        service_param = f"service={rp_service if rp_service is not None else ''}"
        return self._request("PUT",
                             f"debug/restart_service?{service_param}",
                             node=node)

    def is_node_isolated(self, node):
        return self._request("GET", "debug/is_node_isolated", node=node).json()

    def cloud_storage_usage(self) -> int:
        return int(
            self._request(
                "GET", "debug/cloud_storage_usage?retries_allowed=10").json())

    def get_usage(self, node, include_open: bool = True):
        return self._request("GET",
                             f"usage?include_open_bucket={str(include_open)}",
                             node=node).json()

    def refresh_disk_health_info(self, node: Optional[ClusterNode] = None):
        """
        Reset info for cluster health on node
        """
        return self._request("post",
                             "debug/refresh_disk_health_info",
                             node=node)

    def get_partition_cloud_storage_status(self, topic, partition, node=None):
        return self._request("GET",
                             f"cloud_storage/status/{topic}/{partition}",
                             node=node).json()

    def get_partition_manifest(self, topic: str, partition: int):
        """
        Get the in-memory partition manifest for the requested ntp
        """
        return self._request(
            "GET", f"cloud_storage/manifest/{topic}/{partition}").json()

    def get_partition_state(self, namespace, topic, partition, node=None):
        path = f"debug/partition/{namespace}/{topic}/{partition}"
        return self._request("GET", path, node=node).json()
