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
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster import ClusterNode

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
                 auth=None):
        self.redpanda = redpanda

        self._session = AuthPreservingSession()
        if auth is not None:
            self._session.auth = auth

        self._default_node = default_node

        # - We retry on 503s because at any time a POST to a leader-redirected
        # request will return 503 if the partition is leaderless -- this is common
        # at the very start of a test when working with the controller partition to
        # e.g. create users.
        # - We do not let urllib retry on connection errors, because we need to do our
        # own logic in _request for trying a different node in that case.
        # - If the caller wants to handle 503s directly, they can set retry_codes to []
        if retry_codes is None:
            retry_codes = [503]

        retries = Retry(status=5,
                        connect=0,
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

    def _request(self, verb, path, node=None, **kwargs):
        if node is None and self._default_node is not None:
            # We were constructed with an explicit default node: use that one
            # and do not retry on others.
            node = self._default_node
            retry_connection = False
        elif node is None:
            # Pick a random node to run this request on.  If that node gives
            # connection errors we will retry on other nodes.
            node = random.choice(self.redpanda.nodes)
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
                             dry_run=False):
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
                             }).json()

    def get_cluster_config_status(self):
        return self._request("GET", "cluster_config/status").json()

    def get_node_config(self):
        return self._request("GET", "node_config").json()

    def get_features(self):
        return self._request("GET", "features").json()

    def put_feature(self, feature_name, body):
        return self._request("PUT", f"features/{feature_name}", json=body)

    def set_log_level(self, name, level, expires=None):
        """
        Set broker log level
        """
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
                (topic is not None and partition is not None)
        assert topic or namespace is None
        namespace = namespace or "kafka"
        path = "partitions"
        if topic:
            path = f"{path}/{namespace}/{topic}/{partition}"
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
        path = f"transactions"
        return self._request('get', path, node=node).json()

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

    def create_user(self, username, password, algorithm):
        self.redpanda.logger.info(
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

    def list_users(self, node=None):
        return self._request("get", "security/users", node=node).json()

    def partition_transfer_leadership(self, namespace, topic, partition,
                                      target_id):
        path = f"partitions/{namespace}/{topic}/{partition}/transfer_leadership?target={target_id}"
        self._request("POST", path)

    def get_partition_leader(self, *, namespace, topic, partition, node=None):
        partition_info = self.get_partitions(topic=topic,
                                             partition=partition,
                                             namespace=namespace,
                                             node=node)

        return partition_info['leader_id']

    def transfer_leadership_to(self, *, namespace, topic, partition, target):
        """
        Looks up current ntp leader and transfer leadership to target node, 
        this operations is NOP when current leader is the same as target.
        If user pass None for target this function will choose next replica for new leader.
        If leadership transfer was performed this function return True
        """
        target_id = self.redpanda.idx(target) if isinstance(
            target, ClusterNode) else target

        #  check which node is current leader

        def _get_details():
            p = self.get_partitions(topic=topic,
                                    partition=partition,
                                    namespace=namespace)
            self.redpanda.logger.debug(
                f"ntp {namespace}/{topic}/{partition} details: {p}")
            return p

        def _has_leader():
            return self.get_partition_leader(
                namespace=namespace, topic=topic, partition=partition) != -1

        wait_until(_has_leader,
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Failed to establish current leader")

        details = _get_details()

        if target_id is not None:
            if details['leader_id'] == target_id:
                return False
            path = f"raft/{details['raft_group_id']}/transfer_leadership?target={target_id}"
        else:
            path = f"raft/{details['raft_group_id']}/transfer_leadership"

        leader = self.redpanda.get_node(details['leader_id'])
        ret = self._request('post', path=path, node=leader)
        return ret.status_code == 200

    def maintenance_start(self, node):
        """
        Start maintenanceing on node.
        """
        id = self.redpanda.idx(node)
        url = f"brokers/{id}/maintenance"
        self.redpanda.logger.info(
            f"Starting maintenance on node {node.name}/{id}")
        return self._request("put", url)

    def maintenance_stop(self, node):
        """
        Stop maintenanceing on node.
        """
        id = self.redpanda.idx(node)
        url = f"brokers/{id}/maintenance"
        self.redpanda.logger.info(
            f"Stopping maintenance on node {node.name}/{id}")
        return self._request("delete", url)

    def maintenance_status(self, node):
        """
        Get maintenance status of a node.
        """
        id = self.redpanda.idx(node)
        self.redpanda.logger.info(
            f"Getting maintenance status on node {node.name}/{id}")
        return self._request("get", "maintenance", node=node).json()

    def reset_leaders_info(self, node):
        """
        Reset info for leaders on node
        """
        id = self.redpanda.idx(node)
        self.redpanda.logger.info(f"Reset leaders info on {node.name}/{id}")
        url = "debug/reset_leaders"
        return self._request("post", url, node=node)

    def get_leaders_info(self, node):
        """
        Get info for leaders on node
        """
        id = self.redpanda.idx(node)
        self.redpanda.logger.info(f"Get leaders info on {node.name}/{id}")
        url = "debug/partition_leaders_table"
        return self._request("get", url, node=node).json()
