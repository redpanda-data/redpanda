# Copyright 2021 Vectorized, Inc.
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

DEFAULT_TIMEOUT = 30


class Admin:
    """
    Wrapper for Redpanda admin REST API.

    All methods on this class will raise on errors.  For GETs the return
    value is a decoded dict of the JSON payload, for other requests
    the successful HTTP response object is returned.
    """
    def __init__(self, redpanda, default_node=None, retry_codes=None):
        self.redpanda = redpanda

        self._session = requests.Session()

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
                        method_whitelist=None)

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

    def get_config(self):
        return self._request("GET", "config").json()

    def get_node_config(self):
        return self._request("GET", "node_config").json()

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

    def partition_transfer_leadership(self, namespace, topic, partition,
                                      target_id):
        path = f"partitions/{namespace}/{topic}/{partition}/transfer_leadership?target={target_id}"
        self._request("POST", path)

    def transfer_leadership_to(self, namespace, topic, partition, target_id):
        """
        Looks up current ntp leader and transfer leadership to target node, 
        this operations is NOP when current leader is the same as target. 
        If leadership transfer was performed this function return True
        """

        #  check which node is current leader

        def _get_details():
            p = self.get_partitions(topic=topic,
                                    partition=partition,
                                    namespace=namespace)
            self.redpanda.logger.debug(
                f"ntp {namespace}/{topic}/{partition} details: {p}")
            return p

        def _has_leader():
            return _get_details()['leader_id'] != -1

        wait_until(_has_leader,
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Failed to establish current leader")

        details = _get_details()
        if details['leader_id'] == target_id:
            return False
        path = f"raft/{details['raft_group_id']}/transfer_leadership?target={target_id}"
        leader = self.redpanda.get_node(details['leader_id'])
        ret = self._request('post', path=path, node=leader)
        return ret.status_code == 200
