# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import requests
from ducktape.utils.util import wait_until


class Admin:
    def __init__(self, redpanda):
        self.redpanda = redpanda

    @staticmethod
    def ready(node):
        url = f"http://{node.account.hostname}:9644/v1/status/ready"
        return requests.get(url).json()

    @staticmethod
    def _url(node, path):
        return f"http://{node.account.hostname}:9644/v1/{path}"

    def _request(self, verb, path, node=None, **kwargs):
        if node is None:
            node = self.redpanda.controller()
        r = requests.request(verb, self._url(node, path), **kwargs)
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

    def set_log_level(self, name, level, expires=None):
        """
        Set broker log level
        """
        for node in self.redpanda.nodes:
            path = f"config/log_level/{name}?level={level}"
            if expires:
                path = f"{path}&expires={expires}"
            url = self._url(node, path)
            ret = requests.put(url)
            self.redpanda.logger.debug(ret)

    def get_brokers(self, node=None):
        """
        Return metadata about brokers.
        """
        node = node or self.redpanda.controller()
        url = self._url(node, f"brokers")
        ret = requests.get(url).json()
        self.redpanda.logger.debug(ret)
        return ret

    def decommission_broker(self, id, node=None):
        """
        Decommission broker
        """
        node = node or self.redpanda.controller()
        url = self._url(node, f"brokers/{id}/decommission")
        self.redpanda.logger.debug(f"decommissioning URL: {url}")
        ret = requests.put(url)
        self.redpanda.logger.debug(f"decommissioning response: {ret}")
        return ret

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
        node = node or self.redpanda.controller()
        url = self._url(node, f"partitions")
        if topic:
            url = f"{url}/{namespace}/{topic}/{partition}"
        return requests.get(url).json()

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
        node = node or self.redpanda.controller()
        url = self._url(
            node, f"partitions/{namespace}/{topic}/{partition}/replicas")
        ret = requests.post(url, json=replicas)
        self.redpanda.logger.debug(ret)
        return ret

    def create_user(self, username, password, algorithm):
        self.redpanda.logger.info(
            f"Creating user {username}:{password}:{algorithm}")

        path = f"v1/security/users"

        def handle(node):
            url = f"http://{node.account.hostname}:9644/{path}"
            data = dict(
                username=username,
                password=password,
                algorithm=algorithm,
            )
            reply = requests.post(url, json=data)
            self.redpanda.logger.debug(f"{reply.status_code} {reply.text}")
            return reply.status_code == 200

        self._request_to_any(handle)

    def delete_user(self, username):
        self.redpanda.logger.info(f"Deleting user {username}")

        path = f"v1/security/users/{username}"

        def handle(node):
            url = f"http://{node.account.hostname}:9644/{path}"
            reply = requests.delete(url)
            return reply.status_code == 200

        self._request_to_any(handle)

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

    def _request_to_any(self, handler):
        def try_send():
            nodes = [n for n in self.redpanda.started_nodes()]
            random.shuffle(nodes)
            for node in nodes:
                if handler(node):
                    return True

        wait_until(try_send,
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Failed to complete request")
