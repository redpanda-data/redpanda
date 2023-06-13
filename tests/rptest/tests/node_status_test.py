# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import requests
from enum import IntEnum

import numpy as np

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.cluster.cluster import ClusterNode

NODE_STATUS_INTERVAL = 100  # milliseconds
JITTER = 25
MAX_DELTA = NODE_STATUS_INTERVAL + JITTER


def is_live(since_last_status: int) -> bool:
    return since_last_status <= MAX_DELTA


class ConnectionStatus(IntEnum):
    ALIVE = 0
    DOWN = 1
    UNKNOWN = 2


class StatusGraph:
    """
    This class models a status graph for the Redpanda nodes in a
    cluster. It supports marking nodes available, unavailable (i.e. a
    node that has joined the cluster, but is not available) and
    unknown (i.e. a node that has not yet joined the cluster).

    The cluster connection status can be verified via the
    check_cluster_status member. It checks that the status is as
    expected between each pair of nodes via the peer_status admin
    request.
    """
    def __init__(self, redpanda):
        self.redpanda = redpanda
        nodes = self.redpanda.nodes

        self.node_to_vertex = {node: i for i, node in enumerate(nodes)}
        self.vertex_to_node = {
            i: node
            for node, i in self.node_to_vertex.items()
        }
        self.shape = (len(nodes), len(nodes))
        self.edges = np.full(shape=self.shape,
                             fill_value=ConnectionStatus.ALIVE)

    def mark_node_unavailable(self, unavailable_node: ClusterNode):
        generated_id = self.node_to_vertex[unavailable_node]
        self.edges[:, generated_id] = ConnectionStatus.DOWN
        self.edges[generated_id] = ConnectionStatus.DOWN

    def mark_node_available(self, available_node: ClusterNode):
        generated_id = self.node_to_vertex[available_node]
        self.edges[generated_id] = ConnectionStatus.ALIVE
        self.edges[:, generated_id] = ConnectionStatus.ALIVE

    def mark_node_unknwon(self, available_node: ClusterNode):
        generated_id = self.node_to_vertex[available_node]
        self.edges[generated_id] = ConnectionStatus.UNKNOWN
        self.edges[:, generated_id] = ConnectionStatus.UNKNOWN

    def is_node_available(self, node: ClusterNode):
        vertex = self.node_to_vertex[node]
        return self.edges[vertex][vertex] == ConnectionStatus.ALIVE

    def do_check_cluster_status(self):
        admin = Admin(self.redpanda)
        results = []
        for node, peer, expected_status in self._all_edges():
            if not self.is_node_available(node):
                # The starting node is unavailable so the request
                # for peer status would not get a response.
                continue

            self.redpanda.logger.debug(
                f"Checking status of peer {self.redpanda.idx(peer)} "
                f"from node {self.redpanda.idx(node)}, expected status: {expected_status}"
            )
            peer_status = self._get_peer_status(admin, node, peer)

            if expected_status == ConnectionStatus.UNKNOWN:
                results.append(peer_status is None)

            elif expected_status == ConnectionStatus.ALIVE:
                ms_since_last_status = peer_status["since_last_status"]
                is_peer_live = is_live(ms_since_last_status)
                self.redpanda.logger.debug(
                    f"Node {peer.name} expected status: alive, last status: {ms_since_last_status}, is live: {is_peer_live}"
                )
                results.append(is_peer_live)

            elif expected_status == ConnectionStatus.DOWN:
                ms_since_last_status = peer_status["since_last_status"]
                is_not_live = not is_live(ms_since_last_status)
                self.redpanda.logger.debug(
                    f"Node {peer.name} expected status: down, last status: {ms_since_last_status}, is not live: {is_not_live}"
                )
        return all(results)

    def check_cluster_status(self):
        self.redpanda.wait_until(
            self.do_check_cluster_status,
            timeout_sec=30,
            backoff_sec=2,
            err_msg=
            "Node status across cluster nodes did not reach the desired state")

    def _all_edges(self):
        for start_vertex, end_vertex in np.ndindex(self.shape):
            start_node = self.vertex_to_node[start_vertex]
            end_node = self.vertex_to_node[end_vertex]

            yield (start_node, end_node, self.edges[start_vertex, end_vertex])

    def _get_peer_status(self, admin: Admin, node: ClusterNode,
                         peer: ClusterNode):
        try:
            return admin.get_peer_status(node, self.redpanda.idx(peer))
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            if status_code != 400:
                raise e
            else:
                return None


class NodeStatusTest(RedpandaTest):
    def __init__(self, ctx):
        super().__init__(
            test_context=ctx,
            extra_rp_conf={"node_status_interval": NODE_STATUS_INTERVAL})

    def _update_max_backoff(self):
        self.redpanda.set_cluster_config(
            {"node_status_reconnect_max_backoff_ms": 5000})

    @cluster(num_nodes=3)
    def test_all_nodes_up(self):
        status_graph = StatusGraph(self.redpanda)
        status_graph.check_cluster_status()
        self._update_max_backoff()
        status_graph.check_cluster_status()

    @cluster(num_nodes=3)
    def test_node_down(self):
        status_graph = StatusGraph(self.redpanda)

        node_to_stop = random.choice(self.redpanda.nodes)
        status_graph.mark_node_unavailable(node_to_stop)

        self.redpanda.stop_node(node_to_stop)

        status_graph.check_cluster_status()


class NodeStatusStartupTest(RedpandaTest):
    def __init__(self, ctx):
        super().__init__(
            test_context=ctx,
            num_brokers=3,
            extra_rp_conf={"node_status_interval": NODE_STATUS_INTERVAL})

    def setUp(self):
        pass

    @cluster(num_nodes=3)
    def test_late_joiner(self):
        # Start the first two nodes
        self.redpanda.start(self.redpanda.nodes[0:-1])
        late_joiner = self.redpanda.nodes[-1]

        # Check the cluster status with the unavailable node
        status_graph = StatusGraph(self.redpanda)
        status_graph.mark_node_unknwon(late_joiner)
        status_graph.check_cluster_status()

        # Start the late joiner
        self.redpanda.start([late_joiner])

        # Check the cluster status again
        status_graph.mark_node_available(late_joiner)
        status_graph.check_cluster_status()
