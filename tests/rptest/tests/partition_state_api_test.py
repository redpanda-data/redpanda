# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec


class PartitionStateAPItest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, num_brokers=5)

    def _validate_states(self, states, num_replicas, has_leader=True):
        """Does some sanity checks on partition states from all nodes"""

        for s in states:
            assert len(s["replicas"]) == num_replicas
            self.logger.debug(f"validing state {s}")
            leaders = list(
                filter(lambda r: r["raft_state"]["is_elected_leader"],
                       s["replicas"]))
            stms = list(
                filter(lambda r: r["raft_state"]["stms"], s["replicas"]))
            assert all(map(lambda stm: len(stm) > 0, stms)), stms
            assert (has_leader
                    and len(leaders) == 1) or (not has_leader
                                               and len(leaders) == 0), leaders
            if has_leader:
                # Verify the leader reports followers
                leader = leaders[0]["raft_state"]
                assert "followers" in leader.keys() and len(
                    leader["followers"]) > 0, leader

    def _has_stable_leadership(self, no_leader=False):
        """Checks if all nodes report same leadership information. When no_leader is set ensures
        that no node reports a leader for the partition."""
        all_leaders = []
        nodes = self.redpanda.started_nodes()
        for node in nodes:
            p_state = self.redpanda._admin.get_partition_state(
                "kafka", self.topic, 0, node)
            replicas = p_state["replicas"]
            leaders = list(
                filter(lambda r: r["raft_state"]["is_elected_leader"],
                       replicas))
            assert len(leaders) <= 1
            if leaders:
                all_leaders.append(leaders[0]['raft_state']['node_id'])
        if no_leader:
            # No node reports a leader
            return len(all_leaders) == 0
        # Check all nodes report the leader and there is only one unique leader
        return len(all_leaders) == len(nodes) and len(set(all_leaders)) == 1

    def _wait_for_stable_leader(self):
        self.redpanda.wait_until(self._has_stable_leadership,
                                 timeout_sec=30,
                                 backoff_sec=2)

    def _wait_for_no_leader(self):
        self.redpanda.wait_until(
            lambda: self._has_stable_leadership(no_leader=True),
            timeout_sec=30,
            backoff_sec=2)

    def _get_partition_state(self):
        nodes = self.redpanda.started_nodes()
        admin = self.redpanda._admin
        return [
            admin.get_partition_state("kafka", self.topic, 0, n) for n in nodes
        ]

    def _stop_first_replica_node(self, states):
        # Node id of first replica of first state.
        node_id = states[0]["replicas"][0]["raft_state"]["node_id"]
        self.redpanda.logger.debug(f"Stopping node: {node_id}")
        node = self.redpanda.get_node_by_id(node_id)
        self.redpanda.stop_node(node)

    @cluster(num_nodes=5)
    def test_partition_state(self):
        num_replicas = 3
        self.topics = [
            TopicSpec(partition_count=1, replication_factor=num_replicas)
        ]
        self._create_initial_topics()

        self._wait_for_stable_leader()
        # Validate ntp state from each node.
        states = self._get_partition_state()
        self._validate_states(states, num_replicas, has_leader=True)

        # kill a replica, validate leader
        self._stop_first_replica_node(states)
        self._wait_for_stable_leader()
        states = self._get_partition_state()
        self._validate_states(states, num_replicas - 1, has_leader=True)

        # kill another replica, no leader.
        self._stop_first_replica_node(states)
        self._wait_for_no_leader()
        states = self._get_partition_state()
        self._validate_states(states, num_replicas - 2, has_leader=False)

    @cluster(num_nodes=5)
    def test_controller_partition_state(self):

        admin = Admin(self.redpanda)
        controller_state = [
            admin.get_partition_state("redpanda", "controller", 0, n)
            for n in self.redpanda.started_nodes()
        ]

        for s in controller_state:
            assert len(s["replicas"]) == 5
            self.logger.debug(f"validating controller_state")
            leaders = list(
                filter(lambda r: r["raft_state"]["is_elected_leader"],
                       s["replicas"]))

            assert len(leaders) == 1
            leader_state = leaders[0]["raft_state"]
            assert "followers" in leader_state.keys() and len(
                leader_state["followers"]) == 4
