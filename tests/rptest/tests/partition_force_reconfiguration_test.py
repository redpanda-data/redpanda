# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import requests
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.clients.rpk import RpkTool
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from random import shuffle
import time
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.services.admin import Replica
from rptest.clients.kcl import KCL


class PartitionForceReconfigurationTest(EndToEndTest, PartitionMovementMixin):
    """
    Tests that trigger a force partition reconfiguration to down size the
    replica count forcefully. Validates post reconfiguration state.
    """
    def __init__(self, test_context, *args, **kwargs):
        super(PartitionForceReconfigurationTest,
              self).__init__(test_context, *args, **kwargs)

    SPEC = TopicSpec(name="topic", replication_factor=5)

    def _start_redpanda(self, acks=-1):
        self.start_redpanda(
            num_nodes=7,
            extra_rp_conf={"internal_topic_replication_factor": 7})
        self.client().create_topic(self.SPEC)
        self.topic = self.SPEC.name
        # Wait for initial leader
        self.redpanda._admin.await_stable_leader(topic=self.topic,
                                                 replication=5)
        # Start a producer at the desired acks level
        self.start_producer(acks=acks)
        self.await_num_produced(min_records=10000)

    def _wait_until_no_leader(self):
        """Scrapes the debug endpoints of all replicas and checks if any of the replicas think they are the leader"""
        def no_leader():
            state = self.redpanda._admin.get_partition_state(
                "kafka", self.topic, 0)
            if "replicas" not in state.keys() or len(state["replicas"]) == 0:
                return True
            for r in state["replicas"]:
                assert "raft_state" in r.keys()
                if r["raft_state"]["is_leader"]:
                    return False
            return True

        wait_until(no_leader,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="Partition has a leader")

    def _alive_nodes(self):
        return [n.account.hostname for n in self.redpanda.started_nodes()]

    def _stop_majority_nodes(self, replication=5, conf=None):
        """
        Stops a random majority of nodes hosting partition 0 of test topic.
        """
        assert self.redpanda
        if not conf:
            conf = self.redpanda._admin._get_stable_configuration(
                hosts=self._alive_nodes(),
                topic=self.topic,
                replication=replication)
        replicas = conf.replicas
        shuffle(replicas)
        mid = len(replicas) // 2 + 1
        (killed, alive) = (replicas[0:mid], replicas[mid:])
        for replica in killed:
            node = self.redpanda.get_node_by_id(replica.node_id)
            assert node
            self.logger.debug(f"Stopping node with node_id: {replica.node_id}")
            self.redpanda.stop_node(node)
        # The partition should be leaderless.
        self._wait_until_no_leader()
        return (killed, alive)

    def _do_force_reconfiguration(self, replicas):
        try:
            self.redpanda._admin.force_set_partition_replicas(
                topic=self.topic, partition=0, replicas=replicas)
            return True
        except requests.exceptions.RetryError:
            return False
        except requests.exceptions.ConnectionError:
            return False
        except requests.exceptions.HTTPError:
            return False

    def _force_reconfiguration(self, new_replicas):
        replicas = [
            dict(node_id=replica.node_id, core=replica.core)
            for replica in new_replicas
        ]
        self.redpanda.logger.info(f"Force reconfiguring to: {replicas}")
        self.redpanda.wait_until(
            lambda: self._do_force_reconfiguration(replicas=replicas),
            timeout_sec=60,
            backoff_sec=2,
            err_msg=f"Unable to force reconfigure {self.topic}/0 to {replicas}"
        )

    def _start_consumer(self):
        self.start_consumer()
        # Wait for all consumer offsets partitions to have a stable leadership.
        # With lost nodes on debug builds, this seems to take time to converge.
        for part in range(0, 16):
            self.redpanda._admin.await_stable_leader(
                topic="__consumer_offsets",
                partition=part,
                timeout_s=30,
                backoff_s=2,
                hosts=self._alive_nodes())

    @cluster(num_nodes=9)
    @matrix(acks=[-1, 1],
            restart=[True, False],
            controller_snapshots=[True, False])
    def test_basic_reconfiguration(self, acks, restart, controller_snapshots):
        self._start_redpanda(acks=acks)

        if controller_snapshots:
            self.redpanda.set_cluster_config(
                {"controller_snapshot_max_age_sec": 1})
        else:
            self.redpanda._admin.put_feature("controller_snapshots",
                                             {"state": "disabled"})

        # Kill a majority of nodes
        (killed, alive) = self._stop_majority_nodes()

        self._force_reconfiguration(alive)
        # Leadership should be stabilized
        self.redpanda._admin.await_stable_leader(topic=self.topic,
                                                 replication=len(alive),
                                                 hosts=self._alive_nodes())

        self._start_consumer()
        if controller_snapshots:
            # Wait for few seconds to make sure snapshots
            # happen.
            time.sleep(3)
        if restart:
            reduced_replica_set_size = len(alive)
            alive_nodes_before = self._alive_nodes()
            # Restart the killed nodes
            for replica in killed:
                node = self.redpanda.get_node_by_id(replica.node_id)
                assert node
                self.logger.debug(
                    f"Restarting node with node_id: {replica.node_id}")
                self.redpanda.start_node(node=node)
            alive_nodes_after = self._alive_nodes()
            assert len(alive_nodes_before) < len(alive_nodes_after)
            # The partition should still remain with the reduced replica
            # set size even after all the nodes are started back.
            self.redpanda._admin.await_stable_leader(
                topic=self.topic,
                replication=reduced_replica_set_size,
                hosts=alive_nodes_after)
        if acks == -1:
            self.run_validation()

    @cluster(num_nodes=5)
    @matrix(controller_snapshots=[True, False])
    def test_reconfiguring_with_dead_node(self, controller_snapshots):
        self.start_redpanda(num_nodes=5)
        assert self.redpanda

        if controller_snapshots:
            self.redpanda.set_cluster_config(
                {"controller_snapshot_max_age_sec": 1})
        else:
            self.redpanda._admin.put_feature("controller_snapshots",
                                             {"state": "disabled"})

        self.topic = "topic"
        self.client().create_topic(
            TopicSpec(name=self.topic, replication_factor=3))

        # Kill majority nodes of the ntp
        (killed, alive) = self._stop_majority_nodes(replication=3)
        killed = killed[0]

        # Reconfigure with one of the killed nodes
        self._force_reconfiguration(alive + [killed])

        if controller_snapshots:
            # Wait for snapshots to catchup.
            time.sleep(3)

        # Started the killed node back up.
        self.redpanda.start_node(
            node=self.redpanda.get_node_by_id(killed.node_id))
        # New group should include the killed node.
        self.redpanda._admin.await_stable_leader(topic=self.topic,
                                                 replication=len(alive) + 1,
                                                 hosts=self._alive_nodes())

    @cluster(num_nodes=7)
    @matrix(target_replica_set_size=[1, 3])
    def test_reconfiguring_all_replicas_lost(self, target_replica_set_size):
        self.start_redpanda(num_nodes=4)
        assert self.redpanda

        # create a topic with rf = 1
        self.topic = "topic"
        self.client().create_topic(
            TopicSpec(name=self.topic, replication_factor=1))

        kcl = KCL(self.redpanda)

        # produce some data.
        self.start_producer(acks=1)
        self.await_num_produced(min_records=10000)
        self.producer.stop()

        def get_stable_lso():
            def get_lso():
                try:
                    partitions = kcl.list_offsets([self.topic])
                    if len(partitions) == 0:
                        return -1
                    return partitions[0].end_offset
                except:
                    return -1

            wait_until(
                lambda: get_lso() != -1,
                timeout_sec=30,
                backoff_sec=1,
                err_msg=
                f"Partition {self.topic}/0 couldn't achieve a stable lso")

            return get_lso()

        lso = get_stable_lso()
        assert lso >= 10001, f"Partition {self.topic}/0 has incorrect lso {lso}"

        # kill the broker hosting the replica
        (killed, alive) = self._stop_majority_nodes(replication=1)
        assert len(killed) == 1
        assert len(alive) == 0

        self._wait_until_no_leader()

        # force reconfigure to target replica set size
        assert target_replica_set_size <= len(self._alive_nodes())
        new_replicas = [
            Replica(dict(node_id=self.redpanda.node_id(replica), core=0)) for
            replica in self.redpanda.started_nodes()[:target_replica_set_size]
        ]
        self._force_reconfiguration(new_replicas=new_replicas)

        self.redpanda._admin.await_stable_leader(
            topic=self.topic,
            replication=target_replica_set_size,
            hosts=self._alive_nodes())

        # Ensure it is empty
        lso = get_stable_lso()
        assert lso == 0, f"Partition {self.topic}/0 has incorrect lso {lso}"

        # check if we can produce/consume with new replicas from a client perspective
        self.start_producer()
        self.await_num_produced(min_records=10000)
        self.start_consumer()
        self.run_validation()
