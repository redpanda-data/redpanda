# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from dataclasses import dataclass
from enum import Enum
from random import shuffle
from rptest.clients.kcl import KCL
import threading
from typing import Any, Optional

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import PartitionDetails, Replica
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.node_operations import NodeDecommissionWaiter
from rptest.tests.partition_movement import PartitionMovementMixin

from rptest.util import wait_until_result

from connectrpc.unary import UnaryOutput
from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.admin.proto.redpanda.core.admin.internal.v1 import (
    breakglass_pb2,
    breakglass_pb2_connect,
)


@dataclass
class NTP:
    namespace: str = "kafka"
    topic: str = "topic"
    partition: int = 0


@dataclass
class TimeoutConfig:
    timeout_s: int
    backoff_s: int


class Scenario(str, Enum):
    """Only simple operates for now, when nodewise recovery supports moving inprogress moves the others can be reactivated"""

    Simple = "Simple"
    Decommission = "Decommission"
    RandomMoves = "RandomMoves"


really_short_timeout = TimeoutConfig(timeout_s=5, backoff_s=1)
short_timeout = TimeoutConfig(timeout_s=30, backoff_s=2)
medium_timeout = TimeoutConfig(timeout_s=60, backoff_s=2)
long_timeout = TimeoutConfig(timeout_s=120, backoff_s=10)
really_long_timeout = TimeoutConfig(timeout_s=300, backoff_s=10)


class ControllerForceReconfigurationTestBase(RedpandaTest):
    def __init__(
        self, test_context: TestContext, cluster_size: int, *args: Any, **kwargs: Any
    ):
        super(ControllerForceReconfigurationTestBase, self).__init__(
            test_context,
            num_brokers=cluster_size,
            *args,
            **kwargs,
        )
        self.next_node_id = cluster_size + 1

    def _next_node_id(self) -> int:
        """this test kills nodes, cleans them, then reboots with a new node_id, keep track of the node id"""
        next = self.next_node_id
        self.next_node_id += 1
        return next

    def setUp(self):
        """rp will be custom started in each test"""
        pass

    def _start_redpanda(self, cluster_size: int) -> list[ClusterNode]:
        """start redpanda with a specific cluster size"""
        seed_nodes = self.redpanda.nodes[0:cluster_size]
        joiner_nodes = self.redpanda.nodes[cluster_size:]

        self.redpanda.set_seed_servers(seed_nodes)

        """Controller force reconfiguration does not guarantee that internal topics are safe from data loss
           but data loss on these topics does make it really hard to create any produce/consume test that
           passes. We are enforcing no data loss on internal topics for ease of testing."""
        self.redpanda.add_extra_rp_conf(
            {"internal_topic_replication_factor": cluster_size}
        )
        self.redpanda.start(nodes=seed_nodes, omit_seeds_on_idx_one=False)
        return joiner_nodes

    def _setup_topic(self, topic_spec: TopicSpec, timeout: TimeoutConfig):
        """start a topic given the spec"""
        self.client().create_topic(topic_spec)
        # Wait for initial leader
        self.redpanda._admin.await_stable_leader(
            topic=topic_spec.name,
            replication=topic_spec.replication_factor,
            timeout_s=timeout.timeout_s,
            backoff_s=timeout.backoff_s,
        )

    def _living_nodes(self) -> list[ClusterNode]:
        return self.redpanda.started_nodes()

    def _living_hostnames(self) -> list[str]:
        hostnames: list[str] = []
        node: ClusterNode
        for node in self.redpanda.started_nodes():
            hostname = node.account.hostname
            assert hostname is not None
            hostnames.append(hostname)
        return hostnames

    def _wait_until_no_leader(self, ntp: NTP, timeout: TimeoutConfig):
        """Scrapes the debug endpoints of all replicas and checks if any of the replicas think they are the leader"""

        def no_leader():
            living_nodes = self._living_nodes()
            for living_node in living_nodes:
                state = self.redpanda._admin.get_partition_state(
                    ntp.namespace, ntp.topic, ntp.partition, node=living_node
                )
                if "replicas" not in state.keys() or len(state["replicas"]) == 0:
                    continue
                for r in state["replicas"]:
                    assert "raft_state" in r.keys()
                    if r["raft_state"]["is_leader"]:
                        return False
            return True

        wait_until(
            no_leader,
            timeout_sec=timeout.timeout_s,
            backoff_sec=timeout.backoff_s,
            err_msg="Partition has a leader",
        )

    def _split_cluster(
        self, ntp: NTP, timeout: TimeoutConfig, replication: int = 5
    ) -> tuple[list[Replica], list[Replica]]:
        """
        Splits the cluster into nodes to kill and nodes to survive
        """
        assert self.redpanda

        def _get_details() -> tuple[bool, Optional[PartitionDetails]]:
            d = self.redpanda._admin._get_stable_configuration(
                hosts=self._living_hostnames(),
                namespace=ntp.namespace,
                topic=ntp.topic,
                partition=ntp.partition,
                replication=replication,
            )
            if d is None:
                return (False, None)
            return (True, d)

        partition_details: PartitionDetails = wait_until_result(
            _get_details, timeout_sec=timeout.timeout_s, backoff_sec=timeout.backoff_s
        )

        replicas = partition_details.replicas
        shuffle(replicas)
        mid = len(replicas) // 2 + 1
        (to_kill, to_survive) = (replicas[0:mid], replicas[mid:])
        return (to_kill, to_survive)

    def _do_stop_nodes(self, ntp: NTP, to_kill: list[Replica], timeout: TimeoutConfig):
        """ingests the output of _split_cluster, actually stops those nodes"""
        for replica in to_kill:
            node = self.redpanda.get_node_by_id(replica.node_id)
            assert node
            self.logger.debug(f"Stopping node with node_id: {replica.node_id}")
            self.redpanda.stop_node(node)
        # The partition should be leaderless.
        self._wait_until_no_leader(ntp=ntp, timeout=timeout)

    def _stop_majority_nodes(
        self, ntp: NTP, timeout: TimeoutConfig, replication: int = 5
    ) -> tuple[list[Replica], list[Replica]]:
        """chains together the above two, split the cluster then kill the majority"""
        killed, alive = self._split_cluster(
            ntp=ntp, timeout=timeout, replication=replication
        )
        self._do_stop_nodes(ntp=ntp, to_kill=killed, timeout=timeout)
        return (killed, alive)

    def _toggle_recovery_mode(
        self, node: ClusterNode, timeout: TimeoutConfig, recovery_mode_enabled: bool
    ):
        """reboot a node with recovery mode set accordingly"""
        self.redpanda.nodes
        self.logger.info(f"stopping node: {node.name}")
        self.redpanda.stop_node(node, timeout=timeout.timeout_s)

        self.logger.info(f"restarting node: {node.name}")
        self.redpanda.start_node(
            node,
            timeout=timeout.timeout_s,
            auto_assign_node_id=True,
            override_cfg_params={"recovery_mode_enabled": recovery_mode_enabled},
        )

    def _bulk_toggle_recovery_mode(
        self,
        nodes: list[ClusterNode],
        timeout: TimeoutConfig,
        recovery_mode_enabled: bool,
    ):
        """toggle recovery mode on all provided nodes"""
        for node in nodes:
            self._toggle_recovery_mode(node, timeout, recovery_mode_enabled)

    def _do_request(
        self,
        client: breakglass_pb2_connect.BreakglassServiceClient,
        request: breakglass_pb2.ControllerForcedReconfigurationRequest,
    ) -> UnaryOutput[breakglass_pb2.ControllerForcedReconfigurationResponse]:
        """helper method to do a cfr request, handles the typing concerns"""
        return client.call_controller_forced_reconfiguration(request)

    def _join_new_node(self, joiner_node: ClusterNode) -> int:
        """joins a given cluster node with a new node id"""
        self.redpanda.logger.debug(f"joining {joiner_node.name=}")
        self.redpanda.clean_node(
            joiner_node, preserve_logs=True, preserve_current_install=True
        )
        joiner_node_id = self._next_node_id()
        self.redpanda.logger.debug(f"assigned {joiner_node_id=} to {joiner_node.name=}")
        self.redpanda.start_node(
            joiner_node,
            auto_assign_node_id=False,
            node_id_override=joiner_node_id,
            omit_seeds_on_idx_one=True,
        )
        wait_until(
            lambda: self.redpanda.registered(joiner_node),
            timeout_sec=120,
            backoff_sec=5,
        )
        return joiner_node_id

    def _check_tp_recovered(
        self,
        node: ClusterNode,
        ntp: NTP,
        replication_factor: int,
        killed_node_ids: list[int],
    ) -> bool:
        """
        check that an ntp recovered by checking
        1. that it has a leader
        2. that it has a healthy number of voters
        3. none of its replicas are on a dead node
        params:
            :param node: living node against which to send the rpcs
            :param ntp: the namespace/topic/partition to actually check
            :param replication_factor: healthy number of replicas
            :param killed_node_ids: a list of dead node ids, fail if theres a replica on these
        """
        state = self.redpanda._admin.get_partition_state(
            ntp.namespace,
            ntp.topic,
            ntp.partition,
            node=node,
        )
        self.redpanda.logger.debug(
            f"_check_tp_recovered: node: {node.name} waiting for recovery of {ntp=}, found {state=}"
        )

        leader_raft_state: Any = None
        for replica in state["replicas"]:
            raft_state = replica["raft_state"]
            if raft_state["is_leader"]:
                leader_raft_state = raft_state
                break

        if not leader_raft_state:
            self.redpanda.logger.debug(f"_check_tp_recovered: no leader yet for {ntp=}")
            return False

        nodes: list[int] = []
        nodes.append(leader_raft_state["node_id"])

        # get all followers that are NOT learners
        for follower in leader_raft_state["followers"]:
            if not follower["is_learner"]:
                nodes.append(follower["id"])

        if len(nodes) != replication_factor:
            self.redpanda.logger.debug(
                f"_check_tp_recovered: expected group of size: {replication_factor}, but found {len(nodes)}"
            )
            return False

        for killed_node_id in killed_node_ids:
            if killed_node_id in nodes:
                self.redpanda.logger.debug(
                    f"_check_tp_recovered: dead node: {killed_node_id} still in configuration"
                )
                return False

        self.redpanda.logger.debug(
            f"_check_tp_recovered: success for node: {node.name} {ntp=}"
        )
        return True

    def _check_topic_recovered(self, topic: TopicSpec, killed_node_ids: list[int]):
        """checks that a topic recovered, meaning it has the correct amount of followers which are NOT learners. Checks that no topic is hosted on a dead node"""
        for partition in range(0, topic.partition_count):
            ntp = NTP(topic=topic.name, partition=partition)
            for living_node in self._living_nodes():
                wait_until(
                    lambda: self._check_tp_recovered(
                        node=living_node,
                        ntp=ntp,
                        replication_factor=topic.replication_factor,
                        killed_node_ids=killed_node_ids,
                    ),
                    timeout_sec=120,
                    backoff_sec=1,
                )
        return True

    def _no_majority_lost_partitions(
        self, node: ClusterNode, dead_node_ids: list[int], timeout: TimeoutConfig
    ) -> bool:
        """check that no partition currently has quorum loss"""

        def controller_available() -> bool:
            controller = self.redpanda.controller()
            return controller is not None and bool(self.redpanda.node_id(controller))

        try:
            wait_until(
                controller_available,
                timeout_sec=timeout.timeout_s,
                backoff_sec=timeout.backoff_s,
                err_msg="Controller not available",
            )
            lost_majority = (
                self.redpanda._admin.get_majority_lost_partitions_from_nodes(
                    dead_brokers=dead_node_ids,
                    node=node,
                    timeout=medium_timeout.timeout_s,
                )
            )
            self.redpanda.logger.debug(
                f"Partitions with lost majority: {lost_majority}"
            )
            return len(lost_majority) == 0
        except Exception as e:
            self.redpanda.logger.debug(e, exc_info=True)
            return False

    def _pin_ntp_brokers(self, ntp: NTP, assignments: list[int]):
        """use the admin api to move a partition"""
        INVALID_CORE = 12121212
        self.redpanda.logger.info(f"setting assignments for {ntp=}: {assignments=}")

        self.redpanda._admin.set_partition_replicas(
            namespace=ntp.namespace,
            topic=ntp.topic,
            partition=ntp.partition,
            replicas=[
                {
                    "node_id": a,
                    "core": INVALID_CORE,
                }
                for a in assignments
            ],
        )

    def _pin_partition_to_dying_brokers(
        self, dead_node_ids: list[int], topic: TopicSpec
    ):
        """this will pin at least one partition to be completely lost in the cluster breakdown"""
        kcl = KCL(self.redpanda)
        assert len(dead_node_ids) >= topic.replication_factor, (
            "can't fully lose a partition which has greater replication than dead brokers"
        )
        node_ids_to_pin = dead_node_ids[0 : topic.replication_factor]
        # pin partition 0
        p0_pinning: dict[int, list[int]] = {0: node_ids_to_pin}
        topic_pinning: dict[str, dict[int, list[int]]] = {topic.name: p0_pinning}
        kcl.alter_partition_reassignments(topics=topic_pinning)

    def _wait_for_no_force_reconfigurations(self):
        """polls on partition balancer status to wait until all force reconfigurations have completed"""

        def no_pending_force_reconfigurations():
            status = self.redpanda._admin.get_partition_balancer_status()
            return status["partitions_pending_force_recovery_count"] == 0

        wait_until(
            no_pending_force_reconfigurations,
            timeout_sec=long_timeout.timeout_s,
            backoff_sec=long_timeout.backoff_s,
            err_msg="reported force recovery count is non zero",
            retry_on_exc=True,
        )

    def _wait_for_node_removed(self, decommissioned_id: int):
        """wait until decommission and remove actually completes"""
        waiter = NodeDecommissionWaiter(
            self.redpanda,
            decommissioned_id,
            self.logger,
            progress_timeout=medium_timeout.timeout_s,
        )
        waiter.wait_for_removal()

    def _force_reconfigure_stuck_partition(self, ntp: NTP, target_nodes: list[int]):
        """invokes a vanilla force reconfiguration, needed because currently nodewise recovery cannot force moving partitions"""
        self.redpanda.logger.debug(
            f"_force_reconfigure_stuck_partition on ntp {ntp} to nodes {target_nodes}"
        )
        new_replicas_list = [
            Replica(dict(node_id=node_id, core=0)) for node_id in target_nodes
        ]
        new_replicas = [
            dict(node_id=replica.node_id, core=replica.core)
            for replica in new_replicas_list
        ]
        self.redpanda._admin.force_set_partition_replicas(
            topic=ntp.topic, partition=ntp.partition, replicas=new_replicas
        )

    def _execute_cfr_against_node(
        self,
        survivor: ClusterNode,
        admin: AdminV2,
        dead_node_ids: list[int],
        surviving_node_count: int,
    ):
        """Execute a CFR request against a specific node"""
        survivor_id = self.redpanda.node_id(survivor)
        self.redpanda.logger.debug(f"cfr on node {survivor.name} with id {survivor_id}")
        breakglass_client = admin.breakglass(node=survivor)
        request = breakglass_pb2.ControllerForcedReconfigurationRequest(
            dead_node_ids=dead_node_ids,
            surviving_node_count=surviving_node_count,
        )
        result = self._do_request(breakglass_client, request)
        self.redpanda.logger.debug(f"CFR request on {survivor.name} finished")

        error = result.error()
        if error is not None:
            # this happens when there are multiple candidate leaders
            # and one wins the election before all cfr requests have been finished
            if "use the existing controller leader" in error.message:
                return
            error_message = f"CFR request on node {survivor.name} failed with error {result.error()}"
            self.redpanda.logger.info(error_message)
            assert False, error_message

    def _execute_cfr_requests_parallel(
        self,
        nodes: list[ClusterNode],
        admin: AdminV2,
        dead_node_ids: list[int],
        surviving_node_count: int,
    ):
        """Execute CFR requests in parallel against multiple nodes using threads"""
        self.redpanda.logger.debug("beginning CFR requests")
        cfr_threads: list[threading.Thread] = []
        for survivor in nodes:
            cfr_thread = threading.Thread(
                target=self._execute_cfr_against_node,
                args=(survivor, admin, dead_node_ids, surviving_node_count),
            )
            cfr_thread.start()
            cfr_threads.append(cfr_thread)

        for cfr_thread in cfr_threads:
            cfr_thread.join()


class ControllerForcedReconfiguration_SmokeTest(
    ControllerForceReconfigurationTestBase, PartitionMovementMixin
):
    cluster_size: int = 3

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super(ControllerForcedReconfiguration_SmokeTest, self).__init__(
            test_context,
            cluster_size=ControllerForcedReconfiguration_SmokeTest.cluster_size,
            *args,
            **kwargs,
        )

    @cluster(num_nodes=4)
    def test_smoke_cfr(self):
        """
        1. create a cluster of size three
        2. add a topic and produce to it
        3. fail the majority of nodes in the cluster
        4. reboot into recovery mode
        5. force reconfigure the cluster to the remaining survivor
        6. add new brokers back to three
        7. force reconfigure + decommission
        8. reboot into normal mode
        9. produce to topic
        10. check that all partitions on topic have voter set of 3
        """
        admin = AdminV2(self.redpanda)

        # will start a cluster of 3 nodes on 1, 2, 3
        cluster_size: int = 3
        _ = self._start_redpanda(cluster_size=cluster_size)

        controller_ntp = NTP(namespace="redpanda", topic="controller", partition=0)
        short_timeout = TimeoutConfig(timeout_s=30, backoff_s=2)

        topic = TopicSpec(
            replication_factor=3,
            partition_count=1,
            redpanda_remote_read=True,
            redpanda_remote_write=True,
        )

        self.client().create_topic(topic)

        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=10000,
            msg_count=1000,
        )

        killed, living = self._stop_majority_nodes(
            ntp=controller_ntp, timeout=short_timeout, replication=cluster_size
        )

        killed_node_ids = [dead_node.node_id for dead_node in killed]

        self.redpanda.logger.debug(f"killed nodes: {killed}, living nodes: {living}")

        designated_survivors = self._living_nodes()
        assert len(designated_survivors) == 1, (
            f"found too many living expected 1 found: {len(designated_survivors)}"
        )
        designated_survivor = designated_survivors[0]

        self._toggle_recovery_mode(
            node=designated_survivor,
            timeout=medium_timeout,
            recovery_mode_enabled=True,
        )

        self._execute_cfr_requests_parallel(
            nodes=[designated_survivor],
            admin=admin,
            dead_node_ids=killed_node_ids,
            surviving_node_count=1,
        )

        def controller_available():
            controller = self.redpanda.controller()
            return (
                controller is not None
                and self.redpanda.node_id(controller) not in killed_node_ids
            )

        self.redpanda.logger.debug("waiting for controller to recover")
        wait_until(
            lambda: controller_available(),
            timeout_sec=really_long_timeout.timeout_s,
            backoff_sec=really_long_timeout.backoff_s,
            err_msg="Controller never came back",
        )
        self.redpanda.logger.debug("controller recovered")

        # these nodes will rejoin with new node-ids
        for joiner_node_id in killed_node_ids:
            joiner_node = self.redpanda.get_node_by_id(joiner_node_id)
            assert joiner_node is not None, "node should have been found"
            self.redpanda.logger.debug(f"joining node {joiner_node.name}")
            _ = self._join_new_node(joiner_node)

        self._toggle_recovery_mode(
            node=designated_survivor,
            timeout=medium_timeout,
            recovery_mode_enabled=False,
        )

        self.redpanda.logger.debug(f"recovering from: {killed_node_ids}")
        self._rpk = RpkTool(self.redpanda)

        # issue a node wise recovery
        self._rpk.force_partition_recovery(
            from_nodes=killed_node_ids, to_node=designated_survivor
        )

        self._wait_for_no_force_reconfigurations()

        for dead_node_id in killed_node_ids:
            self.redpanda._admin.decommission_broker(dead_node_id, designated_survivor)
            self._wait_for_node_removed(dead_node_id)

        wait_until(
            lambda: self._check_topic_recovered(
                topic=topic, killed_node_ids=killed_node_ids
            ),
            timeout_sec=really_long_timeout.timeout_s,
            backoff_sec=really_long_timeout.backoff_s,
            retry_on_exc=True,
        )

        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=10000,
            msg_count=1000,
        )


class ControllerForcedReconfiguration_Size5(
    ControllerForceReconfigurationTestBase, PartitionMovementMixin
):
    """
    This is a set of tests for controller forced reconfiguration which make sense for clusters 5+
    Namely, what happens if a broker is decomissioning

    """

    cluster_size: int = 5

    def _wait_for_move_complete(self, ntp: NTP, target_nodes: list[int]):
        """lifted from partition_movement, needed to be generalized for internal namespaces"""
        assignments = [{"node_id": node_id} for node_id in target_nodes]

        # We need to add retries, becasue of eventual consistency. Metadata will be updated but it can take some time.
        admin = self.redpanda._admin

        def node_assignments_converged():
            results: list[bool] = []
            for n in self.redpanda._started:
                info = admin.get_partitions(
                    namespace=ntp.namespace,
                    topic=ntp.topic,
                    partition=ntp.partition,
                    node=n,
                )
                node_assignments = [{"node_id": r["node_id"]} for r in info["replicas"]]
                self.logger.info(
                    f"node assignments for {ntp.namespace}{ntp.topic}/{ntp.partition}: {node_assignments}, "
                    f"partition status: {info['status']}"
                )
                converged = self._equal_assignments(node_assignments, assignments)
                results.append(converged and info["status"] == "done")

            return all(results)

        # wait until redpanda reports complete
        wait_until(
            condition=node_assignments_converged,
            timeout_sec=medium_timeout.timeout_s,
            backoff_sec=medium_timeout.backoff_s,
        )

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super(ControllerForcedReconfiguration_Size5, self).__init__(
            test_context,
            cluster_size=ControllerForcedReconfiguration_Size5.cluster_size,
            *args,
            **kwargs,
        )

    @cluster(num_nodes=6)
    @matrix(scenario=[Scenario.Simple])
    def test_cluster_recovery(self, scenario: Scenario):
        """
        This test is meant to drill an approximately real cluster recovery scenario.
        Forcibly recover a cluster with an original node count of 5 cut down to only 2.
        Checks that CFR can recover the controller, and that normal recovery rpcs can be used
        to recover all partitions accordingly.
        phases:
            setup:
                1. bootstrap
                2. produce to hydrate data
                3. split cluster into survivors and nodes to kill
                4. pin partitions
                    - one partition should be guaranteed full quorum loss
            the meat of the test:
                1. kill the majority of nodes
                2. reboot the survivors into recovery mode
                3. foreach survivor run CFR
                4. join new nodes back to original node number
                5. reboot all nodes out of recovery mode
                6. nodewise recovery to force recover all partitions
                7. decommission all dead nodes
            validation:
                1. upsert a configuration
                2. validate topic recovered
                3. check no quorum loss partitions
                4. check that produce succeeds
        """

        """constants"""
        cluster_size: int = 5
        controller_ntp = NTP(namespace="redpanda", topic="controller", partition=0)
        topic = TopicSpec(
            replication_factor=3,
            partition_count=3,
            redpanda_remote_read=True,
            redpanda_remote_write=True,
        )

        """ setup 1: bootstrap """
        admin = AdminV2(self.redpanda)
        _ = self._start_redpanda(cluster_size=cluster_size)
        self.client().create_topic(topic)

        """ setup 2: start with some data in the topic"""
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=10000,
            msg_count=1000,
        )

        """ setup 3: divide the cluster into a majority which will be destroyed, and a minority which will survive """
        to_kill, living = self._split_cluster(
            ntp=controller_ntp, timeout=short_timeout, replication=cluster_size
        )
        # derived lists for convenience
        killed_node_ids = [dead_node.node_id for dead_node in to_kill]
        living_node_ids = [living_node.node_id for living_node in living]
        killed_cluster_nodes = [
            self.redpanda.get_node_by_id(dead_node.node_id) for dead_node in to_kill
        ]
        designated_survivors_tmp: list[ClusterNode | None] = [
            self.redpanda.get_node_by_id(living_node_id)
            for living_node_id in living_node_ids
        ]
        assert None not in designated_survivors_tmp, (
            "survivors list should not contain None"
        )
        designated_survivors: list[ClusterNode] = [
            node for node in designated_survivors_tmp if node is not None
        ]

        """ setup 4: pin at least one partition in the data topic to be entirely killed, may or may not complete """
        self._pin_partition_to_dying_brokers(dead_node_ids=killed_node_ids, topic=topic)

        """ meat 1: kill majority"""
        self._do_stop_nodes(ntp=controller_ntp, to_kill=to_kill, timeout=short_timeout)
        self.redpanda.logger.debug(
            f"killed nodes: {killed_node_ids}, living nodes: {living_node_ids}"
        )
        assert len(designated_survivors) == len(living), (
            f"found too many living expected {len(living)} found: {len(designated_survivors)}"
        )

        """ meat 2: reboot survivors into recovery mode"""
        self._bulk_toggle_recovery_mode(
            nodes=designated_survivors,
            timeout=medium_timeout,
            recovery_mode_enabled=True,
        )

        """ meat 3: CFR requests"""
        self._execute_cfr_requests_parallel(
            nodes=designated_survivors,
            admin=admin,
            dead_node_ids=killed_node_ids,
            surviving_node_count=len(designated_survivors),
        )

        def controller_available():
            controller = self.redpanda.controller()
            return (
                controller is not None
                and self.redpanda.node_id(controller) not in killed_node_ids
            )

        self.redpanda.logger.debug("waiting for controller to recover")
        recovery_timeout = TimeoutConfig(timeout_s=240, backoff_s=10)
        wait_until(
            lambda: controller_available(),
            timeout_sec=recovery_timeout.timeout_s,
            backoff_sec=recovery_timeout.backoff_s,
            err_msg="Controller never came back",
        )
        self.redpanda.logger.debug("controller recovered")

        """ meat 4: join new nodes until the cluster recovers to original node count"""
        # these nodes will rejoin with new node-ids
        for resurrected_node in killed_cluster_nodes:
            assert resurrected_node is not None
            _ = self._join_new_node(resurrected_node)

        """ meat 5: unset recovery mode"""
        self._bulk_toggle_recovery_mode(
            self.redpanda.started_nodes(), medium_timeout, recovery_mode_enabled=False
        )

        wait_until(
            controller_available, medium_timeout.timeout_s, medium_timeout.backoff_s
        )

        """ meat 6: nodewise recovery"""
        self.redpanda.logger.debug(f"recovering from: {killed_node_ids}")
        self._rpk = RpkTool(self.redpanda)
        # issue a node wise recovery
        self._rpk.force_partition_recovery(
            from_nodes=killed_node_ids, to_node=designated_survivors[0]
        )

        try:
            self._wait_for_no_force_reconfigurations()
        except Exception as e:
            """
                if a move was in progress at the time of quorum loss, nodewise recovery will not complete
                at the moment the only partition that can reasonably be in progress is the one we pinned to only dying nodes
                normal force_reconfiguration is more reliable than nodewise recovery,
                we'll use that to force force the partition off the dying nodes.

                This can be removed once nodewise recovery allows forcing forced movements
            """
            self.redpanda.logger.info(
                f"failed first reconfiguration wait with exception {e}"
            )
            # grab enough living nodes for a force reconfiguration, then restart the wait
            force_reconfigure_node_ids = [
                self.redpanda.node_id(node)
                for node in self.redpanda.nodes[0 : topic.replication_factor]
            ]
            to_force_ntp = NTP(topic=topic.name, partition=0)
            self._force_reconfigure_stuck_partition(
                ntp=to_force_ntp, target_nodes=force_reconfigure_node_ids
            )
            self.redpanda.logger.info(
                f"retry wait on partition force reconfiguration for ntp {to_force_ntp}"
            )
            self._wait_for_no_force_reconfigurations()

        """ meat 7: decommission dead node ids"""
        for dead_node_id in killed_node_ids:
            self.redpanda._admin.decommission_broker(
                dead_node_id, designated_survivors[0]
            )
            self._wait_for_node_removed(dead_node_id)

        """ validation 1: upsert a new configuration"""
        # convenient check that the dead nodes have been fully removed and cleaned up
        # if there are any leftovers, cluster configuration will fail to converge to
        # the new configuration version
        self.redpanda.set_cluster_config(
            {"raft_learner_recovery_rate": 1 << 30},
            timeout=really_long_timeout.timeout_s,
        )

        """ validation 2: check that the created topic recovered"""
        wait_until(
            lambda: self._check_topic_recovered(
                topic=topic, killed_node_ids=killed_node_ids
            ),
            timeout_sec=really_long_timeout.timeout_s,
            backoff_sec=really_long_timeout.backoff_s,
            retry_on_exc=True,
        )

        """ validation 3: check that all partitions have quorum"""
        wait_until(
            lambda: self._no_majority_lost_partitions(
                designated_survivors[0], killed_node_ids, really_short_timeout
            ),
            timeout_sec=long_timeout.timeout_s,
            backoff_sec=long_timeout.backoff_s,
        )

        """ validation 4: check that we can produce again"""
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=10000,
            msg_count=3000,
        )
        producer.start(clean=True)
        producer.wait(timeout_sec=medium_timeout.timeout_s)
        status = producer.produce_status
        assert status.sent == 3000


class ControllerForcedReconfiguration_Size6(
    ControllerForceReconfigurationTestBase, PartitionMovementMixin
):
    cluster_size: int = 6

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super(ControllerForcedReconfiguration_Size6, self).__init__(
            test_context,
            cluster_size=ControllerForcedReconfiguration_Size6.cluster_size,
            *args,
            **kwargs,
        )

    @cluster(num_nodes=6)
    def test_longest_log(self):
        """
        This test will ensure that the longest log is always chosen.
        The scheme is as follows:
          Node ids:
           1  2  3  4  5  6
        A [1  2  3  4  5  6] -> will receive the creation of topic "all"
        B             [5  6] -> will be shut down
        C [1  2  3  4]       -> will receive the creation of topic "some"
        D    [2  3  4]       -> will be shut down
        E             [5  6] -> will be started up
        F [1           5  6] -> will receive a cfr command
        G then we check that the resultant post CFR cluster contains all and some
          after new leader election
        """

        """constants"""
        cluster_size: int = ControllerForcedReconfiguration_Size6.cluster_size
        all_topic_spec = TopicSpec(name="all", replication_factor=3, partition_count=3)
        some_topic_spec = TopicSpec(
            name="some", replication_factor=3, partition_count=3
        )
        designated_survivor_id = 1
        step_b_shut_down_ids = [5, 6]
        step_d_shut_down_ids = [2, 3, 4]
        step_e_start_up_ids = step_b_shut_down_ids
        step_f_survivor_ids = step_e_start_up_ids.copy()
        step_f_survivor_ids.append(designated_survivor_id)
        step_f_dead_ids = step_d_shut_down_ids

        admin = AdminV2(self.redpanda)
        _ = self._start_redpanda(cluster_size=cluster_size)

        step_b_shut_down_nodes = [
            self.redpanda.node_by_id(node_id) for node_id in step_b_shut_down_ids
        ]
        step_d_shut_down_nodes = [
            self.redpanda.node_by_id(node_id) for node_id in step_d_shut_down_ids
        ]
        step_e_start_up_nodes = [
            self.redpanda.node_by_id(node_id) for node_id in step_e_start_up_ids
        ]
        step_f_survivor_nodes = [
            self.redpanda.node_by_id(node_id) for node_id in step_f_survivor_ids
        ]

        """ step A: all receive the creation of topic"""
        self.client().create_topic(all_topic_spec)

        """ step B: stop step_b_shut_down_nodes """
        for stop_node in step_b_shut_down_nodes:
            self.redpanda.stop_node(stop_node, timeout=really_short_timeout.timeout_s)

        """ step C: create topic some"""
        self.client().create_topic(some_topic_spec)

        """ step D: stop step_d_shut_down_nodes """
        for stop_node in step_d_shut_down_nodes:
            self.redpanda.stop_node(stop_node, timeout=really_short_timeout.timeout_s)

        """ step E: start step_e_start_up_nodes """
        for start_node in step_e_start_up_nodes:
            self.redpanda.start_node(start_node, timeout=short_timeout.timeout_s)

        """ step F: CFR the surviving nodes"""

        """ bulk reboot into recovery mode """
        self._bulk_toggle_recovery_mode(
            nodes=step_f_survivor_nodes,
            timeout=medium_timeout,
            recovery_mode_enabled=True,
        )

        """ do CFR requests """
        self._execute_cfr_requests_parallel(
            nodes=step_f_survivor_nodes,
            admin=admin,
            dead_node_ids=step_f_dead_ids,
            surviving_node_count=len(step_f_survivor_ids),
        )

        def controller_available():
            controller = self.redpanda.controller()
            return (
                controller is not None
                and self.redpanda.node_id(controller) not in step_f_dead_ids
            )

        self.redpanda.logger.debug("waiting for controller to recover")
        wait_until(
            lambda: controller_available(),
            timeout_sec=long_timeout.timeout_s,
            backoff_sec=long_timeout.backoff_s,
            err_msg="Controller never came back",
        )
        self.redpanda.logger.debug("controller recovered")

        """ step G: ensure 'some' is in the topic list """
        rpk = RpkTool(self.redpanda)
        assert "some" in rpk.list_topics()
        leader_node = self.redpanda.controller()
        assert leader_node is not None, "there should be a controller leader"
        leader_id = self.redpanda.node_id(leader_node)
        assert leader_id is not None, "there should be a controller leader"
        assert int(leader_id) == 1
