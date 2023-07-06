# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from requests.exceptions import HTTPError

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST, RESTART_LOG_ALLOW_LIST


def in_maintenance(node, admin):
    """
    Use the given Admin to check if the given node is in maintenance mode.
    """
    status = admin.maintenance_status(node)
    if "finished" in status and status["finished"]:
        return True
    return status["draining"]


def wait_for_maintenance(node, admin):
    """
    Use the given Admin to wait until the given node is in maintenance mode.
    """
    wait_until(lambda: in_maintenance(node, admin),
               timeout_sec=10,
               backoff_sec=1)


class NodesDecommissioningTest(EndToEndTest):
    """
    Basic nodes decommissioning test.
    """
    def _create_topics(self, replication_factors=[1, 3]):
        topics = []
        for i in range(10):
            spec = TopicSpec(
                partition_count=random.randint(1, 10),
                replication_factor=random.choice(replication_factors))
            topics.append(spec)

        for spec in topics:
            self.client().create_topic(spec)

        self.topic = random.choice(topics).name

    def _partitions_moving(self):
        admin = Admin(self.redpanda)
        reconfigurations = admin.list_reconfigurations()
        return len(reconfigurations) > 0

    def _partitions_not_moving(self):
        admin = Admin(self.redpanda)
        reconfigurations = admin.list_reconfigurations()
        return len(reconfigurations) == 0

    def _partition_to_move(self, predicate):
        rpk = RpkTool(self.redpanda)

        for tp in rpk.list_topics():
            desc = rpk.describe_topic(tp)
            for p in desc:
                if predicate(p):
                    return (tp, p.id, p.replicas)

    def _not_decommissioned_node(self, to_decommission):
        return [
            n for n in self.redpanda.nodes
            if self.redpanda.idx(n) != to_decommission
        ][0]

    def _node_removed(self, removed_id, node_to_query):
        admin = Admin(self.redpanda)
        brokers = admin.get_brokers(node=node_to_query)
        for b in brokers:
            if b['node_id'] == removed_id:
                return False
        return True

    def _find_replacement(self, current_replicas, to_remove):
        new_replicas = []
        unique_node_ids = set()
        for r in current_replicas:
            if r['node_id'] != to_remove:
                unique_node_ids.add(r['node_id'])
                new_replicas.append(r)

        admin = Admin(self.redpanda)
        brokers = admin.get_brokers()

        to_add = None
        while len(unique_node_ids) < len(current_replicas):
            id = random.choice(brokers)['node_id']
            if id == to_remove:
                continue
            to_add = id
            unique_node_ids.add(to_add)

        new_replicas.append({"node_id": to_add, "core": 0})
        return new_replicas

    def _wait_until_status(self, node_id, status, timeout_sec=15):
        def requested_status():
            brokers = Admin(self.redpanda).get_brokers()
            for broker in brokers:
                if broker['node_id'] == node_id:
                    return broker['membership_status'] == status
            return False

        wait_until(requested_status, timeout_sec=timeout_sec, backoff_sec=1)

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommissioning_maintenance_node(self):
        self.start_redpanda(num_nodes=4)
        self._create_topics()
        admin = self.redpanda._admin

        to_decommission = random.choice(self.redpanda.nodes)
        to_decommission_id = self.redpanda.idx(to_decommission)
        survivor_node = self._not_decommissioned_node(to_decommission_id)

        admin.maintenance_start(to_decommission)
        wait_for_maintenance(to_decommission, admin)

        admin.decommission_broker(to_decommission_id)
        wait_until(
            lambda: self._node_removed(to_decommission_id, survivor_node),
            timeout_sec=120,
            backoff_sec=2)

        # We should be unable to run further maintenance commands on the
        # removed node.
        try:
            admin.maintenance_stop(to_decommission)
            assert False, f"Excepted 404 for node {to_decommission_id}"
        except HTTPError as e:
            assert "404 Client Error" in repr(e)

        # We should be able to start maintenance on another node, even if we
        # didn't explicitly stop maintenance on 'to_decommission'.
        admin.maintenance_start(survivor_node)
        wait_for_maintenance(survivor_node, admin)
        assert in_maintenance(to_decommission, admin)

    @cluster(num_nodes=6)
    def test_recommissioning_maintenance_node(self):
        self.start_redpanda(num_nodes=4)
        self._create_topics()
        admin = self.redpanda._admin

        # Start a workload so partition movement takes some amount of time.
        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        to_decommission = random.choice(self.redpanda.nodes)
        to_decommission_id = self.redpanda.idx(to_decommission)

        admin.maintenance_start(to_decommission)
        wait_for_maintenance(to_decommission, admin)

        # Throttle recovery so the node doesn't move its replicas away so
        # quickly, allowing us to recommission.
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("raft_learner_recovery_rate", str(1))

        admin.decommission_broker(to_decommission_id)
        self._wait_until_status(to_decommission_id, 'draining')

        # Recommission the broker. Maintenance state should be unaffected.
        admin.recommission_broker(to_decommission_id)
        self._wait_until_status(to_decommission_id, 'active')
        assert in_maintenance(to_decommission, admin)

        # One last sanity check that partitions aren't moving.
        wait_until(lambda: self._partitions_not_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

    @cluster(
        num_nodes=6,
        # A decom can look like a restart in terms of logs from peers dropping
        # connections with it
        log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommissioning_working_node(self):
        self.start_redpanda(num_nodes=4)
        self._create_topics()

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        admin = Admin(self.redpanda)

        to_decommission = random.choice(self.redpanda.nodes)
        to_decommission_id = self.redpanda.idx(to_decommission)
        self.logger.info(f"decommissioning node: {to_decommission_id}", )
        admin.decommission_broker(to_decommission_id)

        # A node which isn't being decommed, to use when calling into
        # the admin API from this point onwards.
        survivor_node = self._not_decommissioned_node(to_decommission_id)
        self.logger.info(
            f"Using survivor node {survivor_node.name} {self.redpanda.idx(survivor_node)}"
        )

        wait_until(
            lambda: self._node_removed(to_decommission_id, survivor_node),
            timeout_sec=120,
            backoff_sec=2)

        # Stop the decommissioned node, because redpanda internally does not
        # fence it, it is the responsibility of external orchestrator to
        # stop the node they intend to remove.
        # This can be removed when we change redpanda to prevent decommissioned nodes
        # from responding to client Kafka requests.
        self.redpanda.stop_node(to_decommission)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_decommissioning_crashed_node(self):

        self.start_redpanda(num_nodes=4)
        self._create_topics(replication_factors=[3])

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        admin = Admin(self.redpanda)

        to_decommission = self.redpanda.nodes[1]
        node_id = self.redpanda.idx(to_decommission)
        survivor_node = survivor_node = self._not_decommissioned_node(node_id)
        self.redpanda.stop_node(node=to_decommission)
        self.logger.info(f"decommissioning node: {node_id}", )
        admin.decommission_broker(id=node_id)

        wait_until(lambda: self._node_removed(node_id, survivor_node),
                   timeout_sec=120,
                   backoff_sec=2)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

    @cluster(
        num_nodes=6,
        # A decom can look like a restart in terms of logs from peers dropping
        # connections with it
        log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommissioning_cancel_ongoing_movements(self):
        self.start_redpanda(num_nodes=4)
        self._create_topics()

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        admin = Admin(self.redpanda)

        brokers = admin.get_brokers()
        to_decommission = random.choice(brokers)['node_id']

        # throttle recovery
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("raft_learner_recovery_rate", str(1))

        # schedule partition move to the node that is being decommissioned

        tp_to_move, p_to_move, replicas = self._partition_to_move(
            lambda p: to_decommission not in p.replicas)

        details = admin.get_partitions(topic=tp_to_move, partition=p_to_move)

        new_replicas = details['replicas']
        new_replicas = new_replicas[1:]
        new_replicas.append({"node_id": to_decommission, "core": 0})

        self.logger.info(
            f"moving partition {tp_to_move}/{p_to_move} - {details['replicas']} -> {new_replicas}"
        )

        admin.set_partition_replicas(topic=tp_to_move,
                                     partition=p_to_move,
                                     replicas=new_replicas)
        # moving partition should be present in moving list
        wait_until(lambda: self._partitions_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

        self.logger.info(f"decommissioning node: {to_decommission}", )
        admin.decommission_broker(to_decommission)

        survivor_node = self._not_decommissioned_node(to_decommission)
        # adjust recovery throttle to make sure moves will finish
        rpk.cluster_config_set("raft_learner_recovery_rate", str(2 << 30))

        wait_until(lambda: self._node_removed(to_decommission, survivor_node),
                   timeout_sec=120,
                   backoff_sec=2)

        # stop decommissioned node
        self.redpanda.stop_node(self.redpanda.get_node_by_id(to_decommission))

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=90)

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_recommissioning_node(self):
        self.start_redpanda(num_nodes=4)
        self._create_topics()

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        admin = Admin(self.redpanda)

        brokers = admin.get_brokers()
        to_decommission = random.choice(brokers)['node_id']

        # throttle recovery
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("raft_learner_recovery_rate", str(1))

        self.logger.info(f"decommissioning node: {to_decommission}", )
        admin.decommission_broker(to_decommission)

        self._wait_until_status(to_decommission, 'draining')

        wait_until(lambda: self._partitions_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

        # recommission broker
        admin.recommission_broker(to_decommission)
        self._wait_until_status(to_decommission, 'active')

        wait_until(lambda: self._partitions_not_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_recommissioning_node_finishes(self):
        self.start_redpanda(num_nodes=4)
        self._create_topics()

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        admin = Admin(self.redpanda)

        brokers = admin.get_brokers()
        to_decommission = random.choice(brokers)['node_id']

        # throttle recovery
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("raft_learner_recovery_rate", str(1))

        self.logger.info(f"decommissioning node: {to_decommission}", )
        admin.decommission_broker(to_decommission)

        self._wait_until_status(to_decommission, 'draining')

        wait_until(lambda: self._partitions_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

        # recommission broker
        admin.recommission_broker(to_decommission)
        self._wait_until_status(to_decommission, 'active')

        wait_until(lambda: self._partitions_not_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

        admin.decommission_broker(to_decommission)

        self._wait_until_status(to_decommission, 'draining')
        rpk.cluster_config_set("raft_learner_recovery_rate",
                               str(1024 * 1024 * 1024))

        def node_removed():
            brokers = admin.get_brokers()
            for broker in brokers:
                if broker['node_id'] == to_decommission:
                    return False
            return True

        wait_until(node_removed, 60, 2)

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_recommissioning_do_not_stop_all_moves_node(self):
        self.start_redpanda(num_nodes=4)
        self._create_topics()

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        admin = Admin(self.redpanda)

        brokers = admin.get_brokers()
        to_decommission = random.choice(brokers)['node_id']

        # throttle recovery
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("raft_learner_recovery_rate", str(1))

        # schedule partition move from the node being decommissioned before actually calling decommission

        to_move_tp, to_move_p, _ = self._partition_to_move(
            lambda p: to_decommission in p.replicas)
        details = admin.get_partitions(topic=to_move_tp, partition=to_move_p)

        new_replicas = self._find_replacement(details['replicas'],
                                              to_decommission)
        self.logger.info(
            f"moving partition {to_move_tp}/{to_move_p} - {details['replicas']} -> {new_replicas}"
        )

        admin.set_partition_replicas(topic=to_move_tp,
                                     partition=to_move_p,
                                     replicas=new_replicas)
        # moving partition should be present in moving list
        wait_until(lambda: self._partitions_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

        self.logger.info(f"decommissioning node: {to_decommission}", )
        admin.decommission_broker(to_decommission)

        self._wait_until_status(to_decommission, 'draining')

        wait_until(lambda: self._partitions_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

        # recommission broker
        admin.recommission_broker(to_decommission)
        self._wait_until_status(to_decommission, 'active')

        def one_left_moving():
            reconfigurations = admin.list_reconfigurations()
            return len(reconfigurations) == 1

        wait_until(one_left_moving, timeout_sec=15, backoff_sec=1)

    @cluster(num_nodes=7, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_recommissioning_one_of_decommissioned_nodes(self):
        self.start_redpanda(num_nodes=5)
        self._create_topics()

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        admin = Admin(self.redpanda)

        brokers = admin.get_brokers()
        to_decommission_1 = random.choice(brokers)['node_id']
        to_decommission_2 = to_decommission_1

        while to_decommission_1 == to_decommission_2:
            to_decommission_2 = random.choice(brokers)['node_id']

        # throttle recovery
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("raft_learner_recovery_rate", str(1))

        self.logger.info(f"decommissioning node: {to_decommission_1}", )
        admin.decommission_broker(to_decommission_1)
        self.logger.info(f"decommissioning node: {to_decommission_2}", )
        admin.decommission_broker(to_decommission_2)

        self._wait_until_status(to_decommission_1, 'draining')
        self._wait_until_status(to_decommission_2, 'draining')

        wait_until(lambda: self._partitions_moving(),
                   timeout_sec=15,
                   backoff_sec=1)

        # recommission broker that was decommissioned first
        admin.recommission_broker(to_decommission_1)
        self._wait_until_status(to_decommission_1, 'active')

        rpk.cluster_config_set("raft_learner_recovery_rate", str(2 << 30))

        def node_removed():
            brokers = admin.get_brokers()
            for broker in brokers:
                if broker['node_id'] == to_decommission_2:
                    return False
            return True

        wait_until(node_removed, 60, 2)
