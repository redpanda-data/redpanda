# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST, RESTART_LOG_ALLOW_LIST


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
        return len(reconfigurations) > 0

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

        brokers = admin.get_brokers()
        to_decommission = random.choice(brokers)
        self.logger.info(f"decommissioning node: {to_decommission}", )
        admin.decommission_broker(to_decommission['node_id'])

        # A node which isn't being decommed, to use when calling into
        # the admin API from this point onwards.
        survivor_node = self._not_decommissioned_node(
            to_decommission['node_id'])
        self.logger.info(
            f"Using survivor node {survivor_node.name} {self.redpanda.idx(survivor_node)}"
        )

        wait_until(lambda: self._node_removed(to_decommission['node_id'],
                                              survivor_node),
                   timeout_sec=120,
                   backoff_sec=2)

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

        def check_status(node_id, status):
            brokers = admin.get_brokers()
            for broker in brokers:
                if broker['node_id'] == node_id:
                    return broker['membership_status'] == status

            return False

        wait_until(lambda: check_status(to_decommission, 'draining'),
                   timeout_sec=15,
                   backoff_sec=1)

        survivor_node = self._not_decommissioned_node(to_decommission)
        # adjust recovery throttle to make sure moves will finish
        rpk.cluster_config_set("raft_learner_recovery_rate", str(2 << 30))

        wait_until(lambda: self._node_removed(to_decommission, survivor_node),
                   timeout_sec=120,
                   backoff_sec=2)

        # stop decommissioned node
        self.redpanda.stop_node(self.redpanda.get_node(to_decommission))

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=90)
