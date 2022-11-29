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

    @cluster(
        num_nodes=6,
        # A decom can look like a restart in terms of logs from peers dropping
        # connections with it
        log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_decommissioning_working_node(self):
        self.start_redpanda(num_nodes=4)
        topics = []
        for partition_count in range(1, 5):
            for replication_factor in (3, 3):
                name = f"topic{len(topics)}"
                spec = TopicSpec(name=name,
                                 partition_count=partition_count,
                                 replication_factor=replication_factor)
                topics.append(spec)

        for spec in topics:
            self.client().create_topic(spec)
            self.topic = spec.name

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
        survivor_node = [
            n for n in self.redpanda.nodes
            if self.redpanda.idx(n) != to_decommission['node_id']
        ][0]
        self.logger.info(
            f"Using survivor node {survivor_node.name} {self.redpanda.idx(survivor_node)}"
        )

        def node_removed():
            brokers = admin.get_brokers(node=survivor_node)
            for b in brokers:
                if b['node_id'] == to_decommission['node_id']:
                    return False
            return True

        wait_until(node_removed, timeout_sec=120, backoff_sec=2)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_decommissioning_crashed_node(self):

        self.start_redpanda(num_nodes=4)
        topics = []
        for partition_count in range(1, 5):
            for replication_factor in (3, 3):
                name = f"topic{len(topics)}"
                spec = TopicSpec(name=name,
                                 partition_count=partition_count,
                                 replication_factor=replication_factor)
                topics.append(spec)

        for spec in topics:
            self.client().create_topic(spec)
            self.topic = spec.name

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        admin = Admin(self.redpanda)

        survivor_node = self.redpanda.nodes[0]
        to_decommission = self.redpanda.nodes[1]
        node_id = self.redpanda.idx(to_decommission)
        self.redpanda.stop_node(node=to_decommission)
        self.logger.info(f"decommissioning node: {node_id}", )
        admin.decommission_broker(id=node_id)

        def node_removed():
            brokers = admin.get_brokers(node=survivor_node)
            for b in brokers:
                if b['node_id'] == node_id:
                    return False
            return True

        wait_until(node_removed, timeout_sec=120, backoff_sec=2)

        self.run_validation(enable_idempotence=False, consumer_timeout_sec=45)
