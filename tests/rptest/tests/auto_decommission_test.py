# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import time

from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until
from ducktape.tests.test import TestContext

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierMultiProducer,
    KgoVerifierParams,
    KgoVerifierMultiConsumerGroupConsumer,
)
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST
from rptest.tests.prealloc_nodes import PreallocNodesTest


class AutoDecommissionTest(PreallocNodesTest):
    """
    Test automatic node decommissioning when a node is unresponsive.
    """

    def __init__(self, test_context: TestContext):
        self._topic = None
        self._topics = None

        super(AutoDecommissionTest, self).__init__(
            test_context=test_context,
            num_brokers=5,
            node_prealloc_count=2,
        )

    def setup(self):
        # defer starting redpanda to test body
        pass

    @property
    def admin(self):
        return self.redpanda._admin

    def _create_topics(self, replication_factors: list[int] = [1, 3]):
        """
        :return: total number of partitions in all topics
        """
        total_partitions = 0
        topics: list[TopicSpec] = []
        for enumeration in range(10):
            partitions = random.randint(1, 10)
            spec = TopicSpec(
                name=f"topic-{enumeration}",
                partition_count=partitions,
                replication_factor=random.choice(replication_factors),
            )
            topics.append(spec)
            total_partitions += partitions

        rpk = RpkTool(self.redpanda)
        for spec in topics:
            rpk.create_topic(
                topic=spec.name,
                partitions=spec.partition_count,
                replicas=spec.replication_factor,
            )

        # self._topic = random.choice(topics).name
        self._topics = topics

        return total_partitions

    def _not_decommissioned_node(self, decommed_node_id: int) -> ClusterNode:
        return [
            n
            for n in self.redpanda.started_nodes()
            if self.redpanda.node_id(n) != decommed_node_id
        ][0]

    @property
    def msg_size(self) -> int:
        return 64

    @property
    def msg_count(self) -> int:
        # test should run for ~90s, so throughput over msg size * expected runtime should yield runtime
        return int(90 * self.producer_throughput / self.msg_size)

    @property
    def producer_throughput(self) -> int:
        # this is the total throughput for the entire producer
        return 1024

    def _get_messages_per_topic(self) -> int:
        # total messages over number of topics
        assert self._topics is not None, (
            "_topics list must be initialized by the time _get_messages_per_topic is called"
        )
        return int(self.msg_count / len(self._topics))

    def _get_throughput_per_topic(self) -> int:
        # total throughput over number of topics
        assert self._topics is not None, (
            "_topics list must be initialized by the time _get_throughput_per_topic is called"
        )
        return int(self.producer_throughput / len(self._topics))

    def _start_producer(self) -> None:
        self.redpanda.logger.info(
            f"starting kgo-verifier producer with expected runtime of {self.msg_count / self.producer_throughput}"
        )
        assert self._topics is not None, "topics must be defined to start producer"
        params = [
            KgoVerifierParams(
                topic=topic,
                msg_size=self.msg_size,
                msg_count=self._get_messages_per_topic(),
                rate_limit_bps=self._get_throughput_per_topic(),
                node=self.preallocated_nodes[0],
            )
            for topic in self._topics
        ]
        self.producer = KgoVerifierMultiProducer(
            context=self.test_context,
            redpanda=self.redpanda,
            topics=params,
            custom_node=self.preallocated_nodes[0:1],
        )

        self.producer.start()

        self.producer.wait_for_acks([10 for _ in self._topics], 15, 1)

    def _start_consumer(self) -> None:
        assert self._topics is not None, "topics must be defined to start consumer"
        params = [
            KgoVerifierParams(
                topic=topic,
                msg_size=self.msg_size,
                msg_count=self._get_messages_per_topic(),
                node=self.preallocated_nodes[1],
            )
            for topic in self._topics
        ]
        self.consumer = KgoVerifierMultiConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topics=params,
            custom_node=self.preallocated_nodes[1:],
        )

        self.consumer.start()

    def verify(self):
        self.redpanda.logger.info(
            f"verifying workload: topic: {self._topic}, "
            + f"with [rate_limit: {self.producer_throughput}, message size: {self.msg_size},"
            + f"message count: {self.msg_count}]"
        )
        # let the producer and consumer finish
        self.producer.wait()
        self.consumer.wait()

    def start_redpanda(self, new_bootstrap: bool = True):
        if new_bootstrap:
            self.redpanda.set_seed_servers(self.redpanda.nodes)

        self.redpanda.start(
            auto_assign_node_id=new_bootstrap, omit_seeds_on_idx_one=not new_bootstrap
        )

    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_automatic_node_decommissioning(self):
        """
        Test that a node is automatically decommissioned when it's unresponsive
        for the configured timeout period.
        """
        # tick < unavailable < autodecommission
        partition_balancer_tick_interval_ms = 5000
        partition_balancer_unavailable_timeout_s = 15
        autodecommission_timeout_s = 30

        # Configure partition autobalancing for auto-decommission
        self.redpanda.add_extra_rp_conf(
            {
                "partition_autobalancing_mode": "continuous",
                "partition_autobalancing_node_availability_timeout_sec": partition_balancer_unavailable_timeout_s,
                "partition_autobalancing_node_autodecommission_timeout_sec": autodecommission_timeout_s,
                "partition_autobalancing_tick_interval_ms": partition_balancer_tick_interval_ms,
            }
        )

        self.start_redpanda(new_bootstrap=True)
        self._create_topics(replication_factors=[3])

        self._start_producer()
        self._start_consumer()

        # Select a random node to make unresponsive
        to_decommission = random.choice(self.redpanda.nodes)
        node_id = self.redpanda.node_id(to_decommission)

        self.redpanda.logger.info(
            f"Stopping node {node_id} to trigger automatic decommissioning"
        )

        # Stop the node so it registers in the decom logic
        self.redpanda.stop_node(node=to_decommission)

        # Wait for the timeout period plus buffer for processing
        # Total wait: timeout + extra time for detection and decommission start
        wait_time_sec = autodecommission_timeout_s * 4
        self.redpanda.logger.info(
            f"Waiting {wait_time_sec} seconds for automatic decommissioning to trigger"
        )

        # Just make sure we're not pinging the dead node
        survivor_node = self._not_decommissioned_node(node_id)

        # Verify that the node status changes to 'draining' (decommissioning)
        def node_is_removed():
            try:
                brokers = self.admin.get_brokers(node=survivor_node)
                for b in brokers:
                    if b["node_id"] == node_id:
                        return False
                return True
            except Exception as e:
                self.redpanda.logger.info(f"Error checking broker status: {e}")
                return False

        # Decommission will remove the node, wait for it
        wait_until(
            node_is_removed,
            timeout_sec=wait_time_sec,
            backoff_sec=5,
            err_msg=f"Node {node_id} was not automatically decommissioned after {wait_time_sec} seconds",
        )

        self.redpanda.logger.info(f"Node {node_id} was successfully removed")

        # Finish the producers and consumers
        self.verify()

    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_decom_timer_reset(self):
        """
        Auto decommission safety requires that the timer for auto decommission resets when a node restarts
        (if this weren't the case, a restart of a quorum of nodes could easily cause an early node ejection).
        This test is meant to check that auto ejection DOES get delayed by a sufficient number of node restarts.
        """
        # tick < unavailable < autodecommission
        partition_balancer_tick_interval_ms = 5000
        partition_balancer_unavailable_timeout_s = 15
        autodecommission_timeout_s = 60

        # Configure partition autobalancing for auto-decommission
        self.redpanda.add_extra_rp_conf(
            {
                "partition_autobalancing_mode": "continuous",
                "partition_autobalancing_node_availability_timeout_sec": partition_balancer_unavailable_timeout_s,
                "partition_autobalancing_node_autodecommission_timeout_sec": autodecommission_timeout_s,
                "partition_autobalancing_tick_interval_ms": partition_balancer_tick_interval_ms,
            }
        )

        self.start_redpanda(new_bootstrap=True)
        self._create_topics(replication_factors=[3])

        self._start_producer()
        self._start_consumer()

        # Select a random node to make unresponsive
        to_decommission = random.choice(self.redpanda.nodes)
        to_decommission_node_id = self.redpanda.node_id(to_decommission)

        # just make sure we're not pinging the dead node
        survivor_node = self._not_decommissioned_node(to_decommission_node_id)

        self.redpanda.logger.info(
            f"Stopping node {to_decommission_node_id} to trigger automatic decommissioning"
        )

        # Stop the node so it decoms
        self.redpanda.stop_node(node=to_decommission)

        # Sleep for half of the auto decom timeout
        time.sleep(autodecommission_timeout_s / 2)

        # Restart the remaining nodes
        self.redpanda.restart_nodes(
            nodes=[
                node
                for node in self.redpanda.nodes
                if self.redpanda.node_id(node)
                is not self.redpanda.node_id(to_decommission)
            ],
            auto_assign_node_id=True,  # on restart, let the nodes resume with their prior id
        )

        # Wait for the controller to returns
        controller_leader_id = self.admin.await_stable_leader(
            "controller", namespace="redpanda"
        )
        self.redpanda.logger.debug(f"controller leader id: {controller_leader_id}")

        # The leader balancer starts after 30s, sleep for 45 seconds at which point
        # a balancer action should have happened and if the timer was genuinely reset
        # , the dead node should not be eligable for decommissioning yet
        time.sleep(autodecommission_timeout_s * 0.75)

        # Check that the dead broker has not been decommissioned
        broker_statuses = self.admin.get_brokers(node=survivor_node)
        was_decommissioned: bool = to_decommission_node_id not in [
            int(broker_status["node_id"]) for broker_status in broker_statuses
        ]
        assert was_decommissioned == False, (
            "a restart of the brokers should reset the autodecommission timeout"
        )

        # Wait for the remainder of the timeout to make sure it does eventually decom
        wait_time_sec = autodecommission_timeout_s * 2
        self.redpanda.logger.info(
            f"Waiting {wait_time_sec} seconds for automatic decommissioning to trigger"
        )

        # Verify that the node status changes to 'draining' (decommissioning)
        def node_is_removed():
            try:
                brokers = self.admin.get_brokers(node=survivor_node)
                for b in brokers:
                    if b["node_id"] == to_decommission_node_id:
                        return False
                return True
            except Exception as e:
                self.redpanda.logger.info(f"Error checking broker status: {e}")
                return False

        # Wait for the node to be removed from the
        wait_until(
            node_is_removed,
            timeout_sec=wait_time_sec,
            backoff_sec=5,
            err_msg=f"Node {to_decommission_node_id} was not automatically decommissioned after {wait_time_sec} seconds",
        )

        self.redpanda.logger.info(
            f"Node {to_decommission_node_id} was automatically marked for decommissioning"
        )

        # Finish the producers and consumers
        self.verify()
