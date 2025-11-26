# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierConsumerGroupConsumer,
    KgoVerifierProducer,
)
from rptest.services.redpanda import (
    CHAOS_LOG_ALLOW_LIST,
    SISettings,
)
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.node_operations import NodeDecommissionWaiter


class AutoDecommissionTest(PreallocNodesTest):
    """
    Test automatic node decommissioning when a node is unresponsive.
    """

    def __init__(self, test_context):
        self._topic = None

        si_settings = SISettings(
            test_context=test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )

        extra_rp_conf = dict(
            enable_cluster_metadata_upload_loop=False,
            retention_local_trim_interval=5_000,
        )

        super(AutoDecommissionTest, self).__init__(
            test_context=test_context,
            num_brokers=5,
            node_prealloc_count=1,
            si_settings=si_settings,
            extra_rp_conf=extra_rp_conf,
        )

    def setup(self):
        # defer starting redpanda to test body
        pass

    @property
    def admin(self):
        # retry on timeout and service unavailable
        return Admin(self.redpanda, retry_codes=[503, 504])

    def _create_topics(
        self, replication_factors: list[int] = [1, 3]
    ):
        """
        :return: total number of partitions in all topics
        """
        total_partitions = 0
        topics: list[TopicSpec] = []
        for _ in range(10):
            partitions = random.randint(1, 10)
            spec = TopicSpec(
                partition_count=partitions,
                replication_factor=random.choice(replication_factors)            )
            topics.append(spec)
            total_partitions += partitions

        for spec in topics:
            rpk = RpkTool(self.redpanda)
            rpk.create_topic(
                topic=spec.name,
                partitions=spec.partition_count,
                replicas=spec.replication_factor
            )

        self._topic = random.choice(topics).name

        return total_partitions
    
    def _not_decommissioned_node(self, *args):
        decom_node_ids = args
        return [
            n
            for n in self.redpanda.started_nodes()
            if self.redpanda.node_id(n) not in decom_node_ids
        ][0]


    def _check_state_consistent(self, decommissioned_id: int):
        not_decommissioned = [
            n
            for n in self.redpanda.started_nodes()
            if self.redpanda.node_id(n) != decommissioned_id
        ]

        def _state_consistent():
            for n in not_decommissioned:
                cfg_status = self.admin.get_cluster_config_status(n)
                brokers = self.admin.get_brokers(n)
                config_ids = [s["node_id"] for s in cfg_status]
                brokers_ids = [b["node_id"] for b in brokers]
                self.logger.info(
                    f"broker_ids: {brokers_ids}, ids from configuration status: {config_ids}"
                )
                if sorted(brokers_ids) != sorted(config_ids):
                    return False
                if decommissioned_id in brokers_ids:
                    return False

            return True

        wait_until(
            _state_consistent,
            10,
            1,
            err_msg="Timeout waiting for nodes reported from configuration and cluster state to be consistent",
        )

    def _wait_for_node_removed(self, decommissioned_id: int):
        waiter = NodeDecommissionWaiter(
            self.redpanda, decommissioned_id, self.logger, progress_timeout=60
        )
        waiter.wait_for_removal()

        self._check_state_consistent(decommissioned_id)

    @property
    def msg_size(self):
        return 64

    @property
    def msg_count(self):
        return int(20 * self.producer_throughput / self.msg_size)

    @property
    def producer_throughput(self):
        return 1024 if self.debug_mode else 1024 * 1024

    def start_producer(self):
        self.logger.info(
            f"starting kgo-verifier producer with {self.msg_count} messages of size {self.msg_size} and throughput: {self.producer_throughput} bps"
        )
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self._topic,
            self.msg_size,
            self.msg_count,
            custom_node=self.preallocated_nodes,
            rate_limit_bps=self.producer_throughput,
        )

        self.producer.start(clean=False)

        wait_until(
            lambda: self.producer.produce_status.acked > 10,
            timeout_sec=120,
            backoff_sec=1,
        )

    def start_consumer(self):
        self.consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            self._topic,
            self.msg_size,
            readers=1,
            nodes=self.preallocated_nodes,
        )

        self.consumer.start(clean=False)

    def verify(self):
        self.logger.info(
            f"verifying workload: topic: {self._topic}, with [rate_limit: {self.producer_throughput}, message size: {self.msg_size}, message count: {self.msg_count}]"
        )
        self.producer.wait()

        # Await the consumer that is reading only the subset of data that
        # was written before it started.
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.invalid_reads == 0, (
            f"Invalid reads in topic: {self._topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"
        )
        del self.consumer

        # Start a new consumer to read all data written
        self.start_consumer()
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.invalid_reads == 0, (
            f"Invalid reads in topic: {self._topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"
        )

    def start_redpanda(self, new_bootstrap: bool = True):
        if new_bootstrap:
            self.redpanda.set_seed_servers(self.redpanda.nodes)

        self.redpanda.start(
            auto_assign_node_id=new_bootstrap, omit_seeds_on_idx_one=not new_bootstrap
        )

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
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
        self.redpanda.add_extra_rp_conf({
            "partition_autobalancing_mode": "continuous",
            "partition_autobalancing_node_availability_timeout_sec": partition_balancer_unavailable_timeout_s,
            "partition_autobalancing_node_autodecommission_time": autodecommission_timeout_s,
            "partition_autobalancing_tick_interval_ms": partition_balancer_tick_interval_ms,
        })

        # start four nodes s.t. any one node can fail and autobalancer can move the paritions elsewhere
        self.start_redpanda(new_bootstrap=True)
        self._create_topics(replication_factors=[3])

        self.start_producer()
        self.start_consumer()

        # Select a random node to make unresponsive
        to_decommission = random.choice(self.redpanda.nodes)
        node_id = self.redpanda.node_id(to_decommission)

        self.logger.info(
            f"Stopping node {node_id} to trigger automatic decommissioning"
        )

        # Stop the node to make it unresponsive
        self.redpanda.stop_node(node=to_decommission)

        # Wait for the timeout period plus buffer for processing
        # Total wait: timeout + extra time for detection and decommission start
        wait_time_sec =autodecommission_timeout_s * 2
        self.logger.info(
            f"Waiting {wait_time_sec} seconds for automatic decommissioning to trigger"
        )

        # just make sure we're not pinging the dead node
        survivor_node = self._not_decommissioned_node(node_id)

        # Verify that the node status changes to 'draining' (decommissioning)
        def node_is_draining():
            try:
                brokers = self.admin.get_brokers(node=survivor_node)
                for b in brokers:
                    if b["node_id"] == node_id:
                        self.logger.info(f"Node {node_id} status: {b['membership_status']}")
                        return b["membership_status"] == "draining"
                return False
            except Exception as e:
                self.logger.warn(f"Error checking broker status: {e}")
                return False

        # Wait for the node to be automatically marked as draining
        wait_until(
            node_is_draining,
            timeout_sec=wait_time_sec,
            backoff_sec=5,
            err_msg=f"Node {node_id} was not automatically decommissioned after {wait_time_sec} seconds"
        )

        self.logger.info(
            f"Node {node_id} was automatically marked for decommissioning"
        )

        # Wait for the decommission process to complete
        self._wait_for_node_removed(node_id)

        self.logger.info(
            f"Node {node_id} was successfully auto-decommissioned and removed"
        )

        # Verify data integrity
        self.verify()
