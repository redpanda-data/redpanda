# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import random

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

from rptest.services.cluster import cluster
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.types import TopicSpec
from rptest.util import wait_until_result
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.admin import Admin


class LeadershipTransferEndToEndTest(EndToEndTest):
    # todo:
    #  - how much data is unreplicated?
    @cluster(num_nodes=6)
    @parametrize(acks=-1)
    @parametrize(acks=1)
    def test_e2e_while_transfer_leadership(self, acks):
        '''
        Validate that all the records will be delivered to consumers when there
        are multiple producers and log is evicted
        '''
        self.start_redpanda(
            num_nodes=3,
            extra_rp_conf={
                # Disable leader balancer, as this test is doing its own
                # partition movement and the balancer would interfere
                'enable_leader_balancer': False
            })

        spec = TopicSpec(name="topic", partition_count=3, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(2, throughput=10000, acks=acks)
        self.start_consumer(1)
        self.await_startup()

        kc = KafkaCat(self.redpanda)

        # choose a partition and a target node
        partition = self._get_partition(kc)
        target_node_id = next(
            filter(lambda r: r["id"] != partition["leader"],
                   partition["replicas"]))["id"]
        self.logger.debug(
            f"Transfering leader from {partition['leader']} to {target_node_id}"
        )

        # build the transfer url
        meta = kc.metadata()
        brokers = meta["brokers"]
        source_broker = next(
            filter(lambda b: b["id"] == partition["leader"], brokers))
        target_broker = next(
            filter(lambda b: b["id"] == target_node_id, brokers))
        self.logger.debug(f"Source broker {source_broker}")
        self.logger.debug(f"Target broker {target_broker}")

        # Send the request to any host, they should redirect to
        # the leader of the partition.
        partition_id = partition['partition']

        admin = Admin(self.redpanda)
        admin.partition_transfer_leadership("kafka", self.topic, partition_id,
                                            target_node_id)

        def transfer_complete():
            for _ in range(3):  # just give it a moment
                time.sleep(1)
                meta = kc.metadata()
                partition = next(
                    filter(lambda p: p["partition"] == partition_id,
                           meta["topics"][0]["partitions"]))
                if partition["leader"] == target_node_id:
                    return True
            return False

        wait_until(lambda: transfer_complete(),
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg="Transfer did not complete")

        self.run_validation(min_records=100000,
                            producer_timeout_sec=300,
                            consumer_timeout_sec=300)

    @cluster(num_nodes=6)
    @parametrize(acks=0)
    # messages get lost, just check that leadership transfer succeeds
    def test_transfer_leadership_under_load_with_acks0(self, acks):
        '''
        Validate that all the records will be delivered to consumers when there
        are multiple producers and log is evicted
        '''
        self.start_redpanda(
            num_nodes=3,
            extra_rp_conf={
                # Disable leader balancer, as this test is doing its own
                # partition movement and the balancer would interfere
                'enable_leader_balancer': False
            })

        spec = TopicSpec(name="topic", partition_count=3, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(2, throughput=10000, acks=acks)
        self.start_consumer(1)
        self.await_startup()

        kc = KafkaCat(self.redpanda)

        # choose a partition and a target node
        partition = self._get_partition(kc)
        target_node_id = next(
            filter(lambda r: r["id"] != partition["leader"],
                   partition["replicas"]))["id"]
        self.logger.debug(
            f"Transfering leader from {partition['leader']} to {target_node_id}"
        )

        # build the transfer url
        meta = kc.metadata()
        brokers = meta["brokers"]
        source_broker = next(
            filter(lambda b: b["id"] == partition["leader"], brokers))
        target_broker = next(
            filter(lambda b: b["id"] == target_node_id, brokers))
        self.logger.debug(f"Source broker {source_broker}")
        self.logger.debug(f"Target broker {target_broker}")

        # Send the request to any host, they should redirect to
        # the leader of the partition.
        partition_id = partition['partition']

        admin = Admin(self.redpanda)
        admin.partition_transfer_leadership("kafka", self.topic, partition_id,
                                            target_node_id)

        def transfer_complete():
            for _ in range(3):  # just give it a moment
                time.sleep(1)
                meta = kc.metadata()
                partition = next(
                    filter(lambda p: p["partition"] == partition_id,
                           meta["topics"][0]["partitions"]))
                if partition["leader"] == target_node_id:
                    return True
            return False

        wait_until(lambda: transfer_complete(),
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg="Transfer did not complete")

    def _get_partition(self, kc):
        def get_partition():
            meta = kc.metadata()
            topics = meta["topics"]
            assert len(topics) == 1
            assert topics[0]["topic"] == self.topic
            partition = random.choice(topics[0]["partitions"])
            return partition["leader"] > 0, partition

        return wait_until_result(get_partition,
                                 timeout_sec=30,
                                 backoff_sec=2,
                                 err_msg="No partition with leader available")
