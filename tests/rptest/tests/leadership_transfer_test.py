# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import collections
import random
import time

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cat import KafkaCat

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest


class LeadershipTransferTest(RedpandaTest):
    """
    Transfer leadership from one node to another.
    """
    topics = (TopicSpec(partition_count=3, replication_factor=3), )

    def __init__(self, *args, **kwargs):
        super(LeadershipTransferTest, self).__init__(
            *args,
            extra_rp_conf={
                # Disable leader balancer, as this test is doing its own
                # partition movement and the balancer would interfere
                'enable_leader_balancer': False
            },
            **kwargs)

    @cluster(num_nodes=3)
    def test_controller_recovery(self):
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
        partition = [None]

        def get_partition():
            meta = kc.metadata()
            topics = meta["topics"]
            assert len(topics) == 1
            assert topics[0]["topic"] == self.topic
            partition[0] = random.choice(topics[0]["partitions"])
            if partition[0]["leader"] > 0:
                return True
            return False

        wait_until(lambda: get_partition(),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="No partition with leader available")

        return partition[0]


class AutomaticLeadershipBalancingTest(RedpandaTest):
    # number cores = 3 (default)
    # number nodes = 3 (default)
    # parts per core = 7
    topics = (TopicSpec(partition_count=63, replication_factor=3), )

    def __init__(self, test_context):
        extra_rp_conf = dict(leader_balancer_idle_timeout=20000, )

        super(AutomaticLeadershipBalancingTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)

    def _get_leaders_by_node(self):
        kc = KafkaCat(self.redpanda)
        md = kc.metadata()
        topic = next(filter(lambda t: t["topic"] == self.topic, md["topics"]))
        leaders = (p["leader"] for p in topic["partitions"])
        return collections.Counter(leaders)

    @cluster(num_nodes=3)
    def test_automatic_rebalance(self):
        def all_partitions_present(num_nodes, per_node=None):
            leaders = self._get_leaders_by_node()
            for l in leaders:
                self.redpanda.logger.debug(f"Leaders on {l}: {leaders[l]}")
            count = sum(leaders[l] for l in leaders)
            total = len(leaders) == num_nodes and count == 63
            if per_node is not None:
                per_node_sat = all((leaders[l] > per_node for l in leaders))
                return total and per_node_sat
            return total

        # wait until all the partition leaders are elected on all three nodes
        wait_until(lambda: all_partitions_present(3),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Leadership did not stablize")

        # stop node and wait for all leaders to transfer
        # to another node
        node = self.redpanda.nodes[0]
        self.redpanda.stop_node(node)
        wait_until(lambda: all_partitions_present(2),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Leadership did not move to running nodes")

        leaders = self._get_leaders_by_node()
        assert self.redpanda.idx(node) not in leaders

        # sleep for a bit to avoid triggering any of the sticky leaderhsip
        # optimizations
        time.sleep(60)

        # restart the stopped node and wait for 15 (out of 21) leaders to be
        # rebalanced on to the node. the error minimization done in the leader
        # balancer is a little fuzzy so it problematic to assert an exact target
        # number that should return
        self.redpanda.start_node(node)
        wait_until(lambda: all_partitions_present(3, 15),
                   timeout_sec=300,
                   backoff_sec=10,
                   err_msg="Leadership did not stablize")
