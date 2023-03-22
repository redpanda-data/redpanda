# Copyright 2020 Redpanda Data, Inc.
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

from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cat import KafkaCat
from rptest.util import wait_until_result
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

    @cluster(num_nodes=3)
    def test_self_transfer(self):
        admin = Admin(self.redpanda)
        for topic in self.topics:
            for partition in range(topic.partition_count):
                leader = admin.get_partitions(topic, partition)['leader_id']
                admin.partition_transfer_leadership("kafka", topic, partition,
                                                    leader)


class MultiTopicAutomaticLeadershipBalancingTest(RedpandaTest):
    topics = (
        TopicSpec(partition_count=61, replication_factor=3),
        TopicSpec(partition_count=151, replication_factor=3),
        TopicSpec(partition_count=263, replication_factor=3),
    )

    def __init__(self, test_context):
        extra_rp_conf = dict(leader_balancer_idle_timeout=20000,
                             leader_balancer_mode="random_hill_climbing")

        super(MultiTopicAutomaticLeadershipBalancingTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_topic_aware_rebalance(self):
        def all_partitions_present(nodes: int):
            for t in self.topics:
                tps = self.redpanda.partitions(t.name)
                total_leaders = sum(1 if t.leader else 0 for t in tps)
                total_nodes = set(t.leader for t in tps if t.leader)

                if len(total_nodes) < nodes:
                    return False

                if total_leaders != t.partition_count:
                    return False

            return True

        def has_leader_count(topic_name: str, min_per_node: int,
                             nodes: int) -> bool:
            tps = self.redpanda.partitions(topic_name)
            leaders = [p.leader for p in tps if p.leader]

            if len(set(leaders)) < nodes:
                return False

            leaders_per_node = dict(zip(leaders, [0] * len(leaders)))
            for l in leaders:
                leaders_per_node[l] += 1

            self.logger.info(
                f"{topic_name} has dist {leaders_per_node.values()}")
            return all(leader_cnt >= min_per_node
                       for leader_cnt in leaders_per_node.values())

        def topic_leadership_evenly_distributed():
            for t in self.topics:
                expected_leaders_per_node = int(0.8 * (t.partition_count / 3))
                self.logger.info(
                    f"for topic {t} expecting {expected_leaders_per_node} leaders"
                )

                if not has_leader_count(t.name, expected_leaders_per_node, 3):
                    return False

            return True

        self.logger.info("initial stabilization")
        wait_until(lambda: all_partitions_present(3),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Leadership did not stablize")

        node = self.redpanda.nodes[0]
        self.redpanda.stop_node(node)
        self.logger.info("stabilization post stop")
        wait_until(lambda: all_partitions_present(2),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Leadership did not stablize")

        # sleep for a bit to avoid triggering any of the sticky leaderhsip
        # optimizations
        time.sleep(60)

        self.redpanda.start_node(node)
        self.logger.info("stabilization post start")
        wait_until(lambda: topic_leadership_evenly_distributed(),
                   timeout_sec=300,
                   backoff_sec=10,
                   err_msg="Leadership did not stablize")


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

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
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

        # sleep for a bit to avoid triggering any of the sticky leaderhsip
        # optimizations
        time.sleep(60)

        # sanity check -- the node we stopped shouldn't be a leader for any
        # partition after the sleep above as releection should have taken place
        leaders = self._get_leaders_by_node()
        assert self.redpanda.idx(node) not in leaders

        # restart the stopped node and wait for 15 (out of 21) leaders to be
        # rebalanced on to the node. the error minimization done in the leader
        # balancer is a little fuzzy so it problematic to assert an exact target
        # number that should return
        self.redpanda.start_node(node)
        wait_until(lambda: all_partitions_present(3, 15),
                   timeout_sec=300,
                   backoff_sec=10,
                   err_msg="Leadership did not stablize")
