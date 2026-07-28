# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
from math import ceil

from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest

consumer_group_topic_partitions = 64


class ConsumerGroupBalancingTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(ConsumerGroupBalancingTest, self).__init__(
            test_ctx,
            num_brokers=5,
            *args,
            extra_rp_conf={
                "group_topic_partitions": consumer_group_topic_partitions,
            },
            **kwargs,
        )

    @cluster(num_nodes=5)
    def test_coordinator_nodes_balance(self):
        """
        Test checking that consumer group coordinators are distributed evenly across nodes
        """

        topic = TopicSpec(partition_count=1)
        self.client().create_topic(topic)

        rpk = RpkTool(self.redpanda)
        admin = Admin(self.redpanda)

        # Wait for the freshly-started cluster to settle before producing.
        admin.await_stable_leader(topic.name, partition=0, timeout_s=30, backoff_s=1)

        # Initialize __consumer_offsets via a produce/consume. The first produce
        # lazily creates the id_allocator (via InitProducerID), whose leader
        # election can outlast the 5s delivery timeout on a just-started cluster.
        # Retry only that transient timeout; re-raise any other error.
        def initial_produce_succeeds() -> bool:
            try:
                rpk.produce(
                    topic=topic.name, key="test_key", msg="test_msg", partition=0
                )
                return True
            except RpkException as e:
                if "timed out" in e.msg + e.stderr:
                    return False
                raise

        wait_until(
            initial_produce_succeeds,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="initial produce did not succeed while the cluster warmed up",
        )

        rpk.consume(topic=topic.name, n=1, group="test-group")

        partitions = admin.get_partitions("__consumer_offsets")
        replicas_per_node = defaultdict(lambda: 0)

        for p in partitions:
            for node in p["replicas"]:
                replicas_per_node[node["node_id"]] += 1

        # Derive the target from the cluster size and the observed total replica
        # count rather than hard-coding, so the assertion tracks the test config
        # (and the topic's actual replication factor) if either changes.
        num_brokers = len(self.redpanda.nodes)
        total_replicas = sum(len(p["replicas"]) for p in partitions)
        expected = total_replicas / num_brokers

        for node, replicas in sorted(replicas_per_node.items()):
            self.logger.info(
                f"__consumer_offsets has {replicas} replicas on node {node} "
                f"(expected ~{expected:.0f})"
            )

        # Coordinators must be spread across every broker.
        assert len(replicas_per_node) == num_brokers, (
            f"__consumer_offsets replicas are not spread across all {num_brokers} "
            f"brokers: {dict(sorted(replicas_per_node.items()))}"
        )

        # The allocator only balances approximately (it balances total partitions
        # per node and breaks ties randomly), so a single topic drifts from the
        # ideal -- the seed node especially, which carries baseline partitions.
        # Allow each node within 15% of the mean (for the default config, mean 38.4
        # -> band [33, 44]): clears the ~2-3 skew seen under load (CORE-7771:
        # {36,39,39,39,39}) while still failing gross imbalance like replicas packed
        # onto a subset of nodes.
        tolerance = ceil(expected * 0.15)
        for node, replicas in replicas_per_node.items():
            assert abs(replicas - expected) <= tolerance, (
                f"node {node} holds {replicas} __consumer_offsets replicas, outside "
                f"the tolerated band [{expected - tolerance:.0f}, "
                f"{expected + tolerance:.0f}]; distribution: "
                f"{dict(sorted(replicas_per_node.items()))}"
            )
