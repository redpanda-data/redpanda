# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

from ducktape.cluster.cluster import ClusterNode

from rptest.clients.types import TopicSpec
from rptest.services.rpk_producer import RpkProducer
from rptest.services.redpanda import ResourceSettings, RESTART_LOG_ALLOW_LIST
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest

RESIZE_LOG_ALLOW_LIST = RESTART_LOG_ALLOW_LIST + [
    re.compile("Decreasing redpanda core count is not allowed"),
    re.compile(
        "Failure during startup: cluster::configuration_invariants_changed")
]


class NodeResizeTest(RedpandaTest):
    """
    Validate redpanda behaviour on node core count changes.  At time of writing this simply checks
    that redpanda refuses to start if core count has decreased: if we make node resizes more
    flexible in future, this test should be updated to exercise that.
    """

    INITIAL_NUM_CPUS = 2

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            resource_settings=ResourceSettings(num_cpus=self.INITIAL_NUM_CPUS),
            **kwargs)

    def _restart_with_num_cpus(self, node: ClusterNode, num_cpus: int,
                               expect_fail: bool):
        self.redpanda.stop_node(node)
        self.redpanda.set_resource_settings(
            ResourceSettings(num_cpus=num_cpus))

        self.logger.info(f"Restarting redpanda with num_cpus={num_cpus}")
        if expect_fail:
            try:
                self.redpanda.start_node(node)
            except Exception as e:
                self.logger.info(
                    f"As expected, redpanda failed to start ({e})")
            else:
                raise RuntimeError(
                    "Redpanda started: it should have failed to start!")
        else:
            self.redpanda.start_node(node)
            self.logger.info("As expected, redpanda started successfully")

    @cluster(num_nodes=4, log_allow_list=RESIZE_LOG_ALLOW_LIST)
    def test_node_resize(self):
        # Create a topic and write some data to make sure the cluster
        # is all up & initialized, and that subsequent checks are happening
        # with some partitions actually assigned to shards.
        self._client.create_topic(
            TopicSpec(name="test", partition_count=10, replication_factor=3))
        producer = RpkProducer(context=self.test_context,
                               redpanda=self.redpanda,
                               topic="test",
                               msg_size=4096,
                               msg_count=1000,
                               acks=-1)
        producer.start()
        producer.wait()

        # Choose one node from the cluster to exercise checks on.
        target_node = self.redpanda.nodes[0]

        # Attempt to decrease CPU count relative to initial: redpanda should fail to start
        self._restart_with_num_cpus(node=target_node,
                                    num_cpus=self.INITIAL_NUM_CPUS - 1,
                                    expect_fail=True)

        # Increase CPU count: redpanda should accept this
        self._restart_with_num_cpus(node=target_node,
                                    num_cpus=self.INITIAL_NUM_CPUS + 1,
                                    expect_fail=False)

        # Now decrease back to original core count: this should fail, because we previously
        # increased so the original core count is now below the high water mark
        self._restart_with_num_cpus(node=target_node,
                                    num_cpus=self.INITIAL_NUM_CPUS,
                                    expect_fail=True)
