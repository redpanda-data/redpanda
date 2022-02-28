# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster_spec import ClusterSpec

import os
import time

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import ResourceSettings
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableProducer
from rptest.services.kaf_consumer import KafConsumer


class ConnectionRateLimitTest(RedpandaTest):
    MSG_SIZE = 1000000
    PRODUCE_COUNT = 2100
    READ_COUNT = 2000
    RANDOM_READ_PARALLEL = 2

    topics = (TopicSpec(partition_count=1, replication_factor=1), )

    def __init__(self, test_context):
        resource_setting = ResourceSettings(num_cpus=1)
        super(ConnectionRateLimitTest,
              self).__init__(test_context=test_context,
                             num_brokers=1,
                             extra_rp_conf={"max_connection_creation_rate": 2},
                             resource_settings=resource_setting)

        self._node_for_franz_go = test_context.cluster.alloc(
            ClusterSpec.simple_linux(1))
        self.logger.debug(
            f"Allocated verifier node {self._node_for_franz_go[0].name}")

        self._producer = FranzGoVerifiableProducer(test_context, self.redpanda,
                                                   self.topics[0],
                                                   self.MSG_SIZE,
                                                   self.PRODUCE_COUNT,
                                                   self._node_for_franz_go)

    def free_nodes(self):
        # Free the normally allocated nodes (e.g. RedpandaService)
        super().free_nodes()

        assert len(self._node_for_franz_go) == 1

        # The verifier opens huge numbers of connections, which can interfere
        # with subsequent tests' use of the node.  Clear them down first.
        wait_until(
            lambda: self.redpanda.sockets_clear(self._node_for_franz_go[0]),
            timeout_sec=120,
            backoff_sec=10)

        # Free the hand-allocated node that we share between the various
        # verifier services
        self.logger.debug(
            f"Freeing verifier node {self._node_for_franz_go[0].name}")
        self.test_context.cluster.free_single(self._node_for_franz_go[0])

    def read_data(self, consumers_count):
        consumers = []
        for i in range(consumers_count):
            new_consumer = KafConsumer(self.test_context, self.redpanda,
                                       self.topics[0], self.READ_COUNT,
                                       "oldest")
            consumers.append(new_consumer)

        for consumer in consumers:
            consumer.start()

        def consumed():
            need_finish = True
            for consumer in consumers:
                self.logger.debug(consumer.offset)
                if (consumer.offset.get(0) is
                        None) or consumer.offset[0] < self.READ_COUNT:
                    need_finish = False
            return need_finish

        wait_until(consumed, timeout_sec=180, backoff_sec=3)

    @cluster(num_nodes=4)
    def connection_rate_test(self):
        self._producer.start(clean=False)
        self._producer.wait()

        start1 = time.time()
        self.read_data(self.RANDOM_READ_PARALLEL)
        finish1 = time.time()

        start2 = time.time()
        self.read_data(self.RANDOM_READ_PARALLEL * 2)
        finish2 = time.time()

        time1 = finish1 - start1
        time2 = finish2 - start2

        assert time2 >= time1 * 1.7
