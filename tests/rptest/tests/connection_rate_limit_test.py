# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.mark import ignore
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
    PRODUCE_COUNT = 10
    READ_COUNT = 10
    RANDOM_READ_PARALLEL = 2
    MAX_RETRY_COUNT = 4

    topics = (TopicSpec(partition_count=1, replication_factor=1), )

    def __init__(self, test_context):
        resource_setting = ResourceSettings(num_cpus=1)
        super(ConnectionRateLimitTest,
              self).__init__(test_context=test_context,
                             num_brokers=1,
                             extra_rp_conf={"kafka_connection_rate_limit": 6},
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

    def start_consumer(self):
        return KafConsumer(self.test_context, self.redpanda, self.topics[0],
                           self.READ_COUNT, "oldest")

    def stop_consumer(self, consumer):
        try:
            consumer.stop()
        except:
            # Should ignore exception form kaf_consumer
            pass
        consumer.free()

    def read_data(self, consumers_count):
        consumers = dict()
        for i in range(consumers_count):
            consumers[i] = self.start_consumer()

        start = time.time()

        for i in range(consumers_count):
            consumers[i].start()

        def consumed():
            need_finish = True
            for i in range(consumers_count):
                self.logger.debug(
                    f"Offset for {i} consumer: {consumers[i].offset}")

                if consumers[i].done is True:
                    self.stop_consumer(consumers[i])
                    self.logger.debug(f"Rerun consumer {i}")
                    consumers[i] = self.start_consumer()
                    consumers[i].start()

                if (consumers[i].offset.get(0) is
                        None) or consumers[i].offset[0] == 0:
                    need_finish = False

            return need_finish

        wait_until(consumed, timeout_sec=190, backoff_sec=0.1)

        finish = time.time()

        for i in range(consumers_count):
            self.stop_consumer(consumers[i])

        return finish - start

    def get_read_time(self, consumers_count):
        deltas = list()

        for i in range(10):
            time = self.read_data(consumers_count)
            deltas.append(time)

        return sum(deltas) / len(deltas)

    @cluster(num_nodes=6)
    def connection_rate_test(self):
        self._producer.start(clean=False)
        self._producer.wait()

        time1 = self.get_read_time(self.RANDOM_READ_PARALLEL)
        time2 = self.get_read_time(self.RANDOM_READ_PARALLEL * 2)

        assert time2 >= time1 * 1.7
