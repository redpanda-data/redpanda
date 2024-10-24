# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

import time

from rptest.clients.types import TopicSpec
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.redpanda import ResourceSettings
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.metrics_check import MetricCheck

RATE_METRIC = "vectorized_kafka_rpc_connections_wait_rate_total"


class ConnectionRateLimitTest(PreallocNodesTest):
    MSG_SIZE = 1000000
    PRODUCE_COUNT = 10
    READ_COUNT = 10
    RANDOM_READ_PARALLEL = 3
    RATE_LIMIT = 4
    REFRESH_TOKENS_TIME_SEC = 2

    topics = (TopicSpec(partition_count=1, replication_factor=1), )

    def __init__(self, test_context):
        resource_setting = ResourceSettings(num_cpus=1)
        super(ConnectionRateLimitTest, self).__init__(
            test_context=test_context,
            num_brokers=1,
            node_prealloc_count=1,
            extra_rp_conf={"kafka_connection_rate_limit": self.RATE_LIMIT},
            resource_settings=resource_setting)

        self._producer = KgoVerifierProducer(test_context, self.redpanda,
                                             self.topics[0], self.MSG_SIZE,
                                             self.PRODUCE_COUNT,
                                             self.preallocated_nodes)

    def start_consumer(self):
        return RpkConsumer(context=self.test_context,
                           redpanda=self.redpanda,
                           topic=self.topics[0],
                           num_msgs=1,
                           save_msgs=True,
                           retry_sec=(1 / self.RATE_LIMIT))

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
                    f"Offset for {i} consumer: {len(consumers[i].messages)}")

                if len(consumers[i].messages) == 0:
                    need_finish = False
                    if consumers[i].done is True:
                        self.stop_consumer(consumers[i])
                        self.logger.debug(f"Rerun consumer {i}")
                        consumers[i] = self.start_consumer()
                        consumers[i].start()

            return need_finish

        wait_until(consumed,
                   timeout_sec=190,
                   backoff_sec=(1 / self.RATE_LIMIT))

        finish = time.time()

        for i in range(consumers_count):
            self.stop_consumer(consumers[i])

        return finish - start

    def get_read_time(self, consumers_count):
        deltas = list()

        for i in range(20):
            connection_time = self.read_data(consumers_count)
            deltas.append(connection_time)

            # We should wait moment when all tokens will be refreshed
            time.sleep(self.REFRESH_TOKENS_TIME_SEC)

        return sum(deltas) / len(deltas)

    @cluster(num_nodes=8)
    def connection_rate_test(self):
        self._producer.start(clean=False)
        self._producer.wait()

        time1 = self.get_read_time(self.RANDOM_READ_PARALLEL)
        time2 = self.get_read_time(self.RANDOM_READ_PARALLEL * 2)

        metrics = MetricCheck(self.logger, self.redpanda,
                              self.redpanda.nodes[0], RATE_METRIC, {})
        metrics.evaluate([(RATE_METRIC, lambda a, b: b > 0)])

        assert time2 >= time1 * 1.6, f'Time for first iteration:{time1} Time for second iteration:{time2}'
