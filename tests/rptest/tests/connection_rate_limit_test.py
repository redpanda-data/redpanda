# Copyright 2022 Redpanda Data, Inc.
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
from string import Template

import os
import time

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.redpanda import ResourceSettings
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableProducer
from rptest.services.kaf_consumer import KafConsumer
from rptest.services.metrics_check import MetricCheck

RATE_METRIC = "vectorized_kafka_rpc_connections_wait_rate_total"


class ConnectionRateLimitTest(PreallocNodesTest):
    MSG_SIZE = 1000000
    PRODUCE_COUNT = 10
    READ_COUNT = 10
    RANDOM_READ_PARALLEL = 3
    REFRESH_TOKENS_TIME_SEC = 2.0

    topics = (TopicSpec(partition_count=1, replication_factor=1), )

    def __init__(self, test_context):
        resource_setting = ResourceSettings(num_cpus=1)
        super(ConnectionRateLimitTest,
              self).__init__(test_context=test_context,
                             num_brokers=1,
                             node_prealloc_count=1,
                             extra_rp_conf={"kafka_connection_rate_limit": 6},
                             resource_settings=resource_setting)

        self._producer = FranzGoVerifiableProducer(test_context, self.redpanda,
                                                   self.topics[0],
                                                   self.MSG_SIZE,
                                                   self.PRODUCE_COUNT,
                                                   self.preallocated_nodes)

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
            connection_time = self.read_data(consumers_count)
            deltas.append(connection_time)

            # We should wait moment when all tokens will be refreshed
            time.sleep(self.REFRESH_TOKENS_TIME_SEC)

        return sum(deltas) / len(deltas)

    @cluster(num_nodes=8)
    def connection_rate_test(self):
        metrics_sql = Template("select sum(value) as value from metrics where name = '$name'")
        metrics = MetricCheck(self.logger, self.redpanda, self.redpanda.nodes[0], metrics_sql, [RATE_METRIC])

        self._producer.start(clean=False)
        self._producer.wait()

        time1 = self.get_read_time(self.RANDOM_READ_PARALLEL)
        time2 = self.get_read_time(self.RANDOM_READ_PARALLEL * 2)

        assert time2 >= time1 * 1.7
        assert metrics.evaluate([lambda a, b: b > a])
