# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0


from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.metrics_check import MetricCheck
from rptest.services.redpanda import ResourceSettings
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.mode_checks import skip_debug_mode

RATE_METRIC = "vectorized_kafka_rpc_active_connections"


class ConnectionRateLimitTest(PreallocNodesTest):
    MSG_SIZE = 1000000
    PRODUCE_COUNT = 5000

    topics = (TopicSpec(partition_count=1, replication_factor=1),)

    def __init__(self, test_context):
        resource_setting = ResourceSettings(num_cpus=1)
        super(ConnectionRateLimitTest, self).__init__(
            test_context=test_context,
            num_brokers=1,
            node_prealloc_count=1,
            resource_settings=resource_setting,
        )

        self._producer = KgoVerifierProducer(
            test_context,
            self.redpanda,
            self.topics[0],
            self.MSG_SIZE,
            self.PRODUCE_COUNT,
            self.preallocated_nodes,
        )
        self.consumers = []

    def make_consumer(self, retry_sec):
        return RpkConsumer(
            context=self.test_context,
            redpanda=self.redpanda,
            topic=self.topics[0],
            num_msgs=self.PRODUCE_COUNT,
            save_msgs=False,
            retry_sec=retry_sec,
        )

    def stop_consumer(self, consumer):
        try:
            consumer.stop()
        except Exception:
            # Should ignore exception form kaf_consumer
            pass
        consumer.free()

    def make_consumers(self, n_consumers, retry_sec):
        for _ in range(n_consumers):
            self.consumers.append(self.make_consumer(retry_sec))

    def start_consumers(self):
        for c in self.consumers:
            c.start()

    def consumers_finished(self):
        return all([c.message_count == self.PRODUCE_COUNT for c in self.consumers])

    def stop_consumers(self):
        for c in self.consumers:
            self.stop_consumer(c)
        self.consumers = []

    @skip_debug_mode
    @cluster(num_nodes=8)
    def connection_rate_test(self):
        self._producer.start()
        self._producer.wait()
        self._producer.free()

        metrics = MetricCheck(
            self.logger, self.redpanda, self.redpanda.nodes[0], RATE_METRIC, {}
        )

        n_consumers = 6

        for rate_limit in [1, 2, 4]:
            self.logger.info(f"Checking rate_limit: {rate_limit}")
            self.redpanda.set_cluster_config(
                {"kafka_connection_rate_limit": rate_limit}, expect_restart=False
            )
            self.make_consumers(n_consumers, retry_sec=1.0 / rate_limit)
            self.start_consumers()

            old_connections = None

            def eval(_, current_connections):
                nonlocal old_connections
                if not old_connections:
                    old_connections = current_connections
                    return False
                rate = int(current_connections - old_connections)
                old_connections = current_connections
                self.logger.debug(f"Connection rate: {rate}")
                # As the rate of connections is calculated through a metric,
                # it is still quite reliant on timing effects. To try to account
                # for these, as scale factor of 2 is used.
                assert rate <= 2 * rate_limit, (
                    f"Expected rate <= 2*{rate_limit}. Got rate = {rate}."
                )
                return True

            def check_metrics():
                metrics.evaluate([(RATE_METRIC, eval)])
                return self.consumers_finished()

            wait_until(check_metrics, 90, backoff_sec=0.95)

            self.stop_consumers()
