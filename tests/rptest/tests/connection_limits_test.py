# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.rpk_producer import RpkProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.clients.types import TopicSpec
from rptest.services.metrics_check import MetricCheck

REJECTED_METRIC = "vectorized_kafka_rpc_connections_rejected_total"


class ConnectionLimitsTest(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=1), )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=1, **kwargs)

    @cluster(num_nodes=4)
    def test_exceed_broker_limit(self):
        self.redpanda.set_cluster_config({"kafka_connections_max": 6})

        metrics = [
            MetricCheck(self.logger, self.redpanda, n, REJECTED_METRIC, {},
                        sum) for n in self.redpanda.nodes
        ]

        # I happen to know that an `rpk topic consume` occupies three
        # connections.  So after opening two consumers, I should find
        # that a producer cannot get in.
        consumers = [
            RpkConsumer(self.test_context, self.redpanda, self.topic),
            RpkConsumer(self.test_context, self.redpanda, self.topic),
        ]

        for c in consumers:
            c.start()

        producer = RpkProducer(self.test_context,
                               self.redpanda,
                               self.topic,
                               msg_size=16384,
                               msg_count=1,
                               produce_timeout=5)
        producer.start()
        try:
            producer.wait()
        except Exception:
            # This is a non-specific exception because ducktape re-raises in wait()
            # as a bare Exception
            pass
        else:
            raise RuntimeError("Producer should have failed")

        for c in consumers:
            c.stop()
            c.wait()

        assert any([
            m.evaluate([(REJECTED_METRIC, lambda a, b: b > a)])
            for m in metrics
        ])

    @cluster(num_nodes=2)
    def test_null(self):
        """
        The null case where we are never exceeding the limit, but
        are repeatedly creating+destroying connections.
        """
        self.redpanda.set_cluster_config({"kafka_connections_max": 6})

        metrics = [
            MetricCheck(self.logger, self.redpanda, n, REJECTED_METRIC, {},
                        sum) for n in self.redpanda.nodes
        ]

        producer = RpkProducer(self.test_context,
                               self.redpanda,
                               self.topic,
                               msg_size=16384,
                               msg_count=1,
                               quiet=True,
                               produce_timeout=5)
        for n in range(0, 100):
            producer.start()
            producer.wait()

        assert all([
            m.evaluate([(REJECTED_METRIC, lambda a, b: b == a)])
            for m in metrics
        ])
