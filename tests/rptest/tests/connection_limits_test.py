# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import socket

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.rpk_producer import RpkProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.clients.types import TopicSpec
from rptest.services.metrics_check import MetricCheck
from rptest.util import expect_exception

REJECTED_METRIC = "vectorized_kafka_rpc_connections_rejected_total"
ACTIVE_METRIC = "vectorized_kafka_rpc_active_connections"


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

        def connection_count():
            return self.redpanda.metric_sum(ACTIVE_METRIC)

        # wait until the connection count reaches the expected number before starting the
        # producer, since otherwise the consumers and the producer race and the producer
        # may win in which case it would be one of the consumers that fail to connect
        self.redpanda.wait_until(
            lambda: connection_count() >= 6, 60, 1,
            f"Did not get 6 connections, last count was {connection_count()}")

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

    @cluster(num_nodes=3)
    def test_overrides(self):
        """
        Check that per-IP overrides are applied correctly.  More
        detailed tests are in the unit test: this is a smoke test
        that some overrides work end to end.
        """
        def setup_producers():
            producer_a = RpkProducer(self.test_context,
                                     self.redpanda,
                                     self.topic,
                                     msg_size=16384,
                                     msg_count=1,
                                     quiet=True,
                                     produce_timeout=5)
            producer_a_addr = socket.gethostbyname(producer_a.nodes[0].name)

            producer_b = RpkProducer(self.test_context,
                                     self.redpanda,
                                     self.topic,
                                     msg_size=16384,
                                     msg_count=1,
                                     quiet=True,
                                     produce_timeout=5)
            producer_b_addr = socket.gethostbyname(producer_b.nodes[0].name)

            return producer_a, producer_a_addr, producer_b, producer_b_addr

        producer_a, producer_a_addr, producer_b, producer_b_addr = setup_producers(
        )

        self.logger.info(
            f"producer_a: {producer_a_addr}, producer_b: {producer_b_addr}")

        self.redpanda.set_cluster_config(
            {"kafka_connections_max_overrides": [
                f"{producer_a_addr}:0",
            ]})

        self.logger.info(f"Trying producer_a, should be blocked")
        with expect_exception(Exception,
                              lambda e: 'timeout' in str(e).lower()):
            producer_a.start()
            producer_a.wait()

        self.logger.info(f"Tearing down producer_a")
        with expect_exception(Exception,
                              lambda e: 'timeout' in str(e).lower()):
            producer_a.stop()
        producer_a.free()

        self.logger.info(f"Trying producer_b, should be OK")
        producer_b.start()
        producer_b.wait()
        producer_b.free()

        self.redpanda.set_cluster_config(
            {"kafka_connections_max_overrides": []})

        producer_a, producer_a_addr, producer_b, producer_b_addr = setup_producers(
        )

        # Both producers should work now
        producer_a.start()
        producer_b.start()
        producer_a.wait()
        producer_b.wait()
        producer_a.free()
        producer_b.free()
