# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
import random
import signal
import threading
import time

import numpy
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from confluent_kafka import Producer
from confluent_kafka.cimpl import KafkaError
from ducktape.utils.util import wait_until


class ThreadedProducer:
    def __init__(self, topic, redpanda, logger):
        self.thread = threading.Thread(target=lambda: self.produce_loop())
        self.thread.daemon = True
        self.stop_ev = threading.Event()
        self.redpanda = redpanda
        self.logger = logger
        self.producer = None
        self.topic = topic
        self.latencies = []
        self.inflight = threading.Semaphore(20)

    def start(self):
        self.thread.start()

    def produce_loop(self):
        self.producer = Producer({
            "bootstrap.servers": self.redpanda.brokers(),
        })

        def delivery_clb(err: KafkaError, msg, start_time):
            if err:
                self.logger.warn(f"error sending message: {err}")

            self.latencies.append(time.monotonic() - start_time)
            self.inflight.release()

        while not self.stop_ev.is_set():
            self.inflight.acquire()

            start = time.monotonic()
            self.producer.produce(
                topic=self.topic,
                key="test-message-key",
                value="test-message-value",
                on_delivery=lambda err, msg: delivery_clb(err, msg, start))
            self.producer.flush()

    def wait_for_messages(self, messages, timeout_sec):
        wait_until(lambda: len(self.latencies) >= messages,
                   timeout_sec=timeout_sec,
                   backoff_sec=1,
                   err_msg=f"timed out waiting for {messages} messages")

    def stop(self):
        self.stop_ev.set()
        self.inflight.release()
        self.thread.join()


class RaftSlowFollowerTest(RedpandaTest):
    def __init__(self, test_context):
        super(RaftSlowFollowerTest, self).__init__(
            num_brokers=3,
            test_context=test_context,
            extra_rp_conf={
                # disable leader balancer not to introduce variability into the test
                'enable_leader_balancer': False,
            })

    def _get_follower(self, topic, partition):
        rpk = RpkTool(self.redpanda)
        partitions = list(rpk.describe_topic(topic=topic))
        leader_id = partitions[partition].leader
        followers = [
            n for n in self.redpanda.nodes
            if self.redpanda.node_id(n) != leader_id
        ]
        return random.choice(followers)

    @cluster(num_nodes=3)
    def test_single_slow_follower(self):
        topic = TopicSpec(name="latency-test-topic",
                          partition_count=1,
                          replication_factor=3)
        self.client().create_topic(topic)

        try:
            producer = ThreadedProducer(topic.name, self.redpanda, self.logger)
            producer.start()
            producer.wait_for_messages(2000, 100)

            self.logger.info(
                f"average latency: {numpy.average(producer.latencies)*1000} ms, max latency: {numpy.max(producer.latencies)*1000} ms"
            )
            f = self._get_follower(topic.name, 0)
            assert numpy.max(
                producer.latencies
            ) < 2.5, f"Produce latency is unexpectedly high: {numpy.max(producer.latencies)*1000} ms"
            self.redpanda.signal_redpanda(f, signal=signal.SIGSTOP)
            producer.wait_for_messages(12000, 100)
            self.redpanda.signal_redpanda(f, signal=signal.SIGCONT)
            self.logger.info(
                f"average latency: {numpy.average(producer.latencies)*1000} ms, max latency: {numpy.max(producer.latencies)*1000} ms"
            )
            assert numpy.max(
                producer.latencies
            ) < 2.5, f"Produce latency is unexpectedly high: {numpy.max(producer.latencies)*1000} after follower was suspended"
        finally:
            producer.stop()
