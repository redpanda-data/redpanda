# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.services.compatibility.example_runner import ExampleRunner
from rptest.services.compatibility.flink_cluster import FlinkCluster
import rptest.services.compatibility.flink_examples as FlinkExamples
from rptest.services.kaf_producer import KafProducer
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
import math


class FlinkTest(RedpandaTest):
    """
    Base class for Flink tests that contains all
    shared objects between tests and starts the
    Flink cluster
    """

    # The JOB is the java program that uses Flink with kafka connectors.
    # The java program is represented by a wrapper in FlinkExamples.
    JOB = None

    # The producer should be an extension to KafProducer
    PRODUCER = None

    def __init__(self, test_context):
        super(FlinkTest, self).__init__(test_context=test_context)

        self._ctx = test_context

        self._max_records = 10
        self._timeout = 120

        self._flink_cluster = FlinkCluster(test_context)

    def is_valid_msg(self, msg):
        raise NotImplementedError("is_valid_msg() is undefined")

    def setUp(self):
        self._flink_cluster.start()

        super().setUp()

    @cluster(num_nodes=6)
    def test_flink(self):
        # This will raise TypeError if JOB is undefined
        flink_job = self.JOB(self.redpanda)

        # This will raise TypeError if PRODUCER is undefined
        producer = self.PRODUCER(self._ctx, self.redpanda, self.topics[0].name,
                                 self._max_records)
        consumer = RpkConsumer(self._ctx, self.redpanda, self.topics[1].name)

        # Start the flink job on the
        # cluster node
        self._flink_cluster.run_flink_job(flink_job, self._timeout)

        # Produce some messages
        producer.start()
        producer.wait()

        # Consume the data
        consumer.start()

        def check_msgs():
            msgs = consumer.messages
            if len(msgs) < 1:
                return False

            # Check the last message consumed. A lot of messages may be
            # produced or consumed but only the last message represents
            # a successful test.
            return self.is_valid_msg(msgs[-1])

        wait_until(check_msgs,
                   timeout_sec=self._timeout,
                   backoff_sec=5,
                   err_msg=f"{self._ctx.cls_name} consumer failed")

        consumer.stop()
        producer.stop()


class WordProducer(KafProducer):
    def __init__(self, context, redpanda, topic, records):
        super(WordProducer, self).__init__(context,
                                           redpanda,
                                           topic,
                                           num_records=records)

    def value_gen(self):
        # The WordCount job will see the randomly generated
        # number as a seperate word.
        return "redpanda $((1 + $RANDOM % 3))"


class FlinkWordCount(FlinkTest):
    """
    Test Flink with Kafka using a simple streaming job that consumes
    from an input topic and calculates the frequency for each word in
    the topic.
    """

    # Topic names are hardcoded in the examples. Don't change them.
    topics = (
        TopicSpec(name="input-topic"),
        TopicSpec(name="output-topic"),
    )

    JOB = FlinkExamples.FlinkWordCountJob
    PRODUCER = WordProducer

    def __init__(self, test_context):
        super(FlinkWordCount, self).__init__(test_context=test_context)

    def is_valid_msg(self, msg):
        value = msg["value"]
        # The WordCount job is successfull when the consumer
        # reads "redpanda" max_records times.
        return f"redpanda {self._max_records}" in value
