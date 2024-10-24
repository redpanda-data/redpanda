# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any, List, cast
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec


class BytesSentTest(RedpandaTest):
    """
    Produce and consume some data then confirm the bytes sent metric.
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context):
        super(BytesSentTest, self).__init__(test_context=test_context)

        self._ctx = test_context

        # Using a member variable to store counted bytes
        # otherwise nested functions such as count_bytes()
        # would treat the variable as a local instance
        self._bytes_received = 0

    def _bytes_sent(self):
        bytes_sent = 0

        for node in self.redpanda.nodes:
            # Convert the metrics generator to a list
            metrics = list(self.redpanda.metrics(node))

            def filter_sent(fam: Any):
                return fam.name == "vectorized_kafka_rpc_sent_bytes"

            # Find the metric family that tracks bytes sent from kafka subsystem
            family_filter = filter(filter_sent, metrics)

            family = next(family_filter)

            # Sum bytes sent
            for sample in cast(List[Any], family.samples):
                bytes_sent += sample.value

        return bytes_sent

    def _consume_and_count_bytes(self):
        consumer = RpkConsumer(self._ctx, self.redpanda, self.topic)
        consumer.start()

        self._bytes_received = 0

        def count_bytes():
            for msg in consumer.messages:
                value = msg["value"]

                # Ignore None values
                if value is None:
                    return False

                self._bytes_received += len(value)

            # Returns true the first time any bytes
            # are fetched.
            return self._bytes_received > 0

        wait_until(count_bytes,
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg="count_bytes() failed")

        consumer.stop()

    @cluster(num_nodes=4)
    def test_bytes_sent(self):
        num_records = 10240
        records_size = 512

        # Produce some data (10240 records * 512 bytes = 5MB of data)
        kafka_tools = KafkaCliTools(self.redpanda)
        kafka_tools.produce(self.topic, num_records, records_size, acks=-1)

        # Establish the current counter which won't be zero, but
        # we don't really care what the actual value is
        start_bytes = self._bytes_sent()
        self.logger.debug(f"Start bytes: {start_bytes}")

        # Consume and count the bytes. The counted bytes
        # will be in bytes_received
        self._consume_and_count_bytes()
        self.logger.debug(f"Bytes received: {self._bytes_received}")

        # Get the number of bytes sent up to this point
        # and calcuate total
        end_bytes = self._bytes_sent()
        total_sent = end_bytes - start_bytes
        self.logger.debug(f"End bytes: {end_bytes}")
        self.logger.debug(f"Total sent: {total_sent}")

        def in_percent_threshold(n1: int, n2: int, threshold: float):
            percent_increase = (abs(n2 - n1) / n2) * 100
            self.logger.debug(
                f"Percent increase: {percent_increase}, Threshold: {threshold}"
            )
            return threshold >= percent_increase

        # Expect total to be larger than bytes_received
        # because bytes_received doesn't account for other
        # responses between the client and brokers
        assert total_sent > self._bytes_received

        # Total bytes sent should be within 5% threshold.
        # Threshold of 5% is generally an OK place to start
        assert in_percent_threshold(total_sent,
                                    self._bytes_received,
                                    threshold=5.0)
