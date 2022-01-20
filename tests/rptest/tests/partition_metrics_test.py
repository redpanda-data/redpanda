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

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec


class PartitionMetricsTest(RedpandaTest):
    """
    Produce and consume some data then confirm partition metrics
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context):
        super(PartitionMetricsTest, self).__init__(test_context=test_context)

    def _bytes_produced(self):
        return self._sum_metrics(
            "vectorized_cluster_partition_bytes_produced_total")

    def _records_produced(self):
        return self._sum_metrics(
            "vectorized_cluster_partition_records_produced")

    def _bytes_fetched(self):
        return self._sum_metrics(
            "vectorized_cluster_partition_bytes_fetched_total")

    def _records_fetched(self):
        return self._sum_metrics(
            "vectorized_cluster_partition_records_fetched")

    def _sum_metrics(self, metric_name):

        family = self.redpanda.metrics_sample(metric_name,
                                              nodes=self.redpanda.nodes)
        total = 0
        for sample in family.samples:
            self.redpanda.logger.info(
                f"value: {sample.family} - {sample.value}")
            total += sample.value

        return total

    @cluster(num_nodes=3)
    def test_partition_metrics(self):
        num_records = 10240
        records_size = 512

        # initially all metrics have to be equal to 0
        assert self._bytes_produced() == 0
        assert self._records_produced() == 0

        assert self._bytes_fetched() == 0
        assert self._records_fetched() == 0

        # Produce some data (10240 records * 512 bytes = 5MB of data)
        kafka_tools = KafkaCliTools(self.redpanda)
        kafka_tools.produce(self.topic, num_records, records_size, acks=-1)

        rec_produced = self._records_produced()
        self.redpanda.logger.info(f"records produced: {rec_produced}")
        assert rec_produced == num_records
        bytes_produced = self._bytes_produced()
        self.redpanda.logger.info(f"bytes produced: {bytes_produced}")
        # bytes produced should be bigger than sent records size because of
        # batch headers overhead
        assert bytes_produced >= num_records * records_size

        # fetch metrics shouldn't change
        assert self._bytes_fetched() == 0
        assert self._records_fetched() == 0

        # read all messages
        rpk = RpkTool(self.redpanda)
        rpk.consume(self.topic, n=num_records)

        rec_fetched = self._records_fetched()
        self.redpanda.logger.info(f"records fetched: {rec_fetched}")

        bytes_fetched = self._bytes_fetched()
        self.redpanda.logger.info(f"bytes fetched: {bytes_fetched}")

        assert bytes_fetched == bytes_produced
        assert rec_fetched == rec_produced
