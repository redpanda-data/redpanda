# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import time
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from ducktape.utils.util import wait_until
import confluent_kafka as ck
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.rpk import parse_rpk_table
from rptest.util import wait_until_result


class DescribeProducersTest(RedpandaTest):
    partition_count = 3
    topics = (TopicSpec(partition_count=partition_count, replication_factor=3),
              TopicSpec(partition_count=partition_count, replication_factor=3))

    def __init__(self, test_context):
        super(DescribeProducersTest, self).__init__(test_context=test_context,
                                                    num_brokers=3)

        self.kafka_cli = KafkaCliTools(self.redpanda, "3.0.0")

    def _describe_all_producers(self):
        all_producers = []
        for topic in self.topics:
            for partition in range(topic.partition_count):
                producers = self.kafka_cli.describe_producers(
                    topic.name, partition)
                all_producers += producers
        return all_producers

    def _check_timestamp(self, producer_desc, range_start, range_end):
        # convert to python representation of epoch
        ts = int(producer_desc['LastTimestamp']) / 1000.0
        assert ts >= range_start and ts <= range_end, \
            f"Producer timestamp must correspond to system clock. Returned timestamp: {ts}, range: [{range_start}, {range_end}]"

    @cluster(num_nodes=3)
    def test_describe_producer_with_tx(self):
        before = time.time()
        producer_count = 20
        producers = []
        for idx in range(producer_count):
            producer = ck.Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'transactional.id': f'tx-producer-{idx}',
            })
            producer.init_transactions()
            producers.append(producer)

        all_producers_desc = self._describe_all_producers()

        assert len(all_producers_desc
                   ) == 0, "Before producing data producers should be empty"

        for idx, producer in enumerate(producers):
            producer.begin_transaction()
            producer.produce(self.topics[idx % len(self.topics)].name,
                             f'key-{idx}', f'value-{idx}',
                             idx % self.partition_count)
            producer.flush()

        def _all_producers():
            all = self._describe_all_producers()
            if len(all) == producer_count:
                return (True, all)
            return (False, None)

        all_producers_desc = wait_until_result(_all_producers, 30, 1)

        after = time.time()
        assert len(
            all_producers_desc
        ) == producer_count, f"Unexpected size of producers list, expected: {producer_count}, current: {len(all_producers_desc)}"
        for p in all_producers_desc:
            self.logger.info(f"producer state with transaction ongoing: {p}")
            self._check_timestamp(p, before, after)
            # for every partition transaction is the first batch, validate initial offset
            so = int(p['CurrentTransactionStartOffset'])
            assert so >= 0, "Transaction start offset should be a part of producer state"

        # commit transactions
        for producer in producers:
            producer.commit_transaction()
            producer.flush()

        all_producers_desc = self._describe_all_producers()
        after = time.time()
        assert len(
            all_producers_desc
        ) == producer_count, f"Unexpected size of producers list, expected: {producer_count}, current: {len(all_producers_desc)}"
        for p in all_producers_desc:
            self.logger.info(f"producer state with transaction committed: {p}")
            self._check_timestamp(p, before, after)
            # for every partition transaction is the first batch, validate initial offset
            assert p[
                'CurrentTransactionStartOffset'] == 'None', "Transaction start offset should be a part of producer state"

    @cluster(num_nodes=3)
    def test_describe_idempotent_producers(self):
        before = time.time()
        rpk = RpkTool(self.redpanda)
        producer_count = 20
        for i in range(producer_count):
            rpk.produce(self.topics[i % len(self.topics)].name,
                        "test-key",
                        "test-msg",
                        partition=i % self.partition_count)

        all_producers = self._describe_all_producers()

        after = time.time()
        assert len(
            all_producers
        ) == producer_count, f"Unexpected size of producers list, expected: {producer_count}, current: {len(all_producers)}"
        for p in all_producers:
            self.logger.info(f"producer state: {p}")
            self._check_timestamp(p, before, after)
            assert p[
                'CurrentTransactionStartOffset'] == 'None', "Idempotent producers should not have first transaction offset"
