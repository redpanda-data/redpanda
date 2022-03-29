# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict

from rptest.services.cluster import cluster
from ducktape.mark import parametrize

from rptest.clients.kcl import KCL
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest


class FetchTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        super(FetchTest, self).__init__(test_ctx,
                                        num_brokers=1,
                                        *args,
                                        extra_rp_conf={},
                                        **kwargs)

    """
    Validates key availability properties of the system using a single
    partition.
    """

    @cluster(num_nodes=1)
    @parametrize(type='multiple-topics')
    @parametrize(type='multiple-partitions')
    def simple_fetch_handler_fairness_test(self, type):
        """
        Testing if when fetching from single node all partitions are 
        returned in round robin fashion
        """
        def multiple_topics(count):
            return [
                TopicSpec(partition_count=1, replication_factor=1)
                for _ in range(count)
            ]

        def multiple_partitions(count):
            return [TopicSpec(partition_count=count, replication_factor=1)]

        number_of_partitions = 32
        records_per_partition = 100
        topics = []

        if type == 'multiple-topics':
            topics = multiple_topics(number_of_partitions)
        else:
            topics = multiple_partitions(number_of_partitions)

        # create topics
        self.client().create_topic(specs=topics)
        self.redpanda.logger.info(f"topics: {topics}")
        rpk = RpkTool(self.redpanda)

        # publish 10 messages to each topic/partition

        for s in topics:
            t = s.name
            for p in range(0, s.partition_count):
                for i in range(0, records_per_partition):
                    self.redpanda.logger.info(f"producing to : {t}/{p}")
                    rpk.produce(t,
                                f"k.{t}.{p}.{i}",
                                f"p.{t}.{p}.{i}",
                                partition=p)

        # configure kcl to fetch at most 1 byte in single fetch request,
        # this way we should receive exactly one record per each partition

        consumed_frequency = defaultdict(lambda: 0)
        kcl = KCL(self.redpanda)

        expected_frequency = records_per_partition / 2
        # issue single kcl command that will generate multiple fetch requests
        # in single fetch session, it should include single partition in each
        # fetch response.
        to_consume = int(number_of_partitions * expected_frequency)
        consumed = kcl.consume(topic="topic-.*",
                               n=to_consume,
                               regex=True,
                               fetch_max_bytes=1,
                               group="test-gr-1")

        for c in consumed.split():
            parts = c.split('.')
            consumed_frequency[(parts[1], parts[2])] += 1

        for p, freq in consumed_frequency.items():
            self.redpanda.logger.info(f"consumed {freq} messages from: {p}")
            # assert that frequency is in expected range
            assert freq <= expected_frequency + 0.1 * expected_frequency
            assert freq >= expected_frequency - 0.1 * expected_frequency
