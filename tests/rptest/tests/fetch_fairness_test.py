# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import sys
import time
import re
import random

from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.mark import ignore
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kcl import KCL
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.types import TopicSpec
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.rpk_producer import RpkProducer
from rptest.services.kaf_producer import KafProducer
from rptest.services.admin import Admin


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
            return list(
                map(
                    lambda i: TopicSpec(partition_count=1,
                                        replication_factor=1), range(0,
                                                                     count)))

        def multiple_partitions(count):
            return [TopicSpec(partition_count=count, replication_factor=1)]

        number_of_partitions = 32
        records_per_partition = 10
        topics = []

        if type == 'multiple-topics':
            topics = multiple_topics(number_of_partitions)
        else:
            topics = multiple_partitions(number_of_partitions)

        # create topics
        self.redpanda.create_topic(specs=topics)
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

        kcl = KCL(self.redpanda)
        consumed = kcl.consume(topic="topic-.*",
                               n=number_of_partitions,
                               regex=True,
                               fetch_max_bytes=1,
                               group="test-gr-1")
        consumed_partitions = set()
        for c in consumed.split():
            parts = c.split('.')
            consumed_partitions.add((parts[1], parts[2]))

        assert len(consumed_partitions) == number_of_partitions
