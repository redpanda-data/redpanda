# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.services.compatibility.example_runner import ExampleRunner
import rptest.services.compatibility.franzgo_examples as FranzGoExamples
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SecurityConfig
from rptest.clients.types import TopicSpec
import math


class FranzGoBase(RedpandaTest):
    """
    Test FranzGo bench example.
    Not using any other example because bench tests the same
    APIs and code that the other examples do.
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context, enable_sasl=False, group=None):
        # idempotence is necessary for bench example
        extra_rp_conf = {
            "enable_idempotence": True,
        }
        self._ctx = test_context

        security = SecurityConfig()
        security.enable_sasl = enable_sasl

        super(FranzGoBase, self).__init__(test_context=test_context,
                                          security=security,
                                          extra_rp_conf=extra_rp_conf)

        self._max_records = 1000 if self.scale.local else 1000000
        self._enable_sasl = enable_sasl

        # In CI, it takes approx 300s to produce/consume 1mill records (or 3333.33 records/s).
        # The closest factor of 1mill is 320s (or 3125 records/s).
        # Also, add 30 seconds because sometimes franzgo doesn't produce/consume
        # immediatly since RP is in middle of election or other
        # background tasks.
        self._timeout = math.ceil(self._max_records / 3125) + 30
        self.logger.debug(self._timeout)

        franzgo_producer = FranzGoExamples.FranzGoBenchProduce(
            self.redpanda, self.topic, self._max_records, self._enable_sasl)
        self._producer = ExampleRunner(self._ctx,
                                       franzgo_producer,
                                       timeout_sec=self._timeout)

        franzgo_consumer = FranzGoExamples.FranzGoBenchConsume(
            self.redpanda, self.topic, self._max_records, self._enable_sasl,
            group)
        self._consumer = ExampleRunner(self._ctx,
                                       franzgo_consumer,
                                       timeout_sec=self._timeout)

    @cluster(num_nodes=5)
    def test_franzgo_bench(self):
        # Start the produce bench
        self._producer.start()
        wait_until(self._producer.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)

        # Start the consume bench.
        # Running the example sequentially because
        # it's easier to debug.
        self._consumer.start()
        wait_until(self._consumer.condition_met,
                   timeout_sec=self._timeout,
                   backoff_sec=1)


class FranzGoWithoutGroupTest(FranzGoBase):
    """
    Test FranzGo bench example without group consuming.
    """
    def __init__(self, test_context):
        super(FranzGoWithoutGroupTest,
              self).__init__(test_context=test_context)


class FranzGoWithGroupTest(FranzGoBase):
    """
    Test FranzGo bench example with group consuming.
    """
    def __init__(self, test_context):
        suffix_gen = TopicSpec()
        g = f"group-{suffix_gen._random_topic_suffix()}"
        super(FranzGoWithGroupTest, self).__init__(test_context=test_context,
                                                   group=g)


class FranzGoAuthTest(FranzGoBase):
    """
    Test FranzGo bench example using sasl authentication.
    """
    def __init__(self, test_context):
        super(FranzGoAuthTest, self).__init__(test_context=test_context,
                                              enable_sasl=True)
