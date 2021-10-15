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

from rptest.services.compatibility.compat_example import CompatExample
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
import math


class FranzGoBase(RedpandaTest):
    """
    Test FranzGo bench example.
    Not using any other example because bench tests the same
    APIs and code that the other examples do.
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context, enable_sasl=False):
        # idempotence is necessary for bench example
        extra_rp_conf = {
            "enable_idempotence": True,
            "enable_sasl": enable_sasl
        }
        self._ctx = test_context

        super(FranzGoBase, self).__init__(test_context=test_context,
                                          extra_rp_conf=extra_rp_conf)

        # In CI, it takes approx 300s to produce/consume 1mill records (or 3333.33 records/s).
        # The closest factor of 1mill is 320s (or 3125 records/s).
        # Also, add 30 seconds because sometimes franzgo doesn't produce/consume
        # immediatly since RP is in middle of election or other
        # background tasks.
        gen_timeout = lambda r: math.ceil(r / 3125) + 30

        # A representation of the bench producer endpoint
        self._prod_conf = {
            "consume": False,
            # Amount of records to produce.
            "max_records": 1000 if self.scale.local else 1000000,
            "enable_sasl": enable_sasl
        }
        self._prod_conf["timeout"] = gen_timeout(
            self._prod_conf["max_records"])
        self.logger.debug(self._prod_conf["timeout"])

        # A representation of the bench consumer endpoint
        self._cons_conf = {
            "consume": True,
            # Amount of records to consume.
            "max_records": 1000 if self.scale.local else 1000000,
            "enable_sasl": enable_sasl
        }
        self._cons_conf["timeout"] = gen_timeout(
            self._cons_conf["max_records"])
        self.logger.debug(self._cons_conf["timeout"])

        self._producer = None
        self._consumer = None

    def setUp(self):
        if self._producer or self._consumer:
            raise RuntimeError("producer or consumer bench already init")

        self._producer = CompatExample(self._ctx,
                                       self.redpanda,
                                       self.topic,
                                       extra_conf=self._prod_conf)
        self._consumer = CompatExample(self._ctx,
                                       self.redpanda,
                                       self.topic,
                                       extra_conf=self._cons_conf)

        super().setUp()


class FranzGoWithoutGroupTest(FranzGoBase):
    """
    Test FranzGo bench example without group consuming.
    """
    def __init__(self, test_context):
        super(FranzGoWithoutGroupTest,
              self).__init__(test_context=test_context)

    @cluster(num_nodes=5)
    def test_franzgo_bench_wo_group(self):
        # Start the produce bench
        self._producer.start()

        # Wait until the example is ok.
        wait_until(lambda: self._producer.ok(),
                   timeout_sec=self._prod_conf["timeout"],
                   backoff_sec=5,
                   err_msg="franz-go bench_wo_group produce test failed")

        # Start the consume bench.
        # Running the example sequentially because
        # it's easier to debug.
        self._consumer.start()

        # Wait until the example is OK to terminate
        wait_until(lambda: self._consumer.ok(),
                   timeout_sec=self._cons_conf["timeout"],
                   backoff_sec=5,
                   err_msg="franz-go bench_wo_group consume test failed")


class FranzGoWithGroupTest(FranzGoBase):
    """
    Test FranzGo bench example with group consuming.
    """
    def __init__(self, test_context):
        super(FranzGoWithGroupTest, self).__init__(test_context=test_context)

        # Add group to the consumer configuration
        self._cons_conf[
            "group"] = f"group-{self.topics[0]._random_topic_suffix()}"

    @cluster(num_nodes=5)
    def test_franzgo_bench_w_group(self):
        # Start the produce bench
        self._producer.start()

        # Wait until the example is ok.
        wait_until(lambda: self._producer.ok(),
                   timeout_sec=self._prod_conf["timeout"],
                   backoff_sec=5,
                   err_msg="franz-go bench_w_group produce test failed")

        # Start the consume bench.
        # Running the example sequentially because
        # it's easier to debug.
        self._consumer.start()

        # Wait until the example is OK to terminate
        wait_until(lambda: self._consumer.ok(),
                   timeout_sec=self._cons_conf["timeout"],
                   backoff_sec=5,
                   err_msg="franz-go bench_w_group consume test failed")


class FranzGoAuthTest(FranzGoBase):
    """
    Test FranzGo bench example using sasl authentication.
    """
    def __init__(self, test_context):
        super(FranzGoAuthTest, self).__init__(test_context=test_context,
                                              enable_sasl=True)

    @cluster(num_nodes=5)
    def test_franzgo_bench_w_auth(self):
        # Start the produce bench
        self._producer.start()

        # Wait until the example is ok.
        wait_until(lambda: self._producer.ok(),
                   timeout_sec=self._prod_conf["timeout"],
                   backoff_sec=5,
                   err_msg="franz-go bench_no_auth produce test failed")

        # Start the consume bench.
        # Running the example sequentially because
        # it's easier to debug.
        self._consumer.start()

        # Wait until the example is OK to terminate
        wait_until(lambda: self._consumer.ok(),
                   timeout_sec=self._cons_conf["timeout"],
                   backoff_sec=5,
                   err_msg="franz-go bench_no_auth consume test failed")
