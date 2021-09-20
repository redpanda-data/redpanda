# Copyright 2020 Vectorized, Inc.
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


class FranzGoAuthTest(RedpandaTest):
    """
    Test FranzGo bench example using sasl authentication.
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context):
        # idempotence is necessary for bench example
        extra_rp_conf = {"enable_idempotence": True, "enable_sasl": True}
        super(FranzGoAuthTest, self).__init__(test_context=test_context,
                                              extra_rp_conf=extra_rp_conf)

        # A representation of the bench producer endpoint
        self._prod_conf = {
            "consume": False,

            # Amount of records to produce.
            # 1mill seems like a good starting point. Could be scaled.
            "max_records": 1000000,

            # Custom timeout of 30s instead of the default 10s
            # because sometimes no records are produced early on
            # which eats-up execution time.
            "timeout": 300,
            "enable_sasl": True
        }
        self._producer = CompatExample(test_context,
                                       self.redpanda,
                                       self.topic,
                                       extra_conf=self._prod_conf)

        # A representation of the bench consumer endpoint
        self._cons_conf = {
            "consume": True,

            # Amount of records to consume
            # 1mill seems like a good starting point. Could be scaled.
            "max_records": 1000000,

            # Custom timeout of 120s instead of the default 10s
            # because the default is not long enough to consume
            # 1mill records.
            "timeout": 300,
            "enable_sasl": True
        }
        self._consumer = CompatExample(test_context,
                                       self.redpanda,
                                       self.topic,
                                       extra_conf=self._cons_conf)

    @cluster(num_nodes=5)
    def test_franzgo_bench_w_auth(self):
        # Start the produce bench
        self._producer.start()

        # Wait until the example is ok.
        # Add 30s to the timeout incase the example
        # took the full amount of time to produce
        wait_until(lambda: self._producer.ok(),
                   timeout_sec=self._prod_conf["timeout"] + 30,
                   backoff_sec=5,
                   err_msg="franz-go bench_no_auth produce test failed")

        # Start the consume bench.
        # Running the example sequentially because
        # it's easier to debug.
        self._consumer.start()

        # Wait until the example is OK to terminate
        # Add 30s to the timeout incase the example
        # took the full amount of time to consume
        wait_until(lambda: self._consumer.ok(),
                   timeout_sec=self._cons_conf["timeout"] + 30,
                   backoff_sec=5,
                   err_msg="franz-go bench_no_auth consume test failed")
