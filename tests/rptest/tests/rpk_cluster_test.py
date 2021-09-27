# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from rptest.services.rpk_consumer import RpkConsumer
from ducktape.mark.resource import cluster

import threading
import random


class RpkClusterTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkClusterTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=3)
    def test_cluster_info(self):
        def condition():
            brokers = self._rpk.cluster_info()

            if len(brokers) != len(self.redpanda.nodes):
                return False

            advertised_addrs = self.redpanda.brokers()

            ok = True
            for b in brokers:
                ok = ok and \
                    b.address in advertised_addrs

            return ok

        wait_until(condition,
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="No brokers found or output doesn't match")
