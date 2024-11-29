# Copyright 2023 Redpanda Data, Inc.
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

from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.tests.e2e_finjector import Finjector
from rptest.tests.prealloc_nodes import PreallocNodesTest


class QuickTerminateTest(PreallocNodesTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(test_ctx,
                         num_brokers=3,
                         *args,
                         node_prealloc_count=1,
                         **kwargs)

    @cluster(num_nodes=3, log_allow_list=Finjector.LOG_ALLOW_LIST)
    def test_terminate(self):
        for _ in range(10):
            node = random.choice(self.redpanda.started_nodes())
            self.redpanda.stop_node(node)
            time.sleep(random.uniform(0, 1))
            self.redpanda.start_node(node, skip_readiness_check=True)
            time.sleep(random.uniform(0, 1))
