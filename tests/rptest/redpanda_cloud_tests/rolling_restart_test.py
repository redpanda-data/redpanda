# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
from typing import Any
from ducktape.tests.test import TestContext
from rptest.tests.redpanda_cloud_test import RedpandaCloudTest
from rptest.services.cluster import cluster


class RollingRestartTest(RedpandaCloudTest):
    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(test_context, *args, **kwargs)

    @cluster(num_nodes=1)
    def test_restart_pod(self):
        """Simple test of restart_pod() with a random pod in the cluster.
        """

        pod = random.choice(self.redpanda.pods)
        self.logger.info(f'test restart of pod {pod.name}')
        self.redpanda.restart_pod(pod.name)

    @cluster(num_nodes=1)
    def test_rolling_restart(self):
        """Simple test of rolling_restart_pods() with default args."""

        self.logger.info('test rolling restart of all pods in the cluster')
        self.redpanda.rolling_restart_pods()
