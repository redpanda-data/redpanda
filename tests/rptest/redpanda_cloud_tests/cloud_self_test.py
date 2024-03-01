# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any
from ducktape.tests.test import TestContext
from rptest.tests.redpanda_cloud_test import RedpandaCloudTest
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaServiceCloud


class SelfRedpandaCloudTest(RedpandaCloudTest):
    """This test verifies RedpandaCloudTest works as expected. It is not a
    test of redpanda itself. It is for verifying the RedpandaCloudTest class
    functions as expected for test authors to inherit from the class and
    create tests against Redpanda Cloud.
    """
    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(test_context, *args, **kwargs)

        assert isinstance(
            self.redpanda,
            RedpandaServiceCloud), 'test should only run on cloud'

    @cluster(num_nodes=1)
    def test_simple(self):
        """Simple test of startup()
        """
        pass

    @cluster(num_nodes=0)
    def test_healthy(self):
        r = self.redpanda.cluster_unhealthy_reason()
        assert r is None, r
        assert self.redpanda.cluster_healthy()
        self.redpanda.assert_cluster_is_reusable()

    @cluster(num_nodes=1)
    def test_metrics_sample(self):
        """Test metrics_sample() can retrieve internal metrics.
        """
        vectorized_application_uptime = self.redpanda.metrics_sample(
            sample_pattern='vectorized_application_uptime')
        assert vectorized_application_uptime is not None, 'expected some metrics'

    @cluster(num_nodes=1)
    def test_metrics_samples(self):
        """Test metrics_samples() can retrieve internal metrics.
        """
        sample_patterns = [
            'vectorized_application_uptime', 'vectorized_reactor_utilization'
        ]
        samples = self.redpanda.metrics_samples(sample_patterns)
        assert samples is not None, 'expected sample patterns to match'

    @cluster(num_nodes=1)
    def test_metric_sum(self):
        """Test metric_sum() can retrieve internal metrics.
        """

        count = self.redpanda.metric_sum('vectorized_application_uptime')
        assert count > 0, 'expected count greater than 0'
