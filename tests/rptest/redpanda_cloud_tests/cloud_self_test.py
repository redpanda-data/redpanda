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
from rptest.clients.rpk import RpkTool
from rptest.tests.redpanda_cloud_test import RedpandaCloudTest
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaServiceCloud


class RedpandaCloudSelfTest(RedpandaCloudTest):
    """This test verifies the parts of RedpandaCloudTest that can *only* run
    on the cloud work as expected. For things that are implemented by
    RedpandaMixedTest, they should be tested in RedpandaMixedSelfTest instead.
    """
    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        super().__init__(test_context, *args, **kwargs)

        assert isinstance(
            self.redpanda,
            RedpandaServiceCloud), 'test should only run on cloud'

    @cluster(num_nodes=0)
    def test_healthy(self):
        r = self.redpanda.cluster_unhealthy_reason()
        assert r is None, r
        assert self.redpanda.cluster_healthy()
        self.redpanda.assert_cluster_is_reusable()
