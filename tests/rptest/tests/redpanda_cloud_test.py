# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
from ducktape.tests.test import TestContext
from rptest.clients.default import DefaultClient
from rptest.services.redpanda import make_redpanda_cloud_service
from rptest.tests.redpanda_test import RedpandaTestBase


class RedpandaCloudTest(RedpandaTestBase):
    """
    Base class for tests which run only against the Redpanda Cloud.
    """
    def __init__(self, test_context: TestContext):

        super().__init__(test_context=test_context)

        self.redpanda = make_redpanda_cloud_service(test_context)
        self._client = DefaultClient(self.redpanda)

        # for easy access but we should fix callers and remove this
        self.config_profile_name = self.redpanda.config_profile_name

    def setup(self):
        super().setup()
        wait_until(lambda: self.redpanda.cluster_healthy(),
                   timeout_sec=20,
                   backoff_sec=5,
                   err_msg='cluster unhealthy before start of test')

    def client(self):
        return self._client
