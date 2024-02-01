# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
from typing import Sequence

from ducktape.tests.test import Test, TestContext
from rptest.services.redpanda import CloudTierName, SISettings, make_redpanda_cloud_service, make_redpanda_service, CloudStorageType
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.default import DefaultClient
from rptest.tests.redpanda_test import RedpandaTestBase
from rptest.util import Scale
from rptest.utils import mode_checks
from rptest.clients.types import TopicSpec
from rptest.services.redpanda_installer import RedpandaInstaller, RedpandaVersion, RedpandaVersionLine, RedpandaVersionTriple
from rptest.clients.rpk import RpkTool


class RedpandaCloudTest(RedpandaTestBase):
    """
    Base class for tests which run against the Redpanda Cloud.
    """
    def __init__(self, test_context: TestContext):

        super().__init__(test_context=test_context)

        self.redpanda = make_redpanda_cloud_service(test_context)
        self._client = DefaultClient(self.redpanda)

        # for easy access but we should fix callers and remove this
        self.config_profile_name = self.redpanda.config_profile_name

    def setup(self):
        super().setup()
        assert self.redpanda.cluster_healthy(
        ), 'cluster unhealthy before start of test'

    def client(self):
        return self._client
