# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

from ducktape.tests.test import Test
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService


class RedpandaBinaryTest(Test):
    """
    Test class for testing the redpanda binary without necessarily running the
    redpanda service.
    """
    def __init__(self, test_context):
        super(RedpandaBinaryTest, self).__init__(test_context=test_context)
        self.redpanda = RedpandaService(self.test_context, 1)

    @cluster(num_nodes=1, check_allowed_error_logs=False)
    def test_version(self):
        version_cmd = f"{self.redpanda.find_binary('redpanda')} --version"
        version_lines = [
            l for l in self.redpanda.nodes[0].account.ssh_capture(version_cmd)
        ]
        assert len(version_lines) == 1, version_lines
        version_regex_str = "v\\d+\\.\\d+\\.\\d+.*"  # E.g. "v22.1.1-rc1-1373-g77f868..."
        version_re = re.compile(version_regex_str)
        assert version_re.search(
            version_lines[0]
        ), f"Expected '{version_lines[0]}' to match '{version_regex_str}'"
