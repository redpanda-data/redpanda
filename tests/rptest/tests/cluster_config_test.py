# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest


class ClusterConfigTest(RedpandaTest):
    def test_get_config(self):
        admin = Admin(self.redpanda)
        config = admin.get_config()

        # Pick an arbitrary config property to verify that the result
        # contained some properties
        assert 'enable_transactions' in config
