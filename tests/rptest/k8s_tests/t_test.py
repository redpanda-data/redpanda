# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.tests.test import Test
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaServiceK8s


class TTest(Test):
    def __init__(self, test_context):
        """
        Keep it simple.
        """
        super(TTest, self).__init__(test_context)
        self.redpanda = RedpandaServiceK8s(test_context, 1)

    @cluster(num_nodes=1, check_allowed_error_logs=False, node_type='vm')
    def test_t(self):
        assert 2 > 1
