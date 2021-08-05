# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.compatibility.compat_helpers import operatrio_example


class CoordinatorLoadInProgressTest(RedpandaTest):
    """
    Verify that the coordinator_load_in_progress error does not cause a timeout
    when default_topic_replications is greater than 1
    """
    def __init__(self, test_context):
        extra_rp_conf = {
            "default_topic_replications": 3,
            "default_topic_partitions": 18
        }

        super(CoordinatorLoadInProgressTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_coordinator_load_in_progress(self):
        #Get the operatrio example
        cmd = operatrio_example(self.redpanda)
        node = self.redpanda.get_node(1)

        #Run the example
        #Here, we're not using a BackgroundThreadService
        #because the example runs in the foreground
        result = node.account.ssh_output(cmd, allow_fail=True,
                                         timeout_sec=30).decode()

        assert "[GroupMetadata(test-kpow-group)]" in result, "coordinator load in progress test failed"
