# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.kcl import KCL


class ListGroupsReplicationFactorTest(RedpandaTest):
    """
    We encountered an issue where listing groups would return a
    coordinator-loading error when the underlying group membership topic had a
    replication factor of 3 (we had not noticed this until we noticed that
    replication factor were defaulted to 1). it isn't clear if this is specific
    to `kcl` but that is the client that we encountered the issue with.
    """
    topics = (TopicSpec(), )

    def __init__(self, test_context):
        extra_rp_conf = dict(default_topic_replications=3, )

        super(ListGroupsReplicationFactorTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_list_groups(self):
        kcl = KCL(self.redpanda)
        kcl.produce(self.topic, "msg\n")
        kcl.consume(self.topic, n=1, group="g0")
        kcl.list_groups()
        out = kcl.list_groups()
        assert "COORDINATOR_LOAD_IN_PROGRESS" not in out
