# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import time
from rptest.services.cluster import cluster
from rptest.clients.rpk import RpkTool
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest


class OffsetRetentionTest(RedpandaTest):
    topics = (TopicSpec(), )
    period = 30

    def __init__(self, test_context):
        # retention time is set to 30 seconds and expired offset queries happen
        # every second. these are likely unrealistic in practice, but allow us
        # to build tests that are quick and responsive.
        super(OffsetRetentionTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=dict(
                                 group_offset_retention_sec=self.period,
                                 group_offset_retention_check_ms=1000,
                             ))

        self.rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=3)
    def test_offset_expiration(self):
        group = "hey_group"

        def offsets_exist():
            desc = self.rpk.group_describe(group)
            return len(desc.partitions) > 0

        # consume from the group for twice as long as the retention period
        # setting and verify that group offsets exist for the entire time.
        start = time.time()
        while time.time() - start < (self.period * 2):
            self.rpk.produce(self.topic, "k", "v")
            self.rpk.consume(self.topic, n=1, group=group)
            wait_until(offsets_exist, timeout_sec=1, backoff_sec=1)
            time.sleep(1)

        # after one half life the offset should still exist
        time.sleep(self.period / 2)
        assert offsets_exist()

        # after waiting for twice the retention period, it should be gone
        time.sleep(self.period * 2 - self.period / 2)
        assert not offsets_exist()
