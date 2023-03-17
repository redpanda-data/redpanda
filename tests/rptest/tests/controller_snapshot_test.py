# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool
from rptest.util import wait_until_result

from ducktape.utils.util import wait_until


class ControllerSnapshotTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         num_brokers=3,
                         extra_rp_conf={'controller_snapshot_max_age_sec': 5},
                         **kwargs)

    def setUp(self):
        # start the nodes manually
        pass

    @cluster(num_nodes=3)
    def test_snapshotting_policy(self):
        """
        Test that Redpanda creates a controller snapshot some time after controller commands appear.
        """
        self.redpanda.start()
        node = self.redpanda.nodes[0]
        assert self.redpanda.controller_start_offset(node) == 0

        admin = Admin(self.redpanda)
        admin.put_feature("controller_snapshots", {"state": "active"})

        # first snapshot will be triggered by the feature_update command
        (mtime1,
         start_offset1) = self.redpanda.wait_for_controller_snapshot(node)

        # second snapshot will be triggered by the topic creation
        RpkTool(self.redpanda).create_topic('test')
        self.redpanda.wait_for_controller_snapshot(
            node, prev_mtime=mtime1, prev_start_offset=start_offset1)
