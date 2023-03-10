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

    def _controller_start_offset(self, node):
        metrics = list(self.redpanda.metrics(node))
        for family in metrics:
            if family.name == 'vectorized_cluster_partition_start_offset':
                for s in family.samples:
                    if s.labels['namespace'] == 'redpanda' and s.labels[
                            'topic'] == 'controller':
                        return int(s.value)
        return 0

    def _wait_for_controller_snapshot(self,
                                      node,
                                      prev_mtime=0,
                                      prev_start_offset=0):
        def check():
            storage = self.redpanda.node_storage(node)
            controller = storage.partitions('redpanda', 'controller')
            assert len(controller) == 1
            controller = controller[0]

            mtime = 0
            if 'snapshot' in controller.files:
                mtime = controller.get_mtime('snapshot')

            so = self._controller_start_offset(node)
            self.logger.info(
                f"node {node.account.hostname}: "
                f"controller start offset: {so}, snapshot mtime: {mtime}")

            return (mtime > prev_mtime and so > prev_start_offset, (mtime, so))

        return wait_until_result(check, timeout_sec=30, backoff_sec=1)

    @cluster(num_nodes=3)
    def test_snapshotting_policy(self):
        """
        Test that Redpanda creates a controller snapshot some time after controller commands appear.
        """
        self.redpanda.start()
        node = self.redpanda.nodes[0]
        assert self._controller_start_offset(node) == 0

        admin = Admin(self.redpanda)
        admin.put_feature("controller_snapshots", {"state": "active"})

        # first snapshot will be triggered by the feature_update command
        (mtime1, start_offset1) = self._wait_for_controller_snapshot(node)

        # second snapshot will be triggered by the topic creation
        RpkTool(self.redpanda).create_topic('test')
        self._wait_for_controller_snapshot(node,
                                           prev_mtime=mtime1,
                                           prev_start_offset=start_offset1)
