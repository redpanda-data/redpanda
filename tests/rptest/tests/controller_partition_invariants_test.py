# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool

from ducktape.mark import matrix
from rptest.services.metrics_check import MetricCheck

import time


class ControllerLogInvariantsTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            num_brokers=3,
            **kwargs,
            # disable controller snapshots
            extra_rp_conf={'controller_snapshot_max_age_sec': 3600})

    @cluster(num_nodes=3)
    @matrix(cluster_cleanup_policy=['compact', 'delete'])
    def test_controller_is_not_compacted_nor_deleted(self,
                                                     cluster_cleanup_policy):

        metrics = [
            MetricCheck(self.logger, self.redpanda, n,
                        re.compile("vectorized_storage_log.*"),
                        {'topic': 'controller'}) for n in self.redpanda.nodes
        ]

        rpk = RpkTool(self.redpanda)
        for _ in range(10):
            for _ in range(20):
                # create and delete the same topic
                rpk.create_topic('topic-to-compact', partitions=1, replicas=3)
                rpk.delete_topic('topic-to-compact')
            # transfer controller leadership to trigger segment roll
            Admin(self.redpanda).partition_transfer_leadership(
                namespace="redpanda", topic="controller", partition=0)

        self.redpanda.set_cluster_config({
            "log_compaction_interval_ms":
            1000,
            'log_cleanup_policy':
            cluster_cleanup_policy
        })

        time.sleep(30)
        for m in metrics:
            m.expect([("vectorized_storage_log_compacted_segment_total",
                       lambda a, b: b == a == 0),
                      ("vectorized_storage_log_log_segments_removed_total",
                       lambda a, b: b == a == 0)])
