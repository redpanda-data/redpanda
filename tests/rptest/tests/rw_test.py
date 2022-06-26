# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.services.rw_workload import RWWorkload

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool


class WRWorkloadTest(RedpandaTest):
    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_idempotence": True,
            "id_allocator_replication": 3,
            "default_topic_replications": 3,
            "default_topic_partitions": 1,
            "enable_leader_balancer": False,
            "enable_auto_rebalance_on_node_add": False
        }

        super(WRWorkloadTest, self).__init__(test_context=test_context,
                                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=4)
    def test_rw(self):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic("topic1", partitions=2)

        rw_workload = RWWorkload(self.test_context, self.redpanda)
        rw_workload.start()

        rw_workload.remote_start(self.redpanda.brokers(), "topic1", 2)
        self.logger.info(f"Waiting for 1000 r&w operations")
        rw_workload.ensure_progress(1000, 10)
        self.logger.info(f"Done")
        rw_workload.remote_stop()
        rw_workload.remote_wait()
