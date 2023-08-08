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
from rptest.services.redpanda_cloud import RedpandaServiceK8s


class SimpleK8sTest(Test):
    def __init__(self, test_context):
        """
        Keep it simple.
        """
        super(SimpleK8sTest, self).__init__(test_context)
        self.redpanda = RedpandaServiceK8s(test_context, 1)

    @cluster(num_nodes=1, check_allowed_error_logs=False)
    def test_k8s(self):
        '''
        Validate startup of k8s.
        '''
        self.redpanda.start_node(None)
        node_memory = float(self.redpanda.get_node_memory_mb())
        assert node_memory > 1.0

        node_cpu_count = self.redpanda.get_node_cpu_count()
        assert node_cpu_count > 0

        node_disk_free = self.redpanda.get_node_disk_free()
        assert node_disk_free > 0

        self.redpanda.lsof_node(1)

        self.redpanda.set_cluster_config({})
