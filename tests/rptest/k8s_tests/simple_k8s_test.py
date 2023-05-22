# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import os

from ducktape.tests.test import Test
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaServiceK8s


class SimpleK8sTest(Test):
    def __init__(self, test_context):
        """
        Keep it simple.
        """
        super(SimpleK8sTest, self).__init__(test_context)
        self.redpanda = RedpandaServiceK8s(test_context, 1)

    @property
    def debug_mode(self):
        """
        Useful for tests that want to change behaviour when running on
        the much slower debug builds of redpanda, which generally cannot
        keep up with significant quantities of data or partition counts.
        """
        return os.environ.get('BUILD_TYPE', None) == 'debug'

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
