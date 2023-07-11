# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from os.path import join

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.redpanda import RedpandaService, make_redpanda_service
from rptest.services.admin import Admin


class IndexRecoveryTest(RedpandaTest):
    def __init__(self, test_context):
        super(IndexRecoveryTest, self).__init__(test_context=test_context,
                                                log_level="trace")
        self.admin = Admin(self.redpanda)

    @cluster(num_nodes=1)
    def index_exists_test(self):
        single_node = self.redpanda.started_nodes()[0]
        self.redpanda.stop_node(single_node)
        self.redpanda.start_node(single_node)
        self.admin.wait_stable_configuration("controller",
                                             namespace="redpanda",
                                             replication=1)
        path = join(RedpandaService.DATA_DIR, "redpanda", "controller", "0_0")
        files = single_node.account.ssh_output(f"ls {path}").decode(
            "utf-8").splitlines()
        indices = [x for x in files if x.endswith(".base_index")]
        assert len(indices) > 0, "restart should create base_index file"

    @cluster(num_nodes=1)
    def index_recovers_test(self):
        single_node = self.redpanda.started_nodes()[0]
        self.redpanda.stop_node(single_node)
        self.redpanda.start_node(single_node)
        self.admin.wait_stable_configuration("controller",
                                             namespace="redpanda",
                                             replication=1)
        self.redpanda.stop_node(single_node)
        path = join(RedpandaService.DATA_DIR, "redpanda", "controller", "0_0")
        files = single_node.account.ssh_output(f"ls {path}").decode(
            "utf-8").splitlines()
        indices = [x for x in files if x.endswith(".base_index")]
        assert len(indices) > 0, "restart should create base_index file"
        last_index = sorted(indices)[-1]
        path = join(RedpandaService.DATA_DIR, "redpanda", "controller", "0_0",
                    last_index)
        single_node.account.ssh_output(f"rm {path}")
        self.redpanda.start_node(single_node)
        self.admin.wait_stable_configuration("controller",
                                             namespace="redpanda",
                                             replication=1)
        assert single_node.account.exists(path)
