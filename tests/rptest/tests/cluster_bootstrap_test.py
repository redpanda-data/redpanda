# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster


class ClusterBootstrapTest(RedpandaTest):
    def __init__(self, test_context):
        super(ClusterBootstrapTest,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             extra_node_conf={
                                 "empty_seed_starts_cluster": False,
                             })
        self.admin = self.redpanda._admin

    def setUp(self):
        # Defer startup to test body.
        pass

    @cluster(num_nodes=3)
    @matrix(num_seeds=[1, 2, 3], auto_assign_node_ids=[False, True])
    def test_three_node_bootstrap(self, num_seeds, auto_assign_node_ids):
        seeds = [self.redpanda.nodes[i] for i in range(num_seeds)]
        self.redpanda.set_seed_servers(seeds)
        self.redpanda.start(auto_assign_node_id=auto_assign_node_ids,
                            omit_seeds_on_idx_one=False)
        node_ids_per_idx = {}
        for n in self.redpanda.nodes:
            idx = self.redpanda.idx(n)
            node_ids_per_idx[idx] = self.redpanda.node_id(n)

        brokers = self.admin.get_brokers()
        assert 3 == len(brokers), f"Got {len(brokers)} brokers"

        # Restart our nodes and make sure our node IDs persist across restarts.
        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    auto_assign_node_id=auto_assign_node_ids,
                                    omit_seeds_on_idx_one=False)
        for idx in node_ids_per_idx:
            n = self.redpanda.get_node(idx)
            expected_node_id = node_ids_per_idx[idx]
            node_id = self.redpanda.node_id(n)
            assert expected_node_id == node_id, f"Expected {expected_node_id} but got {node_id}"
