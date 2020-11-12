# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest


class ConfigurationUpdateTest(RedpandaTest):
    """
    Updates RPC port on two nodes.
    """
    @cluster(num_nodes=3)
    def test_two_nodes_update(self):
        node_1 = self.redpanda.get_node(1)
        node_2 = self.redpanda.get_node(2)

        orig_partitions = self.redpanda.storage().partitions(
            "redpanda", "controller")
        # stop both nodes
        self.redpanda.stop_node(node_1)
        self.redpanda.stop_node(node_2)
        # change both ports
        altered_port_cfg_1 = dict(
            rpc_server=dict(address="{}".format(node_1.name), port=12345))
        altered_port_cfg_2 = dict(
            rpc_server=dict(address="{}".format(node_2.name), port=54321))

        # start both nodes
        self.redpanda.start_node(node_1, altered_port_cfg_1)
        self.redpanda.start_node(node_2, altered_port_cfg_2)

        def check_elements_equal(iterator):
            iterator = iter(iterator)
            try:
                first = next(iterator)
            except StopIteration:
                return True

            return all(first == rest for rest in iterator)

        def controller_log_replicated():
            # make sure that we have new segments
            node_partitions = dict()
            for p in self.redpanda.storage().partitions(
                    "redpanda", "controller"):
                node_partitions[p.node.name] = p

            for old_p in orig_partitions:
                nn = p.node.name
                if len(old_p.segments) <= len(node_partitions[nn].segments):
                    return False

            all_segments = map(lambda p: p.segments.keys(),
                               node_partitions.values())
            return check_elements_equal(all_segments)

        wait_until(lambda: controller_log_replicated(),
                   timeout_sec=60,
                   backoff_sec=2,
                   err_msg="Controller logs are not the same")
