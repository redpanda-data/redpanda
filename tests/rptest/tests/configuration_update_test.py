# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.services.redpanda import RedpandaService

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

    """
    Should allow to update port of advertised kafka API on all of the nodes at once
    """

    @cluster(num_nodes=3)
    def test_update_advertised_kafka_api_on_all_nodes(self):

        node_1 = self.redpanda.get_node(1)
        node_2 = self.redpanda.get_node(2)
        node_3 = self.redpanda.get_node(3)

        # stop all nodes
        self.redpanda.stop_node(node_1)
        self.redpanda.stop_node(node_2)
        self.redpanda.stop_node(node_3)

        def make_new_address(node, port):
            return dict(address=node.name, port=port)

        # change ports
        altered_cfg_1 = dict(kafka_api=make_new_address(node_1, 10091),
                             advertised_kafka_api=make_new_address(
                                 node_1, 10091))
        altered_cfg_2 = dict(kafka_api=make_new_address(node_2, 10092),
                             advertised_kafka_api=make_new_address(
                                 node_2, 10092))
        altered_cfg_3 = dict(kafka_api=make_new_address(node_3, 10093),
                             advertised_kafka_api=make_new_address(
                                 node_3, 10093))

        # start all
        self.redpanda.start_node(node_1, override_cfg_params=altered_cfg_1)
        self.redpanda.start_node(node_2, override_cfg_params=altered_cfg_2)
        self.redpanda.start_node(node_3, override_cfg_params=altered_cfg_3)

        def metadata_updated():
            brokers = self.client().brokers()
            self.redpanda.logger.debug(f"brokers metadata: {brokers}")
            ports = [b.port for _, b in brokers.items()]
            ports.sort()
            return ports == [10091, 10092, 10093]

        wait_until(lambda: metadata_updated(),
                   timeout_sec=60,
                   backoff_sec=2,
                   err_msg="Broker metadata aren't updated")

    @cluster(num_nodes=3)
    def test_updating_address_after_data_deletion(self):
        node = self.redpanda.get_node(2)

        def make_new_address(node, port):
            return dict(address=node.name, port=port)

        # stop node
        self.redpanda.stop_node(node)

        # change rpc port & kafka advertised address
        altered_port_cfg_1 = dict(
            rpc_server=dict(address="{}".format(node.name), port=54321),
            kafka_api=make_new_address(node, 10091),
            advertised_kafka_api=make_new_address(node, 10091))
        # remove node data folder
        node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/*")

        # start node
        self.redpanda.start_node(node, altered_port_cfg_1)

        def broker_configuration_updated():
            brokers = self.client().brokers()
            self.logger.debug(f"brokers metadata: {brokers}")

            try:
                # Look up by hostname, as broker order in vector is not deterministic
                modified_broker = [
                    b for b in brokers.values()
                    if b.host == node.account.hostname
                ][0]
                return modified_broker.port == 10091
            except IndexError:
                # Our node may be missing if not yet up & healthy
                return False

        wait_until(broker_configuration_updated, timeout_sec=60, backoff_sec=2)
