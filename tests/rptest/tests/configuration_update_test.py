# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.services.redpanda import RedpandaService

from rptest.tests.redpanda_test import RedpandaTest

# Choose ports _below_ the default 33145, because test environment
# is configured to only use emphemeral ports above 34000: this avoids
# port collisions.
ALTERNATIVE_RPC_PORTS = [12345, 20001]


class ConfigurationUpdateTest(RedpandaTest):
    """
    Updates RPC port on two nodes.
    """
    @cluster(num_nodes=3)
    def test_two_nodes_update(self):
        # Select two nodes which are not the controller leader
        controller_node = self.redpanda.controller()
        other_nodes = [
            n for n in self.redpanda.nodes if n is not controller_node
        ]
        node_1, node_2 = other_nodes

        # stop both nodes
        self.redpanda.stop_node(node_1)
        self.redpanda.stop_node(node_2)

        orig_partitions = self.redpanda.storage(all_nodes=True).partitions(
            "redpanda", "controller")
        for old_p in orig_partitions:
            self.logger.debug(
                f"{old_p.node.name}: old segments: {old_p.segments}")

        # change both ports
        altered_port_cfg_1 = dict(rpc_server=dict(
            address="{}".format(node_1.name), port=ALTERNATIVE_RPC_PORTS[0]))
        altered_port_cfg_2 = dict(rpc_server=dict(
            address="{}".format(node_2.name), port=ALTERNATIVE_RPC_PORTS[1]))

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

        # Move controller leadership to one of the restarted nodes, to cause
        # a term increase (all nodes should see it), and confirm that the
        # restarted node is functioning well enough to be a leader.
        admin = Admin(self.redpanda)
        new_leader = node_1
        admin.transfer_leadership_to(namespace="redpanda",
                                     topic="controller",
                                     partition="0",
                                     target_id=self.redpanda.idx(new_leader))

        def all_nodes_participating():
            for node in self.redpanda.nodes:
                p = admin.get_partitions(namespace="redpanda",
                                         topic="controller",
                                         partition="0",
                                         node=node)
                # All nodes should agree on controller leader
                if p['leader_id'] != self.redpanda.idx(new_leader):
                    self.logger.debug(
                        f"Node {node.name} doesn't agree on {new_leader.name} leadership yet ({p})"
                    )
                    return False

            node_terms = []
            for p in self.redpanda.storage(all_nodes=True).partitions(
                    "redpanda", "controller"):
                names = p.segments.keys()
                terms = set()
                for n in names:
                    offset, term, fmt_v = n.split("-")
                    terms.add(term)

                node_terms.append(terms)

            # All nodes should have seen same controller terms
            r = check_elements_equal(node_terms)
            if not r:
                self.logger.debug(f"Terms not yet equal: {node_terms}")
            return r

        wait_until(lambda: all_nodes_participating(),
                   timeout_sec=60,
                   backoff_sec=2,
                   err_msg="Node states did not converge")

    @cluster(num_nodes=3)
    def test_update_advertised_kafka_api_on_all_nodes(self):
        """
        Should allow to update port of advertised kafka API on all of the nodes at once
        """

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
            rpc_server=dict(address="{}".format(node.name),
                            port=ALTERNATIVE_RPC_PORTS[1]),
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
