# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

from ducktape.utils.util import wait_until

from rptest.services.cluster import cluster
from rptest.services.redpanda import PandaproxyConfig, RedpandaService
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
        node_1 = self.redpanda.get_node(1)
        node_2 = self.redpanda.get_node(2)

        orig_committed = max(
            r["committed_offset"]
            for r in self.redpanda._admin.get_partition_state(
                "redpanda", "controller", 0
            )["replicas"]
        )
        # stop both nodes
        self.redpanda.stop_node(node_1)
        self.redpanda.stop_node(node_2)
        # change both ports
        altered_port_cfg_1 = dict(
            rpc_server=dict(
                address="{}".format(node_1.name), port=ALTERNATIVE_RPC_PORTS[0]
            )
        )
        altered_port_cfg_2 = dict(
            rpc_server=dict(
                address="{}".format(node_2.name), port=ALTERNATIVE_RPC_PORTS[1]
            )
        )

        # start both nodes
        self.redpanda.start_node(node_1, altered_port_cfg_1)
        self.redpanda.start_node(node_2, altered_port_cfg_2)

        def controller_log_replicated():
            # segment file layout is per-replica (segments no longer roll on
            # raft term change and roll points depend on local timing), so
            # compare the replicated raft state instead of file names: the
            # controller log advanced past its pre-restart offset and all
            # replicas agree on the committed offset
            state = self.redpanda._admin.get_partition_state(
                "redpanda", "controller", 0
            )
            committed = {r["committed_offset"] for r in state["replicas"]}
            self.logger.debug(f"controller replica committed offsets: {committed}")
            return (
                len(state["replicas"]) == len(self.redpanda.nodes)
                and len(committed) == 1
                and committed.pop() > orig_committed
            )

        wait_until(
            lambda: controller_log_replicated(),
            timeout_sec=60,
            backoff_sec=2,
            err_msg="Controller logs are not the same",
            retry_on_exc=True,
        )

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
        altered_cfg_1 = dict(
            kafka_api=make_new_address(node_1, 10091),
            advertised_kafka_api=make_new_address(node_1, 10091),
        )
        altered_cfg_2 = dict(
            kafka_api=make_new_address(node_2, 10092),
            advertised_kafka_api=make_new_address(node_2, 10092),
        )
        altered_cfg_3 = dict(
            kafka_api=make_new_address(node_3, 10093),
            advertised_kafka_api=make_new_address(node_3, 10093),
        )

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

        wait_until(
            lambda: metadata_updated(),
            timeout_sec=60,
            backoff_sec=2,
            err_msg="Broker metadata aren't updated",
        )

    @cluster(num_nodes=3, log_allow_list=[re.compile(r"std::invalid_argument")])
    def test_update_invalid_advertised_api(self):
        node_1 = self.redpanda.get_node(1)
        node_2 = self.redpanda.get_node(2)
        node_3 = self.redpanda.get_node(3)

        # stop all nodes
        self.redpanda.stop_node(node_1)
        self.redpanda.stop_node(node_2)
        self.redpanda.stop_node(node_3)

        def make_new_address(node, port):
            return dict(address=node.name, port=port)

        altered_cfg_1 = dict(
            kafka_api=make_new_address(node_1, 10091),
            advertised_kafka_api=dict(address="0.0.0.0", port=10091),
        )

        altered_cfg_2 = dict(
            rpc_server=make_new_address(node_2, ALTERNATIVE_RPC_PORTS[0]),
            advertised_rpc_api=dict(address="0.0.0.0", port=ALTERNATIVE_RPC_PORTS[0]),
        )

        altered_cfg_3 = PandaproxyConfig()
        altered_cfg_3.advertised_api_host = "0.0.0.0"

        self.redpanda.start_node(
            node_1, override_cfg_params=altered_cfg_1, expect_fail=True, timeout=10
        )

        n_err_logs = self.redpanda.count_log_node(
            node_1, "'advertised_kafka_api' validation error"
        )
        assert n_err_logs == 1

        self.redpanda.start_node(
            node_2, override_cfg_params=altered_cfg_2, expect_fail=True, timeout=10
        )

        n_err_logs = self.redpanda.count_log_node(
            node_2, "'advertised_rpc_api' validation error"
        )
        assert n_err_logs == 1

        self.redpanda.set_pandaproxy_settings(altered_cfg_3)

        self.redpanda.start_node(node_3, expect_fail=True, timeout=10)

        n_err_logs = self.redpanda.count_log_node(
            node_3, "'advertised_pandaproxy_api' validation error"
        )
        assert n_err_logs == 1

    @cluster(num_nodes=3)
    def test_updating_address_after_data_deletion(self):
        node = self.redpanda.get_node(2)

        def make_new_address(node, port):
            return dict(address=node.name, port=port)

        # stop node
        self.redpanda.stop_node(node)

        # change rpc port & kafka advertised address
        altered_port_cfg_1 = dict(
            rpc_server=dict(
                address="{}".format(node.name), port=ALTERNATIVE_RPC_PORTS[1]
            ),
            kafka_api=make_new_address(node, 10091),
            advertised_kafka_api=make_new_address(node, 10091),
        )
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
                    b for b in brokers.values() if b.host == node.account.hostname
                ][0]
                return modified_broker.port == 10091
            except IndexError:
                # Our node may be missing if not yet up & healthy
                return False

        wait_until(broker_configuration_updated, timeout_sec=60, backoff_sec=2)
