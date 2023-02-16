# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import requests

from ducktape.utils.util import wait_until


class RollingRestarter:
    """
    Encapsulates the logic needed to perform a rolling restart.
    """
    def __init__(self, redpanda):
        self.redpanda = redpanda

    def restart_nodes(self,
                      nodes,
                      override_cfg_params=None,
                      start_timeout=None,
                      stop_timeout=None,
                      use_maintenance_mode=True,
                      omit_seeds_on_idx_one=True):
        """
        Performs a rolling restart on the given nodes, optionally overriding
        the given configs.
        """
        admin = self.redpanda._admin
        if start_timeout is None:
            start_timeout = self.redpanda.node_ready_timeout_s
        if stop_timeout is None:
            stop_timeout = self.redpanda.node_ready_timeout_s

        def has_drained_leaders(node):
            try:
                node_id = self.redpanda.idx(node)
                broker_resp = admin.get_broker(node_id, node=node)
                maintenance_status = broker_resp["maintenance_status"]
                return maintenance_status["draining"] and maintenance_status[
                    "finished"]
            except requests.exceptions.HTTPError:
                return False

        def wait_until_cluster_healthy(timeout_sec):
            wait_until(lambda: self.redpanda.healthy(),
                       timeout_sec=stop_timeout,
                       backoff_sec=1)
            # Wait for the cluster to agree on a controller leader.
            return self.redpanda.get_node(
                admin.await_stable_leader(
                    topic="controller",
                    partition=0,
                    namespace="redpanda",
                    hosts=[n.account.hostname for n in self.redpanda._started],
                    timeout_s=timeout_sec,
                    backoff_s=1))

        # TODO: incorporate rack awareness into this to support updating
        # multiple nodes at a time.
        self.redpanda.logger.info(
            f"Rolling restart of nodes {[n.name for n in nodes]}")
        for node in nodes:
            self.redpanda.logger.info(f"Waiting for cluster healthy")
            controller_leader = wait_until_cluster_healthy(stop_timeout)

            # NOTE: callers may not want to use maintenance mode if the
            # cluster is on a version that does not support it.
            if use_maintenance_mode:
                # In case the cluster was just upgraded, wait for all nodes to
                # acknowledge the 'maintenance_mode' feature activation.
                wait_until(
                    lambda: admin.supports_feature("maintenance_mode"),
                    timeout_sec=stop_timeout,
                    backoff_sec=1,
                    err_msg=
                    "Timeout waiting for cluster to support 'maintenance_mode' feature"
                )
                self.redpanda.logger.info(
                    f"Put node {node.name} into maintenance mode")
                admin.maintenance_start(node, dst_node=controller_leader)
                self.redpanda.logger.info(
                    f"Waiting for node {node.name} leadership drain")
                wait_until(lambda: has_drained_leaders(node),
                           timeout_sec=stop_timeout,
                           backoff_sec=1,
                           err_msg=f"Node {node.name} draining leaderships")

            wait_until(lambda: self.redpanda.healthy(),
                       timeout_sec=stop_timeout,
                       backoff_sec=1)

            self.redpanda.stop_node(node, timeout=stop_timeout)

            self.redpanda.start_node(
                node,
                override_cfg_params,
                timeout=start_timeout,
                omit_seeds_on_idx_one=omit_seeds_on_idx_one)

            controller_leader = wait_until_cluster_healthy(start_timeout)

            if use_maintenance_mode:
                admin.maintenance_stop(node, dst_node=controller_leader)
