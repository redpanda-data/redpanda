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
                      use_maintenance_mode=True):
        """
        Performs a rolling restart on the given nodes, optionally overriding
        the given configs.
        """
        admin = self.redpanda._admin

        def has_drained_leaders(node):
            try:
                node_id = self.redpanda.idx(node)
                broker_resp = admin.get_broker(node_id)
                maintenance_status = broker_resp["maintenance_status"]
                return maintenance_status["draining"] and maintenance_status[
                    "finished"]
            except requests.exceptions.HTTPError:
                return False

        # TODO: incorporate rack awareness into this to support updating
        # multiple nodes at a time.
        for node in nodes:
            wait_until(lambda: self.redpanda.healthy(),
                       timeout_sec=stop_timeout,
                       backoff_sec=1)

            if use_maintenance_mode:
                # NOTE: callers may not want to use maintenance mode if the
                # cluster is on a version that does not support it.
                admin.maintenance_start(node)
                wait_until(lambda: has_drained_leaders(node),
                           timeout_sec=stop_timeout,
                           backoff_sec=1)

            wait_until(lambda: self.redpanda.healthy(),
                       timeout_sec=stop_timeout,
                       backoff_sec=1)

            self.redpanda.stop_node(node, timeout=stop_timeout)

            self.redpanda.start_node(node,
                                     override_cfg_params,
                                     timeout=start_timeout)

            wait_until(lambda: self.redpanda.healthy(),
                       timeout_sec=start_timeout,
                       backoff_sec=1)

            if use_maintenance_mode:
                admin.maintenance_stop(node)
