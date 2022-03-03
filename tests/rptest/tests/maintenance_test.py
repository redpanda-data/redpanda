# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random

from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from ducktape.utils.util import wait_until
import requests


class MaintenanceTest(RedpandaTest):
    topics = (TopicSpec(partition_count=10, replication_factor=3),
              TopicSpec(partition_count=20, replication_factor=3))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.admin = Admin(self.redpanda)

    def _has_leadership_role(self, node):
        """
        Returns true if node is leader for some partition, and false otherwise.
        """
        id = self.redpanda.idx(node)
        partitions = self.admin.get_partitions(node=node)
        return len(list(filter(lambda p: p["leader"] == id, partitions))) > 0

    def _in_maintenance_mode(self, node):
        status = self.admin.maintenance_status(node)
        return status["draining"]

    def _in_maintenance_mode_fully(self, node):
        status = self.admin.maintenance_status(node)
        return status["finished"] and not status["errors"] and \
                status["partitions"] > 0

    def _enable_maintenance(self, node):
        """
        1. Verifies that node is leader for some partitions
        2. Verifies node is not already in maintenance mode
        3. Requests that node enter maintenance mode (persistent interface)
        4. Verifies node enters maintenance mode
        5. Verifies that node has no leadership role
        6. Verifies that maintenance mode completes

        Note that there is a terminology issue that we need to work on. When we
        say that 'maintenance mode completes' it doesn't mean that the node
        leaves maintenance mode. What we mean is that it has entered maintenance
        mode and all of the work associated with that has completed.
        """
        self.logger.debug(
            "Checking that node {node.name} has a leadership role")
        wait_until(lambda: self._has_leadership_role(node),
                   timeout_sec=60,
                   backoff_sec=10)

        self.logger.debug(
            "Checking that node {node.name} is not in maintenance mode")
        status = self.admin.maintenance_status(node)
        assert status[
            "draining"] == False, f"Node {node.name} in maintenance mode"

        self.admin.maintenance_start(node)

        self.logger.debug(
            "Waiting for node {node.name} to enter maintenance mode")
        wait_until(lambda: self._in_maintenance_mode(node),
                   timeout_sec=30,
                   backoff_sec=5)

        self.logger.debug("Waiting for node {node.name} leadership to drain")
        wait_until(lambda: not self._has_leadership_role(node),
                   timeout_sec=60,
                   backoff_sec=10)

        self.logger.debug(
            "Waiting for node {node.name} maintenance mode to complete")
        wait_until(lambda: self._in_maintenance_mode_fully(node),
                   timeout_sec=60,
                   backoff_sec=10)

    def _disable_maintenance(self, node):
        wait_until(lambda: not self._in_maintenance_mode(node),
                   timeout_sec=30,
                   backoff_sec=5)

        wait_until(lambda: self._has_leadership_role(node),
                   timeout_sec=120,
                   backoff_sec=10)

    def _verify_cluster(self, target, target_expect):
        for node in self.redpanda.nodes:
            expect = False if node != target else target_expect
            wait_until(
                lambda: self._in_maintenance_mode(node) == expect,
                timeout_sec=30,
                backoff_sec=5,
                err_msg=f"expected {node.name} maintenance mode: {expect}")

    @cluster(num_nodes=3)
    def test_maintenance(self):
        target = random.choice(self.redpanda.nodes)
        self._enable_maintenance(target)
        self.admin.maintenance_stop(target)
        self._disable_maintenance(target)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_maintenance_sticky(self):
        nodes = random.sample(self.redpanda.nodes, len(self.redpanda.nodes))
        for node in nodes:
            self._enable_maintenance(node)
            self._verify_cluster(node, True)

            self.redpanda.restart_nodes(node)
            self._verify_cluster(node, True)

            self.admin.maintenance_stop(node)
            self._disable_maintenance(node)
            self._verify_cluster(node, False)

        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._verify_cluster(None, False)

    @cluster(num_nodes=3)
    def test_exclusive_maintenance(self):
        target, other = random.sample(self.redpanda.nodes, k=2)
        assert target is not other
        self._enable_maintenance(target)
        try:
            self._enable_maintenance(other)
        except requests.exceptions.HTTPError as e:
            if "invalid state transition" in e.response.text and e.response.status_code == 400:
                return
            raise
        except:
            raise
        else:
            raise Exception("Expected maintenance enable to fail")
