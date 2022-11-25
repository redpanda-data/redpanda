# Copyright 2022 Redpanda Data, Inc.
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
from rptest.clients.rpk import RpkTool, RpkException
from ducktape.utils.util import wait_until
from ducktape.mark import matrix
import requests


class MaintenanceTest(RedpandaTest):
    topics = (TopicSpec(partition_count=10, replication_factor=3),
              TopicSpec(partition_count=20, replication_factor=3))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        self._use_rpk = True

    def _has_leadership_role(self, node):
        """
        Returns true if node is leader for some partition, and false otherwise.
        """
        id = self.redpanda.node_id(node)
        partitions = self.admin.get_partitions(node=node)
        has_leadership = False
        for p in partitions:
            if p["leader"] == id:
                self.logger.debug(f"{node.name} has leadership for {p}")
                has_leadership = True
        return has_leadership

    def _in_maintenance_mode(self, node):
        status = self.admin.maintenance_status(node)
        return status["draining"]

    def _in_maintenance_mode_fully(self, node):
        status = self.admin.maintenance_status(node)
        return status["finished"] and not status["errors"] and \
                status["partitions"] > 0

    def _verify_broker_metadata(self, maintenance_enabled, node):
        """
        check if both brokers interfaces in the admin server return
        the same status for maintenance mode. further, check if the
        mode is returning that draining has been enabled/disabled
        """
        node_id = self.redpanda.node_id(node)
        broker_target = self.admin.get_broker(node_id)
        broker_filtered = None
        for broker in self.admin.get_brokers():
            if broker['node_id'] == node_id:
                broker_filtered = broker
                break
        # both apis should return the same info
        if broker_filtered is None:
            return False
        status = broker_target['maintenance_status']
        if status != broker_filtered['maintenance_status']:
            return False
        # check status wanted
        if maintenance_enabled:
            return status['draining'] and status['finished']
        else:
            return not status['draining']

    def _verify_maintenance_status(self, node, draining):
        """
        Check that cluster reports maintenance status as expected through
        both rpk status tooling as well as raw admin interface.
        """
        # get status for this node via rpk
        node_id = self.redpanda.node_id(node)
        statuses = self.rpk.cluster_maintenance_status()
        self.logger.debug(f"finding node_id {node_id} in rpk "
                          "maintenance status: {statuses}")
        rpk_status = None
        for status in statuses:
            if status.node_id == node_id:
                rpk_status = status
                break
        if rpk_status is None:
            return False

        # get status for this node via admin interface
        admin_status = self.admin.maintenance_status(node)
        self.logger.debug(f"maintenance status from admin for "
                          "{node.name}: {admin_status}")

        # ensure that both agree on expected outcome
        return admin_status["draining"] == rpk_status.draining == draining

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
            f"Checking that node {node.name} has a leadership role")
        wait_until(lambda: self._has_leadership_role(node),
                   timeout_sec=60,
                   backoff_sec=10)

        self.logger.debug(
            f"Checking that node {node.name} is not in maintenance mode")
        wait_until(lambda: self._verify_maintenance_status(node, False),
                   timeout_sec=30,
                   backoff_sec=5)

        self.logger.debug(
            f"Waiting for node {node.name} to enter maintenance mode")
        if self._use_rpk:
            self.rpk.cluster_maintenance_enable(node, wait=True)
            # the node should now report itself in maintenance mode
            assert self._in_maintenance_mode(node), \
                    f"{node.name} not in expected maintenance mode"
        else:
            # when using the low-level admin interface the barrier is
            # implemented using wait_until and query the node directly
            self.admin.maintenance_start(node)
            wait_until(lambda: self._in_maintenance_mode(node),
                       timeout_sec=30,
                       backoff_sec=5)

        def has_drained():
            """
            as we wait for leadership to drain, also print out maintenance mode
            status. this is useful for debugging to detect if maintenance mode
            has been lost or disabled for some unexpected reason.
            """
            status = self.admin.maintenance_status(node)
            self.logger.debug(f"Maintenance status for {node.name}: {status}")
            return not self._has_leadership_role(node),

        self.logger.debug(f"Waiting for node {node.name} leadership to drain")
        wait_until(has_drained, timeout_sec=60, backoff_sec=10)

        self.logger.debug(
            f"Waiting for node {node.name} maintenance mode to complete")
        wait_until(lambda: self._in_maintenance_mode_fully(node),
                   timeout_sec=60,
                   backoff_sec=10)

        self.logger.debug("Verifying expected broker metadata reported "
                          f"for enabled maintenance mode on node {node.name}")
        wait_until(lambda: self._verify_broker_metadata(True, node),
                   timeout_sec=60,
                   backoff_sec=10)

    def _verify_cluster(self, target, target_expect):
        for node in self.redpanda.nodes:
            expect = False if node != target else target_expect
            wait_until(
                lambda: self._verify_maintenance_status(node, expect),
                timeout_sec=30,
                backoff_sec=5,
                err_msg=f"expected {node.name} maintenance mode: {expect}")

    def _maintenance_disable(self, node):
        if self._use_rpk:
            self.rpk.cluster_maintenance_disable(node)
        else:
            self.admin.maintenance_stop(node)

        wait_until(lambda: not self._in_maintenance_mode(node),
                   timeout_sec=30,
                   backoff_sec=5)

        wait_until(lambda: self._has_leadership_role(node),
                   timeout_sec=120,
                   backoff_sec=10)

        self.logger.debug("Verifying expected broker metadata reported "
                          f"for disabled maintenance mode on node {node.name}")
        wait_until(lambda: self._verify_broker_metadata(False, node),
                   timeout_sec=60,
                   backoff_sec=10)

    @cluster(num_nodes=3)
    @matrix(use_rpk=[True, False])
    def test_maintenance(self, use_rpk):
        self._use_rpk = use_rpk
        target = random.choice(self.redpanda.nodes)
        self._enable_maintenance(target)
        self._maintenance_disable(target)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(use_rpk=[True, False])
    def test_maintenance_sticky(self, use_rpk):
        self._use_rpk = use_rpk
        nodes = random.sample(self.redpanda.nodes, len(self.redpanda.nodes))
        for node in nodes:
            self._enable_maintenance(node)
            self._verify_cluster(node, True)

            self.redpanda.restart_nodes(node)
            self._verify_cluster(node, True)

            self._maintenance_disable(node)
            self._verify_cluster(node, False)

        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._verify_cluster(None, False)

    @cluster(num_nodes=3)
    @matrix(use_rpk=[True, False])
    def test_exclusive_maintenance(self, use_rpk):
        self._use_rpk = use_rpk
        target, other = random.sample(self.redpanda.nodes, k=2)
        assert target is not other
        self._enable_maintenance(target)
        try:
            self._enable_maintenance(other)
        except RpkException as e:
            assert self._use_rpk
            if "invalid state transition" in e.msg and "400" in e.msg:
                return
        except requests.exceptions.HTTPError as e:
            assert not self._use_rpk
            if "invalid state transition" in e.response.text and e.response.status_code == 400:
                return
            raise
        except:
            raise
        else:
            raise Exception("Expected maintenance enable to fail")
