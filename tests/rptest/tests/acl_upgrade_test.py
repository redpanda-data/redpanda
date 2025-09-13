# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SecurityConfig
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until


class ACLUpgradeTest(RedpandaTest):
    def __init__(self, test_context):
        self.security = SecurityConfig()
        self.security.enable_sasl = True
        super(ACLUpgradeTest, self).__init__(
            test_context=test_context, num_brokers=3, security=self.security
        )

        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS
        self.installer = self.redpanda._installer
        self.initial_version = self.installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD
        )

    def setUp(self):
        # Start with previous version
        self.installer.install(self.redpanda.nodes, self.initial_version)
        self.redpanda.start()

        self.rpk = RpkTool(
            self.redpanda,
            username=self.superuser.username,
            password=self.superuser.password,
            sasl_mechanism=self.superuser.algorithm,
        )

    def create_acl(self):
        def try_create_acl():
            self.rpk.sasl_allow_principal("User:testuser", ["all"], "topic", "*")
            return self.acl_exists()

        # Retry to handle NOT_CONTROLLER errors
        wait_until(
            try_create_acl,
            timeout_sec=20,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="ACL not created",
        )

    def acl_exists(self):
        return "User:testuser" in self.rpk.acl_list()

    def await_acl_exists(self, exists: bool):
        wait_until(
            lambda: self.acl_exists() == exists,
            timeout_sec=20,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg=f"Failed: ACL expected to {'exist' if exists else 'not exist'}",
        )

    def delete_acl(self):
        def try_delete_acl():
            self.rpk.delete_principal("User:testuser", ["all"], "topic", "*")
            return not self.acl_exists()

        # Retry to handle NOT_CONTROLLER errors
        wait_until(
            try_delete_acl,
            timeout_sec=20,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="ACL not deleted",
        )

    def _verify_acl_operations(self):
        self.create_acl()
        self.delete_acl()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_acl_operations_during_upgrade(self):
        self.logger.info("Testing ACL operations on old version")
        self._verify_acl_operations()

        self.logger.info("Upgrading first node")
        first_node = self.redpanda.nodes[0]
        self.installer.install([first_node], RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes([first_node])
        self.redpanda.wait_until(
            self.redpanda.healthy,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="Cluster failed to stabilize after first node upgrade",
        )

        self.logger.info("Verify ACL operations work on a mixed version cluster")
        self._verify_acl_operations()

        self.logger.info("Completing upgrade")
        remaining_nodes = self.redpanda.nodes[1:]
        self.installer.install(remaining_nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(remaining_nodes)
        self.redpanda.wait_until(
            self.redpanda.healthy,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="Cluster failed to stabilize after full upgrade",
        )

        self.logger.info("Testing ACL operations on new version")
        self._verify_acl_operations()
