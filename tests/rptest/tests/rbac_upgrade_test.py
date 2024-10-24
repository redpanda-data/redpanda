# Copyright 2024 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import time

from rptest.services.admin import Admin, Role, RoleMember
from rptest.util import wait_until_result
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import wait_for_num_versions
from rptest.utils.mode_checks import skip_fips_mode


class UpgradeMigrationCreatingDefaultRole(RedpandaTest):
    """
    Verify that the default role gets created when the RBAC feature is enabled
    """

    DEFAULT_ROLE_NAME = "Users"
    LICENSE_CHECK_INTERVAL_SEC = 1

    def __init__(self, test_ctx, **kwargs):
        super().__init__(test_ctx, **kwargs)
        self.redpanda.set_environment({
            '__REDPANDA_LICENSE_CHECK_INTERVAL_SEC':
            f'{self.LICENSE_CHECK_INTERVAL_SEC}'
        })
        self.installer = self.redpanda._installer
        self.admin = Admin(self.redpanda)

    def setUp(self):
        # 24.1.x is when license went live, so start with a 23.3.x version
        self.installer.install(self.redpanda.nodes, (23, 3))
        super().setUp()

    def _has_license_nag(self):
        return self.redpanda.search_log_any(
            "license is required to use enterprise features")

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @skip_fips_mode  # See NOTE below
    def test_rbac_migration(self):
        # Create some users to add to the default role
        self.admin.create_user("alice")
        self.admin.create_user("bob")

        # Update all nodes to newest version
        self.installer.install(self.redpanda.nodes,
                               self.installer.head_version())
        self.redpanda.restart_nodes(self.redpanda.nodes)
        _ = wait_for_num_versions(self.redpanda, 1)

        def default_role_created():
            res = self.admin.get_role(self.DEFAULT_ROLE_NAME)
            role = Role.from_response(res)

            expected_users = [
                RoleMember.User("alice"),
                RoleMember.User("bob"),
                RoleMember.User("admin"),
            ]

            for member in expected_users:
                assert member in role.members,\
                    f"'{member}' missing from role members: {role.members}"
            return True

        wait_until_result(
            default_role_created,
            timeout_sec=30,
            backoff_sec=1,
            retry_on_exc=True,
            err_msg="Timeout waiting for default role to be created")

        # Verify that we don't get a license nag for the default role
        # NOTE: This assertion will FAIL if running in FIPS mode because
        # being in FIPS mode will trigger the license nag
        time.sleep(self.LICENSE_CHECK_INTERVAL_SEC * 2)
        assert not self._has_license_nag()
