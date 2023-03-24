# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import time

import requests.exceptions
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions, ver_string
from rptest.tests.redpanda_test import RedpandaTest


class ControlCharacterPermittedBase(RedpandaTest):
    feature_legacy_permit_control_char = 'legacy_permit_unsafe_log_operation'

    def __init__(self, test_context, **kwargs):
        super(ControlCharacterPermittedBase,
              self).__init__(test_context=test_context, **kwargs)
        self._installer = self.redpanda._installer
        self._admin = Admin(self.redpanda)

    def setUp(self):
        # handled by test case to support parameterization
        pass

    def _start_redpanda(self):
        super(ControlCharacterPermittedBase, self).setUp()

    def _validate_pre_upgrade(self, version):
        self._installer.install(self.redpanda.nodes, version)
        self._start_redpanda()

        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert ver_string(version) in unique_versions

        config = self._admin.get_cluster_config()
        assert config.keys().isdisjoint(
            {self.feature_legacy_permit_control_char})

    def _perform_update(self, initial_version, version):
        self._installer.install(self.redpanda.nodes, version)
        self.redpanda.restart_nodes(self.redpanda.nodes)

        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert ver_string(initial_version) not in unique_versions

        config = self._admin.get_cluster_config()
        assert config.keys() > {self.feature_legacy_permit_control_char}

        assert config[self.feature_legacy_permit_control_char] is True


class ControlCharacterPermittedAfterUpgrade(ControlCharacterPermittedBase):
    """
    When upgrading to Redpanda v23.2.1, a new flag is added that will
    permit the use of strings that have control characters in them.  When this
    flag is set to true, these characters are permitted.  When false, the characters
    are rejected.  In new clusters, those that start at v23.2.1, the flag does
    nothing.
    """
    def __init__(self, test_context):
        super(ControlCharacterPermittedAfterUpgrade,
              self).__init__(test_context=test_context, num_brokers=3)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(initial_version=(22, 2, 9))
    @parametrize(initial_version=(22, 3, 11))
    @parametrize(initial_version=(23, 1, 1))
    def test_upgrade_from_pre_v23_2(self, initial_version):
        """
        Validate that when upgrading from pre v23.2 that the flag is
        honored
        """
        self._validate_pre_upgrade(initial_version)

        # Creates a user with invalid control characters

        self._admin.create_user("my\nuser", "password", "SCRAM-SHA-256")
        self._perform_update(initial_version, RedpandaInstaller.HEAD)
        # Should still be able to create a user
        self._admin.create_user("my\notheruser", "password", "SCRAM-SHA-256")
        self._admin.patch_cluster_config(
            {self.feature_legacy_permit_control_char: False})
        try:
            self._admin.create_user("my\nthirduser", "password",
                                    "SCRAM-SHA-256")
            assert False
        except requests.exceptions.HTTPError:
            pass

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_new_cluster(self):
        """
        Validates that new clusters ignore the flag
        """
        super(ControlCharacterPermittedAfterUpgrade, self)._start_redpanda()

        config = self._admin.get_cluster_config()
        assert config[self.feature_legacy_permit_control_char] is True

        try:
            self._admin.create_user("my\nuser", "password", "SCRAM-SHA-256")
            assert False
        except requests.exceptions.HTTPError:
            pass

        self._admin.patch_cluster_config(
            {self.feature_legacy_permit_control_char: False})

        try:
            self._admin.create_user("my\nuser", "password", "SCRAM-SHA-256")
            assert False
        except requests.exceptions.HTTPError:
            pass


class ControlCharacterNag(ControlCharacterPermittedBase):
    """
    Validates the presence (or lack there of) of the flag nag message
    """
    def __init__(self, test_context):
        super(ControlCharacterNag, self).__init__(test_context=test_context,
                                                  num_brokers=3)

    def _has_flag_nag(self):
        return self.redpanda.search_log_all(
            "You have enabled unsafe log operations")

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(initial_version=(22, 2, 9))
    @parametrize(initial_version=(22, 3, 11))
    @parametrize(initial_version=(23, 1, 1))
    def test_validate_nag_message(self, initial_version):
        """
        Validates that the nag message is present after upgrading a cluster
        """
        self._validate_pre_upgrade(initial_version)

        # Nag shouldn't be in logs
        assert not self._has_flag_nag()

        self._perform_update(initial_version, RedpandaInstaller.HEAD)

        assert self._has_flag_nag()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_validate_no_nag_in_new_cluster(self):
        """
        Validates that the nag message is not present when using a fresh cluster
        """
        super(ControlCharacterNag, self)._start_redpanda()

        config = self._admin.get_cluster_config()
        assert config[self.feature_legacy_permit_control_char] is True

        assert not self._has_flag_nag()

        self._admin.patch_cluster_config(
            {self.feature_legacy_permit_control_char: False})
        # Give time for config to propagate
        time.sleep(2)

        assert not self._has_flag_nag()

        self._admin.patch_cluster_config(
            {self.feature_legacy_permit_control_char: True})
        # Give time for config to propagate
        time.sleep(2)

        assert not self._has_flag_nag()
