# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import re
import time

from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from rptest.utils.rpenv import sample_license
from rptest.services.admin import Admin
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings, CloudStorageType, get_cloud_storage_type
from rptest.services.cluster import cluster
from requests.exceptions import HTTPError
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions


class UpgradeMigratingLicenseVersion(RedpandaTest):
    """
    Verify that the cluster can interpret licenses between versions
    """
    def __init__(self, test_context):
        super(UpgradeMigratingLicenseVersion,
              self).__init__(test_context=test_context,
                             num_brokers=3,
                             si_settings=SISettings(test_context))
        self.installer = self.redpanda._installer
        self.admin = Admin(self.redpanda)

    def setUp(self):
        # 22.2.x is when license went live
        self.installer.install(self.redpanda.nodes, (22, 2))
        super(UpgradeMigratingLicenseVersion, self).setUp()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(cloud_storage_type=get_cloud_storage_type(
        applies_only_on=[CloudStorageType.S3]))
    def test_license_upgrade(self, cloud_storage_type):
        license = sample_license()
        if license is None:
            self.logger.info(
                "Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
            return

        # Upload a license
        assert self.admin.put_license(license).status_code == 200

        # Update all nodes to newest version
        self.installer.install(self.redpanda.nodes, (22, 3))
        self.redpanda.restart_nodes(self.redpanda.nodes)
        _ = wait_for_num_versions(self.redpanda, 1)

        # Attempt to read license written by older version
        def license_loaded_ok():
            license = self.admin.get_license()
            return license is not None and license['loaded'] is True

        wait_until(license_loaded_ok,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="Timeout waiting for license to exist in cluster")
