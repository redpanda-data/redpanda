# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from requests.exceptions import HTTPError
from rptest.services.admin import Admin
from rptest.services.workload_protocol import PWorkload
from rptest.utils.rpenv import sample_license
from typing import Optional


class LicenseWorkload(PWorkload):
    """
    Test that ensures the licensing work does not incorrectly print license
    enforcement errors during upgrade when a guarded feature is already
    enabled. Also tests that the license can only be uploaded once the cluster
    has completed upgrade to the latest version.
    """
    LICENSE_CHECK_INTERVAL_SEC = 1

    def __init__(self, ctx) -> None:
        self.ctx = ctx
        self.first_license_check: Optional[float] = None
        self.license_installed = False
        self.license: Optional[str] = None
        self.installed_license_timeout: Optional[float] = None
        self.assert_admin_done = False

    def get_earliest_applicable_release(self):
        return (22, 1)  # last version without the license feature

    def begin(self):
        self.license = sample_license()
        if self.license is None:
            self.ctx.logger.info(
                "Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
            return

        self.ctx.redpanda.set_environment({
            '__REDPANDA_LICENSE_CHECK_INTERVAL_SEC':
            f'{LicenseWorkload.LICENSE_CHECK_INTERVAL_SEC}'
        })

    def on_partial_cluster_upgrade(self, versions) -> int:
        if self.license is None:
            return PWorkload.DONE

        if len([v for _, v in versions.items() if v[0:2] <= (22, 1)]) > 0:
            self.first_license_check = self.first_license_check or (
                time.time() + LicenseWorkload.LICENSE_CHECK_INTERVAL_SEC * 2)
            # Ensure a valid license cannot be uploaded in this cluster state
            try:
                Admin(self.ctx.redpanda).put_license(self.license)
                assert False
            except HTTPError as e:
                assert e.response.status_code == 400

            # Ensure the log is not written, if the fiber was enabled a log should
            # appear within one interval of the license check fiber
            if self.first_license_check > time.time():
                return PWorkload.NOT_DONE

            assert self.ctx.redpanda.search_log_any(
                "license is required to use enterprise features") is False
            return PWorkload.DONE

        return PWorkload.DONE

    def on_cluster_upgraded(self, version: tuple[int, int, int]) -> int:
        # skip test
        if self.license is None:
            return PWorkload.DONE

        # just check that no log nag is present
        if version[0:2] <= (22, 1):
            # These logs can't exist in v22.1 but double check anyway...
            assert self.ctx.redpanda.search_log_any(
                "license is required to use enterprise features") is False
            return PWorkload.DONE

        # license is installable
        admin = Admin(self.ctx.redpanda)

        if not self.assert_admin_done:
            assert admin.supports_feature("license")
            self.assert_admin_done = True

        # first license installation
        if not self.license_installed:
            self.first_license_check = self.first_license_check or (
                time.time() + LicenseWorkload.LICENSE_CHECK_INTERVAL_SEC * 4 *
                len(self.ctx.redpanda.nodes))
            # ensure that enough time passed for log nag to appear
            if self.first_license_check > time.time():
                return PWorkload.NOT_DONE

            # check for License nag in the log
            assert self.ctx.redpanda.search_log_any(
                "license is required to use enterprise features"
            ), "License nag log not found"

            # Install license
            assert admin.put_license(self.license).status_code == 200
            self.ctx.redpanda.unset_environment(
                ['__REDPANDA_LICENSE_CHECK_INTERVAL_SEC'])
            self.license_installed = True
            return PWorkload.DONE

        # license was installed and this is a new version of redpanda
        self.installed_license_timeout = self.installed_license_timeout or (
            time.time() + 30)

        assert self.installed_license_timeout >= time.time(
        ), "Timeout of installed license check"

        # Attempt to read license written by older version
        cluster_license = admin.get_license()

        if cluster_license is not None and cluster_license['loaded'] is True:
            self.installed_license_timeout = None  # check complete for this version
            return PWorkload.DONE
        else:
            # check needs more time
            return PWorkload.NOT_DONE
