# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0


from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from requests.exceptions import HTTPError

from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    RESTART_LOG_ALLOW_LIST,
    CloudStorageType,
    SISettings,
    get_cloud_storage_type,
)
from rptest.services.redpanda_installer import wait_for_num_versions
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception
from rptest.utils.rpenv import sample_license, sample_license_v1


class UpgradeMigratingLicenseVersion(RedpandaTest):
    """
    Verify that the cluster can interpret licenses between versions
    """

    def __init__(self, test_context):
        super(UpgradeMigratingLicenseVersion, self).__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=SISettings(test_context),
        )
        self.installer = self.redpanda._installer
        self.admin = Admin(self.redpanda)

    def setUp(self):
        # 22.2.x is when license went live
        self.installer.install(self.redpanda.nodes, (22, 2))
        super(UpgradeMigratingLicenseVersion, self).setUp()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_license_upgrade(self, cloud_storage_type):
        license = sample_license()
        if license is None:
            self.logger.info("Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
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
            return self.admin.is_sample_license(license)

        wait_until(
            license_loaded_ok,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timeout waiting for license to exist in cluster",
        )


class UpgradeFormatLicenseVersion(RedpandaTest):
    """
    Verify that the cluster can interpret licenses between versions
    """

    def __init__(self, test_context):
        super(UpgradeFormatLicenseVersion, self).__init__(
            test_context=test_context,
            num_brokers=2,
            si_settings=SISettings(test_context),
        )
        self.installer = self.redpanda._installer
        self.admin = Admin(self.redpanda)

    def setUp(self):
        # 25.2.x is when license formating went live
        # Setting to previous version
        self.installer.install(self.redpanda.nodes, (25, 1))
        super(UpgradeFormatLicenseVersion, self).setUp()

    @cluster(num_nodes=2, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_license_versioning_upgrade(self):
        license_v0 = sample_license()
        license_v1 = sample_license_v1()
        if license_v0 is None:
            self.logger.info("Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
            return
        if license_v1 is None:
            self.logger.info(
                "Skipping test, REDPANDA_SAMPLE_LICENSE_V1_PRODUCTS env var not found"
            )
            return

        # V1 format should be rejected, as this is an older version
        with expect_exception(
            HTTPError, lambda e: "400 Client Error: Bad Request" in str(e)
        ):
            self.admin.put_license(license_v1)

        # V0 format should be accepted
        res = self.admin.put_license(license_v0)
        assert res.status_code == 200, (
            f"Expected status_code {200} but got {res.status_code} instead.\nContent: {res.content}"
        )

        # Update all nodes to newest version
        self.installer.install(self.redpanda.nodes, (25, 2))
        self.redpanda.restart_nodes(self.redpanda.nodes)
        _ = wait_for_num_versions(self.redpanda, 1)

        # Attempt to read license written by older version
        def v0_license_loaded_ok():
            license = self.admin.get_license()
            return self.admin.is_sample_license(license)

        wait_until(
            v0_license_loaded_ok,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timeout waiting for license v0 to exist in cluster",
        )

        res = self.admin.put_license(license_v1)
        assert res.status_code == 200, (
            f"Expected status_code {200} but got {res.status_code} instead.\nContent: {res.content}"
        )

        # Verify newer format license
        def v1_license_loaded_ok():
            lic = self.admin.get_license()
            if lic is None or "license" not in lic:
                return False
            return (lic["license"]["format_version"] == 1, lic)

        wait_until(
            v1_license_loaded_ok,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timeout waiting for license v1 to exist in cluster",
        )

    @cluster(num_nodes=2, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_license_versioning_partial_upgrade(self):
        license_v1 = sample_license_v1()
        if license_v1 is None:
            self.logger.info(
                "Skipping test, REDPANDA_SAMPLE_LICENSE_V1_PRODUCTS env var not found"
            )
            return

        node_v25_2 = self.redpanda.nodes[0]
        node_v25_2_id = self.redpanda.node_id(node_v25_2)
        node_v25_1 = self.redpanda.nodes[1]
        node_v25_1_id = self.redpanda.node_id(node_v25_1)

        # Update only the first node to newest version
        self.installer.install([node_v25_2], (25, 2))
        self.redpanda.restart_nodes([node_v25_2])
        _ = wait_for_num_versions(self.redpanda, 2)

        # Verify that we are working with the expected versions in each node
        version = self.redpanda.get_version(node_v25_2)
        assert "v25.2" in version, (
            f"Expected a 'v25.2' version for node {node_v25_2_id}. Got '{version}' instead"
        )
        version = self.redpanda.get_version(node_v25_1)
        assert "v25.1" in version, (
            f"Expected a 'v25.1' version for node {node_v25_1_id}. Got '{version}' instead"
        )

        def change_controller(target_controller):
            self.admin.partition_transfer_leadership(
                namespace="redpanda",
                topic="controller",
                target_id=self.redpanda.node_id(target_controller),
                partition=0,
            )

            return self.admin.await_stable_leader(
                topic="controller", partition=0, namespace="redpanda"
            )

        # Set controller to the upgraded node
        controller_id = change_controller(node_v25_2)
        assert controller_id == node_v25_2_id, (
            f"Expected node {node_v25_2_id} to be controller. Got node {controller_id} instead"
        )

        # V1 format should be rejected on any older node
        with expect_exception(
            HTTPError, lambda e: "400 Client Error: Bad Request" in str(e)
        ):
            self.admin.put_license(license_v1, node=node_v25_1)

        # V1 format should be accepted on upgraded node
        res = self.admin.put_license(license_v1, node=node_v25_2)
        assert res.status_code == 200, (
            f"Expected status_code {200} but got {res.status_code} instead.\nContent: {res.content}"
        )

        expected_license_contents = {
            "format_version": 1,
            "org": "redpanda-testing",
            "type": "testing_license",
            "products": ["some_prod", "some_other_prod"],
            "expires": 4344165449,
            "sha256": "0937a2d8e4437a63373c1c1cb0f5f62c5cae9366fea1b00467b4c4eaab8ca4cf",
        }

        # Wait until license has been loaded in all nodes
        def v1_license_loaded_ok():
            def verify_license(lic):
                if lic is None or "license" not in lic:
                    return False
                correct_version = (
                    lic["license"]["format_version"]
                    == expected_license_contents["format_version"]
                )
                correct_sha256 = (
                    lic["license"]["sha256"] == expected_license_contents["sha256"]
                )
                return correct_version and correct_sha256

            lics = [self.admin.get_license(node=node) for node in self.redpanda.nodes]
            return all([verify_license(lic) for lic in lics])

        wait_until(
            v1_license_loaded_ok,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timeout waiting for license v1 to propagate through cluster",
        )

        # Validate the full license against upgraded node
        license_v1 = self.admin.get_license(node=node_v25_2)
        assert expected_license_contents == license_v1["license"], (
            f"Invalid license loaded.\n"
            f"Found: {license_v1['license']}\n"
            f"Expected: {expected_license_contents}"
        )

        # Now check how it behaves on a node that is not aware of V1
        lic = self.admin.get_license(node=node_v25_1)["license"]

        # Validate that backwards compatible fields are maintained
        for key in ["format_version", "org", "expires", "sha256"]:
            assert lic[key] == expected_license_contents[key], (
                f"Invalid value for key: '{key}'\n"
                f"Found: {lic[key]}\n"
                f"Expected: {expected_license_contents[key]}"
            )

        # 'type' is now a string. 'type' should be in the defaulted value, i.e. free trial
        assert lic["type"] == "free_trial", (
            f"Expected 'type' as 'free_trial'. Got '{lic['type']}'."
        )

        # 'products' is a new field in V1. 'products' should not be part of the license
        assert "products" not in lic, (
            f"Expected 'products' not in license. License: {lic}"
        )
