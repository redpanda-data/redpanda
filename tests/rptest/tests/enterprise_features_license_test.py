import time
import json
from enum import IntEnum

from rptest.utils.rpenv import sample_license
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin, EnterpriseLicenseStatus, RolesList, RoleDescription
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SecurityConfig, SchemaRegistryConfig
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions
from rptest.util import expect_exception
from rptest.utils.mode_checks import skip_fips_mode

from ducktape.errors import TimeoutError as DucktapeTimeoutError
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize, matrix
from rptest.util import wait_until_result


class EnterpriseFeaturesTestBase(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.admin = Admin(self.redpanda)
        self.installer = self.redpanda._installer

    def setUp(self):
        super().setUp()


class Features(IntEnum):
    audit_logging = 0
    cloud_storage = 1
    partition_auto_balancing_continuous = 2
    core_balancing_continuous = 3
    gssapi = 4
    oidc = 5
    schema_id_validation = 6
    rbac = 7
    fips = 8
    datalake_iceberg = 9
    leadership_pinning = 10


SKIP_FEATURES = [
    Features.audit_logging,  # NOTE(oren): omit due to shutdown issues
    Features.
    cloud_storage,  # TODO(oren): initially omitted because it's a bit complicated to initialize infra
    Features.fips  # NOTE(oren): omit because it's too much of a pain for CDT
]


class EnterpriseFeaturesTest(EnterpriseFeaturesTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            num_brokers=3,
            schema_registry_config=SchemaRegistryConfig(),
            **kwargs,
        )

        self.su, self.pw, self.mech = self.redpanda.SUPERUSER_CREDENTIALS

        self.security = SecurityConfig()
        self.security.enable_sasl = True
        self.kafka_enable_authorization = True
        self.endpoint_authn_method = 'sasl'

        self.redpanda.set_security_settings(self.security)

    def _put_license(self):
        license = sample_license()
        if license is None:
            return None
        assert self.admin.put_license(license).status_code == 200, \
            "License update failed"

        def obtain_license(node):
            lic = self.admin.get_license(node=node)
            return (lic is not None and lic['loaded'] is True, lic)

        result = None
        for n in self.redpanda.nodes:
            resp = wait_until_result(lambda: obtain_license(n),
                                     timeout_sec=5,
                                     backoff_sec=1)
            assert resp['license'] is not None, "License upload failed!"
            result = resp['license'] if result is None else result

        return result

    @skip_fips_mode
    @cluster(num_nodes=3)
    @matrix(with_license=[
        True,
        False,
    ])
    def test_get_enterprise(self, with_license):
        if with_license:
            lic = self._put_license()
            if lic is None:
                self.logger.info(
                    "Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
                return

        rsp = self.admin.get_enterprise_features().json()

        expect_status = EnterpriseLicenseStatus.valid if with_license else EnterpriseLicenseStatus.not_present
        status = rsp.get('license_status', None)
        assert type(status) == str, f"Ill-formed license_status {type(status)}"
        try:
            assert EnterpriseLicenseStatus(status) == expect_status, \
                f"Unexpected status '{status}'"
        except ValueError:
            assert False, f"Unexpected status in response: '{status}'"

        violation = rsp.get('violation', None)
        assert type(violation) == bool, \
            f"Ill-formed violation flag {type(violation)}"
        assert not violation, "Config unexpectedly in violation"

        features_rsp = [f['name'] for f in rsp.get('features', [])]

        assert set([f.name for f in Features]) == set(features_rsp), \
            f"Unexpected feature list: {json.dumps(features_rsp, indent=1)}"

    @skip_fips_mode
    @cluster(num_nodes=3)
    @matrix(feature=[f for f in Features if f not in SKIP_FEATURES],
            with_license=[
                True,
                False,
            ])
    def test_license_violation(self, feature, with_license):
        if with_license:
            lic = self._put_license()
            if lic is None:
                self.logger.info(
                    "Skipping test, REDPANDA_SAMPLE_LICENSE env var not found")
                return

        if feature == Features.audit_logging:
            self.redpanda.set_cluster_config(
                {
                    'audit_enabled': True,
                },
                expect_restart=True,
            )
        elif feature == Features.cloud_storage:
            self.redpanda.set_cluster_config({'cloud_storage_enabled': 'true'},
                                             expect_restart=True)
        elif feature == Features.partition_auto_balancing_continuous:
            self.redpanda.set_cluster_config(
                {'partition_autobalancing_mode': 'continuous'})
        elif feature == Features.core_balancing_continuous:
            self.redpanda.set_cluster_config(
                {'core_balancing_continuous': 'true'})
        elif feature == Features.gssapi:
            self.redpanda.set_cluster_config(
                {'sasl_mechanisms': ['SCRAM', 'GSSAPI']})
        elif feature == Features.oidc:
            self.redpanda.set_cluster_config(
                {'sasl_mechanisms': ['SCRAM', 'OAUTHBEARER']})
        elif feature == Features.schema_id_validation:
            self.redpanda.set_cluster_config(
                {'enable_schema_id_validation': 'compat'})
        elif feature == Features.rbac:

            # NOTE(oren): make sure the role has propagated to every node since we don't know
            # where the get_enterprise request will go
            def has_role(r: str):
                return all(
                    len(
                        RolesList.from_response(
                            self.admin.list_roles(filter=r, node=n)).roles) > 0
                    for n in self.redpanda.nodes)

            self.admin.create_role('dummy')
            wait_until(lambda: has_role('dummy'),
                       timeout_sec=30,
                       backoff_sec=1)
        elif feature == Features.fips:
            self.redpanda.rolling_restart_nodes(
                self.redpanda.nodes,
                override_cfg_params={
                    'fips_mode':
                    'permissive',
                    "openssl_config_file":
                    self.redpanda.get_openssl_config_file_path(),
                    "openssl_module_directory":
                    self.redpanda.get_openssl_modules_directory()
                })
        elif feature == Features.datalake_iceberg:
            self.redpanda.set_cluster_config({'iceberg_enabled': 'true'},
                                             expect_restart=True)
        elif feature == Features.leadership_pinning:
            RpkTool(self.redpanda).create_topic(
                "foo",
                partitions=1,
                replicas=1,
                config={"redpanda.leaders.preference": "racks:rack1"})
        else:
            assert False, f"Unexpected feature={feature}"

        rsp = self.admin.get_enterprise_features().json()
        enabled = {f['name']: f['enabled'] for f in rsp['features']}
        for f in Features:
            if f == feature:
                assert enabled[f.name], \
                    f"expected {f} enabled, got {json.dumps(enabled, indent=1)}"
            else:
                assert not enabled[f.name], \
                    f"expected {f} not enabled, got {json.dumps(enabled, indent=1)}"

        expect_status = EnterpriseLicenseStatus.valid if with_license else EnterpriseLicenseStatus.not_present
        status_str = rsp.get('license_status')
        try:
            status = EnterpriseLicenseStatus(status_str)
            assert status == expect_status, f"Unexpected license status: {status} (expected {expect_status})"
        except ValueError:
            assert False, f"Unexpected status in response: {status_str}"

        violation = rsp.get('violation')
        assert violation == (not with_license), \
            f"Expected{' no' if with_license else ''} enterprise license violation, got violation='{violation}'"
