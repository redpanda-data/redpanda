import time
import json
from enum import IntEnum
import requests

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
from rptest.util import wait_until_result, expect_exception


class EnterpriseFeaturesTestBase(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.admin = Admin(self.redpanda)
        self.installer = self.redpanda._installer

    def setUp(self):
        return


class Feature(IntEnum):
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


FEATURE_DEPENDENT_CONFIG = {
    Feature.audit_logging: 'audit_enabled',
    Feature.cloud_storage: 'cloud_storage_enabled',
    Feature.partition_auto_balancing_continuous:
    'partition_autobalancing_mode',
    Feature.core_balancing_continuous: 'core_balancing_continuous',
    Feature.gssapi: 'sasl_mechanisms',
    Feature.oidc: 'sasl_mechanisms',
    Feature.schema_id_validation: 'enable_schema_id_validation',
    Feature.datalake_iceberg: 'datalake_iceberg',
    Feature.leadership_pinning: 'default_leaders_preference',
}

SKIP_FEATURES = [
    Feature.audit_logging,  # NOTE(oren): omit due to shutdown issues
    Feature.
    cloud_storage,  # TODO(oren): initially omitted because it's a bit complicated to initialize infra
    Feature.datalake_iceberg,  # TODO: also depends on cloud infra
    Feature.fips  # NOTE(oren): omit because it's too much of a pain for CDT
]


class EnterpriseFeaturesTest(EnterpriseFeaturesTestBase):
    OIDC_UPDATE_FAILURE_LOGS = [
        # Example: security - oidc_service.cc:232 - Error updating jwks: security::exception (Invalid jwks: Invalid response from jwks_uri: https://auth.prd.cloud.redpanda.com:443/.well-known/jwks.json)"
        "Error updating jwks",
        "Error updating metadata",
    ]

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

    def check_feature(
        self,
        feature: Feature,
        enabled: bool,
        license_valid: bool,
    ) -> dict[Feature, bool]:
        rsp = self.admin.get_enterprise_features().json()
        feature_statuses: dict[Feature, bool] = {
            Feature[f['name']]: f['enabled']
            for f in rsp.get('features', [])
        }
        ELS = EnterpriseLicenseStatus

        compliant = license_valid or not enabled
        violation = not compliant

        assert feature_statuses.get(feature) == enabled, \
            f"Expected {feature.name} {enabled=}"
        assert (ELS(rsp.get('license_status')) == ELS.valid) is license_valid, \
            f"Expected {license_valid=} (got {rsp.get('license_status')})"
        assert rsp.get('violation') == violation, \
            f"Expected {violation=} got {rsp.get('violation')}"

        return feature_statuses

    @skip_fips_mode
    @cluster(num_nodes=3)
    @matrix(with_license=[
        True,
        False,
    ])
    def test_get_enterprise(self, with_license):
        if not with_license:
            self.redpanda.set_environment(
                dict(__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE='1'))

        self.redpanda.start()

        statuses = self.check_feature(Feature.audit_logging,
                                      enabled=False,
                                      license_valid=with_license)

        assert not any(statuses[f] for f in Feature), \
            f"Unexpected status: {json.dumps(statuses)}"

    def try_enable_feature(self, feature):
        if feature == Feature.audit_logging:
            self.redpanda.set_cluster_config(
                {
                    'audit_enabled': True,
                },
                expect_restart=True,
            )
        elif feature == Feature.cloud_storage:
            self.redpanda.set_cluster_config({'cloud_storage_enabled': 'true'},
                                             expect_restart=True)
        elif feature == Feature.partition_auto_balancing_continuous:
            self.redpanda.set_cluster_config(
                {'partition_autobalancing_mode': 'continuous'})
        elif feature == Feature.core_balancing_continuous:
            self.redpanda.set_cluster_config(
                {'core_balancing_continuous': 'true'})
        elif feature == Feature.gssapi:
            self.redpanda.set_cluster_config(
                {'sasl_mechanisms': ['SCRAM', 'GSSAPI']})
        elif feature == Feature.oidc:
            # Note: the default OIDC server is flaky in CI, so use `OIDC_UPDATE_FAILURE_LOGS`
            # to ignore the related error logs in the background updater.
            self.redpanda.set_cluster_config(
                {'sasl_mechanisms': ['SCRAM', 'OAUTHBEARER']})
        elif feature == Feature.schema_id_validation:
            self.redpanda.set_cluster_config(
                {'enable_schema_id_validation': 'compat'})
        elif feature == Feature.rbac:
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
        elif feature == Feature.fips:
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
        elif feature == Feature.datalake_iceberg:
            self.redpanda.set_cluster_config({'iceberg_enabled': 'true'},
                                             expect_restart=True)
        elif feature == Feature.leadership_pinning:
            self.redpanda.set_cluster_config(
                {"default_leaders_preference": "racks:rack1"},
                expect_restart=True)
            RpkTool(self.redpanda).create_topic(
                "foo",
                partitions=1,
                replicas=1,
                config={"redpanda.leaders.preference": "racks:rack1"})
        else:
            assert False, f"Unexpected feature={feature}"

    @skip_fips_mode
    @cluster(num_nodes=3, log_allow_list=OIDC_UPDATE_FAILURE_LOGS)
    @matrix(feature=[f for f in Feature if f not in SKIP_FEATURES],
            install_license=[
                True,
                False,
            ],
            disable_trial=[
                False,
                True,
            ])
    def test_enable_features(self, feature, install_license, disable_trial):
        if disable_trial:
            self.redpanda.set_environment(
                dict(__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE='1'))

        self.redpanda.start()

        if install_license:
            self.redpanda.install_license()

        has_license = not disable_trial or install_license

        expect_rejected = not has_license

        if expect_rejected:
            with expect_exception(
                    requests.exceptions.HTTPError, \
                    lambda e: e.response.status_code == 403 or FEATURE_DEPENDENT_CONFIG[
                        feature] in e.response.json().keys()):
                self.try_enable_feature(feature)
        else:
            self.try_enable_feature(feature)

        self.logger.debug(f"Check that {feature.name} has the expected state")

        statuses = self.check_feature(
            feature,
            enabled=not expect_rejected,
            license_valid=has_license,
        )

        self.logger.debug(
            "Everything else should still be off regardless of license status")

        assert not any([statuses[f] for f in Feature if f != feature]), \
            f"Features unexpectedly enabled: {json.dumps(statuses, indent=1)}"

    @skip_fips_mode
    @cluster(num_nodes=3)
    def test_license_violation(self):
        self.logger.debug(
            "Suppress the trial license before starting redpanda the first time"
        )
        self.redpanda.set_environment(
            dict(__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE='1'))

        self.logger.debug("Switch on any old enterprise feature and start")
        feature = Feature.core_balancing_continuous
        self.redpanda._extra_rp_conf.update(
            {FEATURE_DEPENDENT_CONFIG[feature]: True})
        self.redpanda.start()

        self.logger.debug(
            "Confirm license violation per GET /features/enterprise")
        self.check_feature(feature, enabled=True, license_valid=False)

        self.redpanda.set_cluster_config(
            {FEATURE_DEPENDENT_CONFIG[feature]: 'false'})

        self.logger.debug(
            "We should be able to revert to a compliant setting without issue")

        self.check_feature(feature, enabled=False, license_valid=False)
