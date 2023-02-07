# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import socket
import time
from ducktape.errors import TimeoutError
from ducktape.mark import parametrize, matrix, ok_to_fail
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool, ClusterAuthorizationError, RpkException
from rptest.services.redpanda import SecurityConfig, TLSProvider
from rptest.services.redpanda_installer import RedpandaInstaller, wait_for_num_versions
from rptest.services import tls
from typing import Optional


class MTLSProvider(TLSProvider):
    def __init__(self, tls):
        self.tls = tls

    @property
    def ca(self):
        return self.tls.ca

    def create_broker_cert(self, redpanda, node):
        assert node in redpanda.nodes
        return self.tls.create_cert(node.name)

    def create_service_client_cert(self, _, name):
        return self.tls.create_cert(socket.gethostname(),
                                    name=name,
                                    common_name=name)


class AccessControlListTest(RedpandaTest):
    password = "password"
    algorithm = "SCRAM-SHA-256"

    def __init__(self, *args, **kwargs):
        super().__init__(*args,
                         num_brokers=3,
                         skip_if_no_redpanda_log=True,
                         **kwargs)
        self.base_user_cert = None
        self.cluster_describe_user_cert = None
        self.admin_user_cert = None
        self.admin = Admin(self.redpanda)

    def setUp(self):
        # Skip starting redpanda, so that test can explicitly start
        # it with custom security settings
        return

    def authz_enabled(self, use_sasl, enable_authz) -> bool:
        if enable_authz is not None:
            return enable_authz
        return use_sasl

    def authn_enabled(self) -> bool:
        return self.security.sasl_enabled(
        ) or self.security.mtls_identity_enabled()

    def prepare_cluster(self,
                        use_tls: bool,
                        use_sasl: bool,
                        enable_authz: Optional[bool] = None,
                        authn_method: Optional[str] = None,
                        principal_mapping_rules: Optional[str] = None,
                        client_auth: bool = True,
                        expect_fail: bool = False):
        self.security = SecurityConfig()
        self.security.enable_sasl = use_sasl
        self.security.kafka_enable_authorization = enable_authz
        self.security.endpoint_authn_method = authn_method
        self.security.require_client_auth = client_auth

        if use_tls:
            self.tls = tls.TLSCertManager(self.logger)

            # cert for principal with no explicitly granted permissions
            self.base_user_cert = self.tls.create_cert(socket.gethostname(),
                                                       common_name="morty",
                                                       name="base_client")

            # cert for principal with cluster describe permissions
            self.cluster_describe_user_cert = self.tls.create_cert(
                socket.gethostname(),
                common_name="cluster_describe",
                name="cluster_describe_client")

            # cert for admin user used to bootstrap
            self.admin_user_cert = self.tls.create_cert(
                socket.gethostname(),
                common_name="admin",
                name="test_admin_client")

            self.security.tls_provider = MTLSProvider(self.tls)

        if self.security.mtls_identity_enabled():
            if principal_mapping_rules is not None:
                self.security.principal_mapping_rules = principal_mapping_rules
            self.redpanda.add_extra_rp_conf({
                'kafka_mtls_principal_mapping_rules':
                [self.security.principal_mapping_rules]
            })

        self.redpanda.set_security_settings(self.security)
        self.redpanda.start(expect_fail=expect_fail)
        if expect_fail:
            # If we got this far without exception, RedpandaService.start
            # has successfully confirmed that redpanda failed to start.
            return

        # base case user is not a superuser and has no configured ACLs
        if self.security.sasl_enabled() or enable_authz:
            self.admin.create_user("base", self.password, self.algorithm)

        # only grant cluster describe permission to user cluster_describe
        if self.security.sasl_enabled() or enable_authz:
            self.admin.create_user("cluster_describe", self.password,
                                   self.algorithm)
        client = self.get_super_client()
        client.acl_create_allow_cluster("cluster_describe", "describe")

        # Hack: create a user, so that we can watch for this user in order to
        # confirm that all preceding controller log writes landed: this is
        # an indirect way to check that ACLs (and users) have propagated
        # to all nodes before we proceed.
        checkpoint_user = "_test_checkpoint"
        self.admin.create_user(checkpoint_user, "_password", self.algorithm)

        # wait for users to propagate to nodes
        def auth_metadata_propagated():
            for node in self.redpanda.nodes:
                users = self.admin.list_users(node=node)
                if checkpoint_user not in users:
                    return False
                elif self.security.sasl_enabled() or enable_authz:
                    assert "base" in users and "cluster_describe" in users
            return True

        wait_until(auth_metadata_propagated, timeout_sec=10, backoff_sec=1)

    def get_client(self, username):
        if self.security.mtls_identity_enabled(
        ) or not self.security.sasl_enabled():
            if username == "base":
                cert = self.base_user_cert
            elif username == "cluster_describe":
                cert = self.cluster_describe_user_cert
            else:
                assert False, f"unknown user {username}"

            return RpkTool(self.redpanda, tls_cert=cert)

        # uses base user cert with no explicit permissions. the cert should only
        # participate in tls handshake and not principal extraction.
        return RpkTool(self.redpanda,
                       username=username,
                       password=self.password,
                       sasl_mechanism=self.algorithm,
                       tls_cert=self.base_user_cert)

    def get_super_client(self):
        if self.security.mtls_identity_enabled(
        ) or not self.security.sasl_enabled():
            return RpkTool(self.redpanda, tls_cert=self.admin_user_cert)

        username, password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        # uses base user cert with no explicit permissions. the cert should only
        # participate in tls handshake and not principal extraction.
        return RpkTool(self.redpanda,
                       username=username,
                       password=password,
                       sasl_mechanism=self.algorithm,
                       tls_cert=self.base_user_cert)

    def check_permissions(self,
                          pass_w_base_user: Optional[bool] = None,
                          pass_w_cluster_user: Optional[bool] = None,
                          pass_w_super_user: Optional[bool] = None,
                          timeout_sec: int = 90,
                          err_msg: str = '',
                          repeat_check: int = 3):
        # Check user permissions on the cluster
        #
        # :param pass_w_base_user: Should perms check pass with the base user?
        # :param pass_w_cluster_user: Should perms check pass with the user with cluster describe perms?
        # :param pass_w_super_user: Should perms check pass with the super user?
        # :param err_msg: error message to pass to wait until
        # :param repeat_check: how many times to repeat perms check?

        # :raise:  TimeoutError if a perms check fails. Pass otherwise
        def check_base_user_perms():
            self.logger.debug(
                f'list acls with base user, expect pass: {pass_w_base_user}')
            try:
                self.get_client("base").acl_list()
                return pass_w_base_user
            except ClusterAuthorizationError:
                return not pass_w_base_user

        def check_cluster_user_perms():
            self.logger.debug(
                f'list acls with cluster user, expect pass: {pass_w_cluster_user}'
            )
            try:
                self.get_client("cluster_describe").acl_list()
                return pass_w_cluster_user
            except Exception:
                return not pass_w_cluster_user

        def check_super_user_perms():
            self.logger.debug(
                f'list acls with super user, expect pass: {pass_w_super_user}')
            try:
                self.get_super_client().acl_list()
                return pass_w_super_user
            except Exception:
                return not pass_w_super_user

        # Run a few times for good health. The target condition
        # should be consistent
        for _ in range(repeat_check):
            if pass_w_base_user != None:
                wait_until(check_base_user_perms,
                           timeout_sec=timeout_sec,
                           err_msg=f'base user: {err_msg}')

            if pass_w_cluster_user != None:
                wait_until(check_cluster_user_perms,
                           timeout_sec=timeout_sec,
                           err_msg=f'cluster user: {err_msg}')

            if pass_w_super_user != None:
                wait_until(check_super_user_perms,
                           timeout_sec=timeout_sec,
                           err_msg=f'super user: {err_msg}')

    '''
    The old config style has use_sasl at the top level, which enables
    authorization. New config style has kafka_enable_authorization at the
    top-level, with authentication_method on the listener.

    Brief param descriptions:
        use_tls - Controls whether tls certs are used
        use_sasl - Controls the value of enable_sasl RP config
        enable_authz - Controls the value of kafka_enable_authorization RP config
        authn_method - Controls the broker level authentication_method (e.g., mtls_identity)
        client_auth - Controls the value of require_client_auth RP config
    '''

    @cluster(num_nodes=3, log_allow_list=["Validation errors in node config"])
    @matrix(use_tls=[True, False],
            use_sasl=[True, False],
            enable_authz=[True, False, None],
            authn_method=[None, 'sasl', 'mtls_identity', 'none'],
            client_auth=[True, False])
    def test_describe_acls(self, use_tls: bool, use_sasl: bool,
                           enable_authz: Optional[bool],
                           authn_method: Optional[str], client_auth: bool):
        """
        security::acl_operation::describe, security::default_cluster_name
        """
        def expect_startup_failure(use_tls: bool, authn_method: Optional[str],
                                   client_auth: bool):
            if authn_method == 'mtls_identity':
                return not use_tls or not client_auth
            return False

        startup_should_fail = expect_startup_failure(use_tls, authn_method,
                                                     client_auth)

        self.logger.info(f"startup_should_fail={startup_should_fail}")

        prepare_failed_auth = False
        try:
            self.prepare_cluster(use_tls,
                                 use_sasl,
                                 enable_authz,
                                 authn_method,
                                 client_auth=client_auth,
                                 expect_fail=startup_should_fail)
        except RpkException as e:
            if "CLUSTER_AUTHORIZATION_FAILED" not in str(e):
                raise
            prepare_failed_auth = True

        # these combinations end up causing anonymous user and so we get authz
        # failed rejected when preparing the cluster above.
        if authn_method == "none" and enable_authz is True:
            assert prepare_failed_auth
        elif authn_method == "none" and enable_authz is None and use_sasl:
            assert prepare_failed_auth
        elif authn_method is None and enable_authz is True:
            assert prepare_failed_auth
        else:
            assert not prepare_failed_auth

        if startup_should_fail:
            return

        def should_pass_w_base_user(use_sasl: bool,
                                    enable_authz: Optional[bool]) -> bool:
            return not self.authz_enabled(use_sasl, enable_authz)

        def should_pass_w_authn_user(use_tls: bool, use_sasl: bool,
                                     enable_authz: Optional[bool],
                                     client_auth: bool) -> bool:
            if should_pass_w_base_user(use_sasl, enable_authz):
                return True
            if self.security.mtls_identity_enabled():
                return use_tls and client_auth
            if self.security.sasl_enabled():
                return True
            return False

        pass_w_base_user = should_pass_w_base_user(use_sasl, enable_authz)

        pass_w_authn_user = should_pass_w_authn_user(use_tls, use_sasl,
                                                     enable_authz, client_auth)

        self.check_permissions(pass_w_base_user=pass_w_base_user,
                               pass_w_cluster_user=pass_w_authn_user,
                               pass_w_super_user=pass_w_authn_user,
                               err_msg='check_permissions failed')

    # Test mtls identity
    # Principals in use:
    # * redpanda.service.admin: the default admin client
    # * admin: used for acl bootstrap
    # * cluster_describe: the principal under test
    @cluster(num_nodes=3)
    # DEFAULT: The whole SAN
    @parametrize(rules="DEFAULT", fail=True)
    #  Match admin, or O (Redpanda)
    @parametrize(
        rules=
        "RULE:^O=Redpanda,CN=(redpanda.service.admin|admin)$/$1/, RULE:^O=([^,]+),CN=(.*?)$/$1/",
        fail=True)
    # Wrong Case
    @parametrize(rules="RULE:^O=Redpanda,CN=(.*?)$/$1/U", fail=True)
    # Match CN
    @parametrize(rules="RULE:^O=Redpanda,CN=(.*?)$/$1/L", fail=False)
    # Full Match
    @parametrize(
        rules=
        "RULE:^O=Redpanda,CN=(cluster_describe|redpanda.service.admin|admin)$/$1/",
        fail=False)
    # Match admin or empty
    @parametrize(
        rules=
        "RULE:^O=Redpanda,CN=(admin|redpanda.service.admin)$/$1/, RULE:^O=Redpanda,CN=()$/$1/L",
        fail=True)
    def test_mtls_principal(self, rules=None, fail=False):
        """
        security::acl_operation::describe, security::default_cluster_name
        """
        prepare_failed_auth = False
        try:
            self.prepare_cluster(use_tls=True,
                                 use_sasl=False,
                                 enable_authz=True,
                                 authn_method="mtls_identity",
                                 principal_mapping_rules=rules)
        except RpkException as e:
            if "CLUSTER_AUTHORIZATION_FAILED" not in str(e):
                raise
            prepare_failed_auth = True

        # fail will be cluster auth when preparing, but one case the failure
        # comes later (see below in check permissions)
        if fail and "service.admin" not in rules:
            assert prepare_failed_auth
        else:
            assert not prepare_failed_auth

        self.check_permissions(pass_w_cluster_user=not fail,
                               err_msg='check_permissions failed')

    @cluster(num_nodes=3)
    @parametrize(authn_method='sasl')
    @parametrize(authn_method='mtls_identity')
    def test_security_feature_migration(self, authn_method: str):
        self.prepare_cluster(use_tls=True, use_sasl=True)

        self.check_permissions(
            pass_w_base_user=False,
            pass_w_cluster_user=True,
            pass_w_super_user=True,
            err_msg='check_permissions failed before migration')

        # Change the authn method and initiate feature migration
        # with a rolling restart. Do not recreate tls certs.
        self.security.endpoint_authn_method = authn_method
        self.redpanda.set_security_settings(self.security)
        self.logger.debug('Initiating rolling restart')
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                            start_timeout=90,
                                            stop_timeout=90)

        # Check that the authn_method was set on all brokers
        pattern = 'Started Kafka API server.*:{' + authn_method + '}.*'
        assert self.redpanda.search_log_all(pattern)

        # Once restart is complete, check permissions should succeed when authn_method
        # is sasl because the system will validate against SASL/SCRAM creds already stored
        # on the brokers. However, when authn_method is mtls_identiy, check permissions
        # should fail because no principal mapping rule is set yet.
        if authn_method == 'sasl':
            self.check_permissions(
                pass_w_base_user=False,
                pass_w_cluster_user=True,
                pass_w_super_user=True,
                err_msg='check_permissions failed in intermediate state')
        elif authn_method == 'mtls_identity':
            try:
                self.check_permissions(pass_w_base_user=False,
                                       pass_w_cluster_user=True,
                                       pass_w_super_user=True,
                                       timeout_sec=10)
                raise RuntimeError(
                    'check_perms should have failed in intermediate state')
            except TimeoutError:
                # Expect a timeout failure which comes from
                # wait_until. The timeout is because check_perms
                # should fail.
                pass

        # Change cluster wide configs for kafka_enable_authorization and
        # kafka_mtls_principal_mapping_rules via the admin api.
        # Do not expect a restart.
        #
        # NOTE:
        # - kafka_enable_authorization overrides sasl in an authorization context
        # - authentication_method overrides sasl in an authentication context
        new_cfg = {'kafka_enable_authorization': True}
        if authn_method == 'mtls_identity':
            new_cfg['kafka_mtls_principal_mapping_rules'] = [
                'RULE:.*CN=([^,]+).*/$1/', 'DEFAULT'
            ]
        self.redpanda.set_cluster_config(new_cfg)

        # Check that new cluster configs are set
        cluster_cfg = self.admin.get_cluster_config()
        assert cluster_cfg['kafka_enable_authorization'] == new_cfg[
            'kafka_enable_authorization']

        if authn_method == 'mtls_identity':
            assert new_cfg['kafka_mtls_principal_mapping_rules'][
                0] in cluster_cfg['kafka_mtls_principal_mapping_rules']
            assert new_cfg['kafka_mtls_principal_mapping_rules'][
                1] in cluster_cfg['kafka_mtls_principal_mapping_rules']
        else:
            assert cluster_cfg['kafka_mtls_principal_mapping_rules'] == None

        self.check_permissions(
            pass_w_base_user=False,
            pass_w_cluster_user=True,
            pass_w_super_user=True,
            err_msg='check_permissions failed after migration')


class AccessControlListTestUpgrade(AccessControlListTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.installer = self.redpanda._installer

    # Test that a cluster configured with enable_sasl can be upgraded
    # from v22.1.x, and still have sasl enabled. See PR 5292.
    @cluster(num_nodes=3,
             log_allow_list=[
                 r'rpc - .* The TLS connection was non-properly terminated.*'
             ])
    def test_upgrade_sasl(self):
        old_version, old_version_str = self.installer.install(
            self.redpanda.nodes, (22, 1))
        self.prepare_cluster(use_tls=True,
                             use_sasl=True,
                             enable_authz=None,
                             authn_method=None,
                             principal_mapping_rules=None)

        self.check_permissions(
            pass_w_base_user=False,
            pass_w_cluster_user=True,
            pass_w_super_user=True,
            err_msg='check_permissions failed before upgrade')

        self.installer.install(self.redpanda.nodes, (22, 2))
        self.redpanda.restart_nodes(self.redpanda.nodes)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert old_version_str not in unique_versions

        self.check_permissions(
            pass_w_base_user=False,
            pass_w_cluster_user=True,
            pass_w_super_user=True,
            err_msg='check_permissions failed after upgrade')
