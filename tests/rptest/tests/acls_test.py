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
from ducktape.mark import parametrize, matrix, ignore
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool, ClusterAuthorizationError
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

        admin = Admin(self.redpanda)

        # base case user is not a superuser and has no configured ACLs
        if self.security.sasl_enabled() or enable_authz:
            admin.create_user("base", self.password, self.algorithm)

        # only grant cluster describe permission to user cluster_describe
        if self.security.sasl_enabled() or enable_authz:
            admin.create_user("cluster_describe", self.password,
                              self.algorithm)
        client = self.get_super_client()
        client.acl_create_allow_cluster("cluster_describe", "describe")

        # Hack: create a user, so that we can watch for this user in order to
        # confirm that all preceding controller log writes landed: this is
        # an indirect way to check that ACLs (and users) have propagated
        # to all nodes before we proceed.
        checkpoint_user = "_test_checkpoint"
        admin.create_user(checkpoint_user, "_password", self.algorithm)

        # wait for users to propagate to nodes
        def auth_metadata_propagated():
            for node in self.redpanda.nodes:
                users = admin.list_users(node=node)
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

        self.prepare_cluster(use_tls,
                             use_sasl,
                             enable_authz,
                             authn_method,
                             client_auth=client_auth,
                             expect_fail=startup_should_fail)

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

        # run a few times for good health
        for _ in range(2):
            try:
                self.get_client("base").acl_list()
                assert pass_w_base_user, "list acls should have failed for base user"
            except ClusterAuthorizationError:
                assert not pass_w_base_user

            try:
                self.get_client("cluster_describe").acl_list()
                assert pass_w_authn_user, "list acls should have failed for cluster user"
            except ClusterAuthorizationError:
                assert not pass_w_authn_user

            try:
                self.get_super_client().acl_list()
                assert pass_w_authn_user, "list acls should have failed for super user"
            except ClusterAuthorizationError:
                assert not pass_w_authn_user

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
        self.prepare_cluster(use_tls=True,
                             use_sasl=False,
                             enable_authz=True,
                             authn_method="mtls_identity",
                             principal_mapping_rules=rules)

        # run a few times for good health
        for _ in range(5):
            try:
                self.get_client("cluster_describe").acl_list()
                assert not fail, "list acls should have failed"
            except ClusterAuthorizationError:
                assert fail, "list acls should have succeeded"


class AccessControlListTestUpgrade(AccessControlListTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.installer = self.redpanda._installer

    def check_permissions(self):
        # run a few times for good health
        for _ in range(5):
            try:
                self.get_client("base").acl_list()
                assert False, "list acls should have failed"
            except ClusterAuthorizationError:
                pass

            self.get_client("cluster_describe").acl_list()
            self.get_super_client().acl_list()

    # Test that a cluster configured with enable_sasl can be upgraded
    # from v22.1.x, and still have sasl enabled. See PR 5292.
    @cluster(num_nodes=3)
    def test_upgrade_sasl(self):

        self.installer.install(self.redpanda.nodes, (22, 1, 3))
        self.prepare_cluster(use_tls=True,
                             use_sasl=True,
                             enable_authz=None,
                             authn_method=None,
                             principal_mapping_rules=None)

        self.check_permissions()

        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert "v22.1.3" not in unique_versions

        self.check_permissions()
