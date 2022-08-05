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
from ducktape.mark import parametrize, matrix, ignore
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool, ClusterAuthorizationError
from rptest.services.redpanda import SecurityConfig, TLSProvider
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
        super().__init__(*args, num_brokers=3, **kwargs)
        self.base_user_cert = None
        self.cluster_describe_user_cert = None
        self.admin_user_cert = None

    def setUp(self):
        # Skip starting redpanda, so that test can explicitly start
        # it with custom security settings
        return

    def authz_enabled(self, use_sasl, enable_authz):
        if enable_authz != None and enable_authz:
            return True
        elif enable_authz == None and use_sasl:
            return True
        else:
            return False

    def authn_enabled(self, use_sasl, enable_authz, authn_method):
        if authn_method != None:
            return True
        elif authn_method == None and enable_authz == None and use_sasl:
            return True
        else:
            return False

    def prepare_cluster(self,
                        use_tls: bool,
                        use_sasl: bool,
                        enable_authz: Optional[bool] = None,
                        authn_method: Optional[str] = None,
                        principal_mapping_rules: Optional[str] = None,
                        client_auth: bool = True):
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
        self.redpanda.start()

        admin = Admin(self.redpanda)

        if self.security.mtls_identity_enabled():
            feature_name = "mtls_authentication"
            admin.put_feature(feature_name, {"state": "active"})

            # wait for feature to be active so that tests don't have to retry
            def check_feature_active():
                for f in admin.get_features()["features"]:
                    if f["name"] == feature_name and f["state"] == "active":
                        return True
                return False

            wait_until(check_feature_active, timeout_sec=10, backoff_sec=1)

        # base case user is not a superuser and has no configured ACLs
        if self.security.sasl_enabled() or enable_authz:
            admin.create_user("base", self.password, self.algorithm)

        # only grant cluster describe permission to user cluster_describe
        if self.security.sasl_enabled() or enable_authz:
            admin.create_user("cluster_describe", self.password,
                              self.algorithm)
        client = self.get_super_client()
        client.acl_create_allow_cluster("cluster_describe", "describe")

        # there is not a convenient interface for waiting for acls to propogate
        # to all nodes so when we are using mtls only for identity we inject a
        # sleep here to try to avoid any acl propogation races.
        if self.security.mtls_identity_enabled():
            time.sleep(5)
            return

        # wait for users to propagate to nodes
        def users_propogated():
            for node in self.redpanda.nodes:
                users = admin.list_users(node=node)
                if "base" not in users or "cluster_describe" not in users:
                    return False
            return True

        if self.security.sasl_enabled() or enable_authz:
            wait_until(users_propogated, timeout_sec=10, backoff_sec=1)

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

    @cluster(num_nodes=3)
    @matrix(use_tls=[True, False],
            use_sasl=[True, False],
            enable_authz=[True, False, None],
            authn_method=[None, 'sasl', 'mtls_identity'],
            client_auth=[True, False])
    def test_describe_acls(self, use_tls: bool, use_sasl: bool,
                           enable_authz: Optional[bool],
                           authn_method: Optional[str], client_auth: bool):
        """
        security::acl_operation::describe, security::default_cluster_name
        """
        self.prepare_cluster(use_tls,
                             use_sasl,
                             enable_authz,
                             authn_method,
                             client_auth=client_auth)

        def should_pass_w_base_user(use_tls: bool, use_sasl: bool,
                                    enable_authz: Optional[bool],
                                    authn_method: Optional[str]):
            if enable_authz is False:
                return True
            elif not use_sasl and enable_authz is not True:
                return True
            else:
                return False

        def should_always_fail(use_tls: bool, use_sasl: bool,
                               enable_authz: Optional[bool],
                               authn_method: Optional[str]):
            if enable_authz is False:
                return False
            if not use_sasl and enable_authz is not True:
                return False
            if enable_authz is True and authn_method is None:
                return True
            if enable_authz is not False and authn_method == 'mtls_identity' and not (
                    use_tls and client_auth):
                return True
            return False

        pass_w_base_user = should_pass_w_base_user(use_tls, use_sasl,
                                                   enable_authz, authn_method)

        should_always_fail = should_always_fail(use_tls, use_sasl,
                                                enable_authz, authn_method)
        # run a few times for good health
        for _ in range(5):
            try:
                self.get_client("base").acl_list()
                assert pass_w_base_user, "list acls should have failed for base user"
            except ClusterAuthorizationError:
                assert not pass_w_base_user

            try:
                self.get_client("cluster_describe").acl_list()
                assert not should_always_fail, "list acls should have failed for cluster user"
            except ClusterAuthorizationError:
                assert should_always_fail

            try:
                self.get_super_client().acl_list()
                assert not should_always_fail, "list acls should have failed for super user"
            except ClusterAuthorizationError:
                assert should_always_fail

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
