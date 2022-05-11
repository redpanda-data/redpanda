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
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool, ClusterAuthorizationError
from rptest.services.redpanda import SecurityConfig, TLSProvider
from rptest.services import tls


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
        return self.tls.create_cert(socket.gethostname(), name=name)


class AccessControlListTest(RedpandaTest):
    password = "password"
    algorithm = "SCRAM-SHA-256"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)

    def setUp(self):
        # Skip starting redpanda, so that test can explicitly start
        # it with custom security settings
        return

    def prepare_cluster(self, use_tls, use_sasl, principal_mapping_rules=None):
        self.security = SecurityConfig()
        self.security.enable_sasl = use_sasl
        self.security.enable_mtls_identity = use_tls and not use_sasl

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

        if self.security.enable_mtls_identity and principal_mapping_rules is not None:
            self.security.principal_mapping_rules = principal_mapping_rules

        self.redpanda.set_security_settings(self.security)
        self.redpanda.start()

        admin = Admin(self.redpanda)

        if self.security.enable_mtls_identity:
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
        if use_sasl:
            admin.create_user("base", self.password, self.algorithm)

        # only grant cluster describe permission to user cluster_describe
        if use_sasl:
            admin.create_user("cluster_describe", self.password,
                              self.algorithm)
        client = self.get_super_client()
        client.acl_create_allow_cluster("cluster_describe", "describe")

        # there is not a convenient interface for waiting for acls to propogate
        # to all nodes so when we are using mtls only for identity we inject a
        # sleep here to try to avoid any acl propogation races.
        if self.security.enable_mtls_identity:
            time.sleep(5)
            return

        # wait for users to proogate to nodes
        def users_propogated():
            for node in self.redpanda.nodes:
                users = admin.list_users(node=node)
                if "base" not in users or "cluster_describe" not in users:
                    return False
            return True

        wait_until(users_propogated, timeout_sec=10, backoff_sec=1)

    def get_client(self, username):
        if self.security.enable_mtls_identity:
            if username == "base":
                cert = self.base_user_cert
            elif username == "cluster_describe":
                cert = self.cluster_describe_user_cert
            else:
                assert False, f"unknown user {username}"

            return RpkTool(self.redpanda, tls_cert=cert)

        # uses base user cert with no explicit permissions. the cert should only
        # participate in tls handshake and not principal extraction.
        cert = None if not self.security.tls_provider else self.base_user_cert
        return RpkTool(self.redpanda,
                       username=username,
                       password=self.password,
                       sasl_mechanism=self.algorithm,
                       tls_cert=cert)

    def get_super_client(self):
        if self.security.enable_mtls_identity:
            return RpkTool(self.redpanda, tls_cert=self.admin_user_cert)

        username, password, _ = self.redpanda.SUPERUSER_CREDENTIALS

        # uses base user cert with no explicit permissions. the cert should only
        # participate in tls handshake and not principal extraction.
        cert = None if not self.security.tls_provider else self.base_user_cert
        return RpkTool(self.redpanda,
                       username=username,
                       password=password,
                       sasl_mechanism=self.algorithm,
                       tls_cert=cert)

    @cluster(num_nodes=3)
    @parametrize(use_tls=False,
                 use_sasl=True)  # plaintext conn + sasl for authn
    @parametrize(use_tls=True, use_sasl=True)  # ssl/tls conn + sasl for authn
    @parametrize(use_tls=True, use_sasl=False)  # ssl/tls conn + mtls for authn
    def test_describe_acls(self, use_tls, use_sasl):
        """
        security::acl_operation::describe, security::default_cluster_name
        """
        self.prepare_cluster(use_tls, use_sasl)

        # run a few times for good health
        for _ in range(5):
            try:
                self.get_client("base").acl_list()
                assert False, "list acls should have failed"
            except ClusterAuthorizationError:
                pass

            self.get_client("cluster_describe").acl_list()
            self.get_super_client().acl_list()
