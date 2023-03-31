# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import requests

from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.pandaproxy_test import PandaProxyEndpoints
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import SaslCredentials, SecurityConfig
from rptest.util import expect_exception, expect_http_error

from ducktape.utils.util import wait_until


def create_user_and_wait(redpanda, admin: Admin, creds: SaslCredentials):
    admin.create_user(*creds)

    def user_exists_everywhere():
        for node in redpanda.nodes:
            users = redpanda._admin.list_users(node=node)
            if creds.username not in users:
                redpanda.logger.info(f"{creds.username} not in {users}")
                return False

        return True

    # It should only take milliseconds for raft0 write to propagate
    wait_until(user_exists_everywhere, timeout_sec=5, backoff_sec=0.5)


# A user account who is not the default superuser
ALICE = SaslCredentials("alice", "itsMeH0nest", "SCRAM-SHA-256")


class AdminApiAuthTest(RedpandaTest):
    """
    Test the behaviour of a redpanda cluster with admin API authentication
    enabled.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.rpk = RpkTool(self.redpanda)

        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS

        self.superuser_admin = Admin(self.redpanda,
                                     auth=(self.superuser.username,
                                           self.superuser.password))
        self.regular_user_admin = Admin(self.redpanda,
                                        auth=(ALICE.username, ALICE.password))
        self.anonymous_admin = Admin(self.redpanda)

    def setUp(self):
        super().setUp()
        create_user_and_wait(self.redpanda, self.anonymous_admin, ALICE)

        self.redpanda.set_cluster_config({'admin_api_require_auth': True})

    @cluster(num_nodes=3)
    def test_superuser_access(self):
        # A superuser may access the config API
        self.superuser_admin.get_cluster_config()

    @cluster(num_nodes=3)
    def test_regular_user_access(self):
        # A non-superuser may not access the config API
        with expect_http_error(403):
            self.regular_user_admin.get_cluster_config()

    @cluster(num_nodes=3)
    def test_anonymous_access(self):
        # An anonymous user may not access the config API
        with expect_http_error(403):
            self.anonymous_admin.get_cluster_config()

        # An anonymous user may access unauthenticated endpoints
        self.anonymous_admin.get_status_ready()
        self.redpanda.metrics(self.redpanda.nodes[0])

    @cluster(num_nodes=3)
    def test_scram_sha512(self):
        """
        Check that username/password authentication works for users that
        were created using the scram_sha512 mechanism (as opposed to the
        default scram_sha256)
        """

        charles = SaslCredentials("charles", "highEntropyHipster",
                                  "SCRAM-SHA-512")
        create_user_and_wait(self.redpanda, self.superuser_admin, charles)
        self.redpanda.set_cluster_config({
            'superusers':
            [charles.username, self.redpanda.SUPERUSER_CREDENTIALS.username]
        })

        charles_admin = Admin(self.redpanda,
                              auth=(charles.username, charles.password))
        # Hit an endpoint requiring superuser
        charles_admin.get_cluster_config()


class AdminApiAuthEnablementTest(RedpandaTest):
    """
    Test redpanda's rules for when admin API auth may be switched on
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @cluster(num_nodes=3)
    def test_no_superusers(self):
        anonymous_admin = Admin(self.redpanda)

        # Nobody may enable auth if there are no superusers
        self.redpanda.set_cluster_config({'superusers': []})
        with expect_http_error(400):
            self.redpanda.set_cluster_config({'admin_api_require_auth': True})
        with expect_http_error(400):
            anonymous_admin.patch_cluster_config(
                {'admin_api_require_auth': True})

        # Once we are a superuser, we can enable auth
        self.redpanda.set_cluster_config(
            {'superusers': [self.redpanda.SUPERUSER_CREDENTIALS.username]})
        self.redpanda.set_cluster_config({'admin_api_require_auth': True})

        # Once auth is enabled, we cannot clear the superusers list
        with expect_http_error(400):
            self.redpanda.set_cluster_config({'superusers': []})

    @cluster(num_nodes=3)
    def test_not_a_superuser(self):
        anonymous_admin = Admin(self.redpanda)

        # Nobody may enable auth unless they are themselves in the superusers list
        self.redpanda.set_cluster_config({'superusers': ['bob']})
        with expect_http_error(400):
            self.redpanda.set_cluster_config({'admin_api_require_auth': True})
        with expect_http_error(400):
            anonymous_admin.patch_cluster_config(
                {'admin_api_require_auth': True})

        # A superuser may enable auth
        self.redpanda.set_cluster_config({
            'superusers':
            ['bob', self.redpanda.SUPERUSER_CREDENTIALS.username]
        })
        self.redpanda.set_cluster_config({'admin_api_require_auth': True})

    @cluster(num_nodes=3)
    def test_combined_request(self):
        """
        Check that the API accepts a config update that simultaneously updates superusers
        and enables auth.
        """
        regular_user_admin = Admin(self.redpanda,
                                   auth=(ALICE.username, ALICE.password))

        # We can use our regular user for admin API access right away, because
        # we didn't enable authentication yet.
        create_user_and_wait(self.redpanda, regular_user_admin, ALICE)

        regular_user_admin.patch_cluster_config({
            'admin_api_require_auth':
            True,
            "superusers":
            [self.redpanda.SUPERUSER_CREDENTIALS.username, ALICE.username]
        })


class AdminApiListUsersTest(PandaProxyEndpoints):
    def __init__(self, context):
        security = SecurityConfig()
        security.kafka_enable_authorization = True
        security.endpoint_authn_method = 'sasl'
        security.auto_auth = True

        super(AdminApiListUsersTest, self).__init__(context, security=security)

        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS
        self.superuser_admin = Admin(self.redpanda,
                                     auth=(self.superuser.username,
                                           self.superuser.password))

    @cluster(num_nodes=3)
    def test_list_users(self):
        # Create ephemeral users for each pandaproxy instance
        pp_hosts = [node.account.hostname for node in self.redpanda.nodes]
        for host in pp_hosts:
            res = requests.get(f"http://{host}:8082/status/ready")
            assert res.status_code == requests.codes.ok

        users = self.superuser_admin.list_users()
        ephemeral_users = self.superuser_admin.list_users(
            include_ephemeral=True)

        self.logger.debug(
            f"users: {users}\n:ephemeral_users: {ephemeral_users}\npp_hosts: {pp_hosts}"
        )
        assert len(pp_hosts) > 0
        assert len(ephemeral_users) - len(users) == len(pp_hosts)
