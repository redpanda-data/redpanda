# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.failure_injector import make_failure_injector, FailureSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.admin_api_auth_test import create_user_and_wait
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.redpanda import SecurityConfig, SaslCredentials
from rptest.util import expect_exception
from ducktape.utils.util import wait_until


class RpkACLTest(RedpandaTest):
    username = "red"
    password = "panda"
    mechanism = "SCRAM-SHA-256"

    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        security = SecurityConfig()
        security.enable_sasl = True
        security.endpoint_authn_method = 'sasl'
        super(RpkACLTest,
              self).__init__(test_ctx,
                             security=security,
                             extra_rp_conf={'election_timeout_ms': 10000},
                             *args,
                             **kwargs)
        self._rpk = RpkTool(redpanda=self.redpanda,
                            username=self.username,
                            password=self.password,
                            sasl_mechanism=self.mechanism)
        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS

    def _superclient(self):
        return RpkTool(self.redpanda,
                       username=self.superuser.username,
                       password=self.superuser.password,
                       sasl_mechanism=self.mechanism)

    @cluster(num_nodes=3)
    def test_modify_then_query_error(self):
        """
        This test ensures that even in cases where multiple nodes may
        think they are the leader, the freshest data is returned
        """
        superclient = self._superclient()
        superclient.acl_create_allow_cluster(self.superuser.username,
                                             "describe")

        # Modify an ACL
        self._rpk.sasl_allow_principal(f'User:{self.username}', ['CREATE'],
                                       'topic', 'foo', self.superuser.username,
                                       self.superuser.password,
                                       self.superuser.algorithm)
        described = superclient.acl_list()
        assert 'CREATE' in described, "Failed to modify ACL"
        assert self.redpanda.search_log_any(
            ".*Failed waiting on catchup with controller leader.*") is False

        # Network partition the leader away from the rest of the cluster
        with make_failure_injector(self.redpanda) as fi:
            fi.inject_failure(
                FailureSpec(FailureSpec.FAILURE_ISOLATE,
                            self.redpanda.controller()))

            # Timeout must be larger then hardcoded timeout of 5s within redpanda
            _ = superclient.acl_list(request_timeout_overhead=30)

            # Of the other remaining nodes, none can be declared a leader before
            # the election timeout occurs; also the "current" leader is technically
            # stale so it cannot be sure its returning the freshest data either. In
            # all cases the log below should be observed on the node handling the req.
            assert self.redpanda.search_log_any(
                ".*Failed waiting on catchup with controller leader.*") is True

    @cluster(num_nodes=3)
    def test_modify_then_query(self):
        """
        This test ensures that sending a command to enable an ACL then
        querying it (on a node that may be a follower) returns the most
        up to data result
        """
        # All operations apart from 'ALL'
        operations = [
            'DESCRIBE', 'READ', 'WRITE', 'CREATE', 'DELETE', 'ALTER',
            'DESCRIBE_CONFIGS', 'ALTER_CONFIGS'
        ]
        superclient = self._superclient()
        superclient.acl_create_allow_cluster(self.superuser.username,
                                             "describe")

        for op in operations:
            self._rpk.sasl_allow_principal(f'User:{self.username}', [op],
                                           'topic', 'foo',
                                           self.superuser.username,
                                           self.superuser.password,
                                           self.superuser.algorithm)
            described = superclient.acl_list()
            assert op in described, f"Failed looking for {op} in {described}"

    @cluster(num_nodes=1)
    def test_create_update(self):
        topic = "create-update"
        new_password = "new-pass"
        self._rpk.sasl_create_user(self.username, self.password,
                                   self.mechanism)
        # Only the super user can add ACLs
        self._rpk.sasl_allow_principal(f'User:{self.username}', ['all'],
                                       'topic', topic, self.superuser.username,
                                       self.superuser.password,
                                       self.superuser.algorithm)
        self._rpk.create_topic(topic)
        topic_list = self._rpk.list_topics()

        # We check that we can list the topics:
        assert topic in topic_list

        out = self._rpk.sasl_update_user(self.username, new_password,
                                         self.mechanism)
        assert f'Updated user "{self.username}" successfully' in out

        with expect_exception(
                RpkException, lambda e:
                'Invalid credentials: SASL_AUTHENTICATION_FAILED' in str(e)):
            # the rpk tool instance here will use old credentials so now it
            # should fail. We wait until it fails because the user update
            # it's not instant.
            wait_until(lambda: len(set(self._rpk.list_topics())) == 0,
                       timeout_sec=60,
                       backoff_sec=5)

        rpk_two = RpkTool(self.redpanda,
                          username=self.username,
                          password=new_password,
                          sasl_mechanism=self.mechanism)
        topic_list_two = rpk_two.list_topics()

        # We check that we can list the topics with the new password:
        assert topic in topic_list_two

    @cluster(num_nodes=1)
    def test_create_user_no_pass(self):
        # No password
        out = self._rpk.sasl_create_user(new_username="foo_1",
                                         mechanism=self.mechanism)
        assert "Automatically generated password" in out

        # with --new-password
        out = self._rpk.sasl_create_user(new_username="foo_2",
                                         new_password="any-pass",
                                         mechanism=self.mechanism)
        assert "Automatically generated password" not in out

        # with --user and --password, NO --new-password
        out = self._rpk.sasl_create_user_basic(new_username="foo_3",
                                               auth_user="anyUser",
                                               auth_password="any_pw",
                                               mechanism=self.mechanism)
        assert "Automatically generated password" in out

        # with --password AND --new-password, NO --user
        out = self._rpk.sasl_create_user_basic(new_username="foo_4",
                                               new_password="my_pass",
                                               auth_password="any_pw",
                                               mechanism=self.mechanism)
        assert "Automatically generated password" not in out

        # with --new-password, --user, and --password
        out = self._rpk.sasl_create_user_basic(new_username="foo_5",
                                               new_password="my_pass",
                                               auth_user="anyUser",
                                               auth_password="any_pw",
                                               mechanism=self.mechanism)
        assert "Automatically generated password" not in out

    @cluster(num_nodes=1)
    def test_back_compat_flags(self):
        """
        This test ensures that we can use old flags ("--password")
        mixed with new flags (-X pass, and --new-password)
        """
        user_1 = "foo_6"
        # This uses --user, --password, and --new-password
        out = self._rpk.sasl_create_user_basic(new_username=user_1,
                                               new_password="my_pass",
                                               auth_user=self.username,
                                               auth_password=self.password,
                                               mechanism=self.mechanism)
        assert f'Created user "{user_1}"' in out

        user_2 = "foo_7"
        # This uses -X user, --password, and -X pass
        out = self._rpk.sasl_create_user_basic_mix(new_username=user_2,
                                                   new_password="my_pass",
                                                   auth_user=self.username,
                                                   auth_password=self.password,
                                                   mechanism=self.mechanism)
        assert f'Created user "{user_2}"' in out

    @cluster(num_nodes=1)
    def test_create_delete_user_special_chars(self):
        """
        This test ensures that we can create and delete users that have 
        special characters (eg. +, /) in their names. It indirectly tests 
        that url encoding and decoding works across rpk and the redpanda
        admin API.
        """

        username = "a+complex/username?"

        superclient = self._superclient()
        superclient.sasl_create_user(username)

        wait_until(lambda: username in self._rpk.sasl_list_users(),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="fUser {username} has not been created in time")

        superclient.sasl_delete_user(username)

        wait_until(lambda: username not in self._rpk.sasl_list_users(),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="fUser {username} has not been deleted in time")

    @cluster(num_nodes=1)
    def test_role_acl(self):
        """
        This test ensures that we can create ACLs bound to a role principal
        and that this won't interfere with "User:" permissions, even with a
        matching principal name.
        """

        TOPIC_NAME = 'some-topic'
        ROLE_NAME = self.username
        superclient = self._superclient()

        superclient.sasl_create_user(self.username, self.password,
                                     self.mechanism)
        superclient.create_topic(TOPIC_NAME)

        with expect_exception(RpkException,
                              lambda e: 'AUTHORIZATION_FAILED' in str(e)):
            self._rpk.produce(TOPIC_NAME, 'foo', 'bar')

        self.logger.debug("Now add topic access rights for user 'alice'")

        self.logger.debug(
            "Wildcard is illegal for role name, so this should have no effect")
        superclient.sasl_deny_role('*', ['write'], 'topic', TOPIC_NAME)

        superclient.sasl_allow_role(ROLE_NAME, ['all'], 'topic', TOPIC_NAME)
        superclient.sasl_deny_role(f"RedpandaRole:{ROLE_NAME}", ['read'],
                                   'topic', TOPIC_NAME)

        def strip_acls(acls):
            return list(
                filter(lambda l: l != '' and 'PRINCIPAL' not in l,
                       acls.split('\n')))

        acls = strip_acls(superclient.acl_list())

        assert len(acls) == 2, f"Wrong number of ACLs: {acls}"
        for acl in acls:
            assert acl.find(f"RedpandaRole:{ROLE_NAME} "
                            ) == 0, f"Expected RedpandaRole ACL: {acls[-1]}"

        acls = strip_acls(
            superclient.acl_list(flags=['--allow-role', ROLE_NAME]))

        assert len(acls) == 1, f"Wrong number of ACLs: {acls}"
        for acl in acls:
            assert acl.find(f"RedpandaRole:{ROLE_NAME} "
                            ) == 0, f"Expected RedpandaRole ACL: {acls[-1]}"

        acls = strip_acls(
            superclient.acl_list(flags=['--deny-role', ROLE_NAME]))

        assert len(acls) == 1, f"Wrong number of ACLs: {acls}"
        for acl in acls:
            assert acl.find(f"RedpandaRole:{ROLE_NAME}"
                            ) == 0, f"Expected RedpandaRole ACL: {acls[-1]}"

        # The user is still not authorized
        with expect_exception(RpkException,
                              lambda e: 'AUTHORIZATION_FAILED' in str(e)):
            self._rpk.produce(TOPIC_NAME, 'foo', 'bar')
