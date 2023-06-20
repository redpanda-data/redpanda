# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.redpanda import SecurityConfig
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
        super(RpkACLTest, self).__init__(test_ctx,
                                         security=security,
                                         *args,
                                         **kwargs)
        self._rpk = RpkTool(redpanda=self.redpanda,
                            username=self.username,
                            password=self.password,
                            sasl_mechanism=self.mechanism)
        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS

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

        out = self._rpk.sasl_update_user(self.username, new_password)
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
