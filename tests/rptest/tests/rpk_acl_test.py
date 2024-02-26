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
