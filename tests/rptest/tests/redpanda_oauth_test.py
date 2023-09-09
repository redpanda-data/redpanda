# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import functools
import json

from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.services.redpanda import SecurityConfig, make_redpanda_service
from rptest.services.keycloak import KeycloakService
from rptest.services.cluster import cluster

from ducktape.tests.test import Test
from rptest.services.redpanda import make_redpanda_service
from rptest.clients.rpk import RpkTool
from rptest.util import expect_exception

from confluent_kafka import KafkaException

CLIENT_ID = 'myapp'
EXAMPLE_TOPIC = 'foo'


class RedpandaOIDCTestBase(Test):
    """
    Base class for tests that use the Redpanda service with OIDC
    """
    def __init__(self,
                 test_context,
                 num_nodes=5,
                 sasl_mechanisms=['SCRAM', 'OAUTHBEARER'],
                 **kwargs):
        super(RedpandaOIDCTestBase, self).__init__(test_context, **kwargs)
        self.produce_messages = []
        self.produce_errors = []
        num_brokers = num_nodes - 1
        self.keycloak = KeycloakService(test_context)

        security = SecurityConfig()
        security.enable_sasl = True
        security.sasl_mechanisms = sasl_mechanisms

        self.redpanda = make_redpanda_service(test_context, num_brokers)
        self.redpanda.set_security_settings(security)

        self.su_username, self.su_password, self.su_algorithm = self.redpanda.SUPERUSER_CREDENTIALS

        self.rpk = RpkTool(self.redpanda,
                           username=self.su_username,
                           password=self.su_password,
                           sasl_mechanism=self.su_algorithm)

    def setUp(self):
        self.produce_messages.clear()
        self.produce_errors.clear()
        self.redpanda.logger.info("Starting Redpanda")
        self.redpanda.start()


class RedpandaOIDCTest(RedpandaOIDCTestBase):
    @cluster(num_nodes=5)
    def test_init(self):
        kc_node = self.keycloak.nodes[0]
        try:
            self.keycloak.start_node(kc_node)
        except Exception as e:
            self.logger.error(f"{e}")
            self.keycloak.clean_node(kc_node)
            assert False, "Keycloak failed to start"

        self.keycloak.admin.create_user('norma',
                                        'desmond',
                                        realm_admin=True,
                                        email='10086@sunset.blvd')
        self.keycloak.login_admin_user(kc_node, 'norma', 'desmond')
        self.keycloak.admin.create_client(CLIENT_ID)

        # add an email address to myapp client's service user. this should
        # appear alongside the access token.
        self.keycloak.admin.update_user(f'service-account-{CLIENT_ID}',
                                        email='myapp@customer.com')

        self.rpk.create_topic(EXAMPLE_TOPIC)
        service_user_id = self.keycloak.admin_ll.get_user_id(
            f'service-account-{CLIENT_ID}')
        result = self.rpk.sasl_allow_principal(f'User:{service_user_id}',
                                               ['all'], 'topic', EXAMPLE_TOPIC,
                                               self.su_username,
                                               self.su_password,
                                               self.su_algorithm)

        cfg = self.keycloak.generate_oauth_config(kc_node, CLIENT_ID)
        assert cfg.client_secret is not None
        assert cfg.token_endpoint is not None
        k_client = PythonLibrdkafka(self.redpanda,
                                    algorithm='OAUTHBEARER',
                                    oauth_config=cfg)
        producer = k_client.get_producer()

        # Expclicit poll triggers OIDC token flow. Required for librdkafka
        # metadata requests to behave nicely.
        producer.poll(0.0)

        with expect_exception(KafkaException, lambda _: True):
            producer.list_topics(timeout=5)
