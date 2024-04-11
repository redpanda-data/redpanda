# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import requests
import time
import functools

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from typing import Any, Optional, cast
from rptest.services import tls
from rptest.services.keycloak import OAuthConfig
from rptest.services.redpanda_types import KafkaClientSecurity


class PythonLibrdkafka:
    """
    https://github.com/confluentinc/confluent-kafka-python
    """
    def __init__(self,
                 redpanda,
                 *,
                 username: str | None = None,
                 password: str | None = None,
                 algorithm: str | None = None,
                 tls_cert: Optional[tls.Certificate] = None,
                 oauth_config: Optional[OAuthConfig] = None):
        self._redpanda = redpanda
        self._security = cast(KafkaClientSecurity,
                              redpanda.kafka_client_security()).override(
                                  username, password, algorithm, None)
        self._tls_cert = tls_cert
        self._oauth_config = oauth_config
        self._oauth_count = 0

    @property
    def oauth_count(self):
        return self._oauth_count

    def brokers(self) -> dict[str, Any]:
        client = AdminClient(self._get_config())
        return client.list_topics(timeout=10).brokers

    def topics(self, topic: str | None = None):
        client = AdminClient(self._get_config())
        return client.list_topics(timeout=10, topic=topic).topics

    def create_topic(self, spec):
        topics = [
            NewTopic(spec.name,
                     num_partitions=spec.partition_count,
                     replication_factor=spec.replication_factor)
        ]
        client = AdminClient(self._get_config())
        res = client.create_topics(topics, request_timeout=10)
        for topic, fut in res.items():
            try:
                fut.result()
                self._redpanda.logger.debug(f"topic {topic} created")
            except Exception as e:
                self._redpanda.logger.debug(
                    f"topic {topic} creation failed: {e}")
                raise

    def get_client(self):
        return AdminClient(self._get_config())

    def get_producer(self):
        producer_conf = self._get_config()
        self._redpanda.logger.debug(f"{producer_conf}")
        return Producer(producer_conf)

    def get_consumer(self, extra_config: dict[str, Any] = {}):
        conf = self._get_config()
        conf.update(extra_config)
        self._redpanda.logger.debug(f"{conf}")
        return Consumer(conf)

    def _get_config(self):
        conf = {
            'bootstrap.servers': self._redpanda.brokers(),
        }

        if self._security.mechanism == 'OAUTHBEARER':
            conf.update(self._get_oauth_config())
        else:
            conf.update(self._get_sasl_config())

        if self._tls_cert:
            conf.update({
                'ssl.key.location': self._tls_cert.key,
                'ssl.certificate.location': self._tls_cert.crt,
                'ssl.ca.location': self._tls_cert.ca.crt,
            })
            if self._redpanda.sasl_enabled():
                conf.update({
                    'security.protocol': 'sasl_ssl',
                })
            else:
                conf.update({
                    'security.protocol': 'ssl',
                })
        # log as debug to eliminate log spamming
        # when creating a lot of producers
        self._redpanda.logger.debug(conf)
        return conf

    def _get_oauth_config(self):
        assert self._oauth_config is not None
        return {
            'security.protocol':
            'sasl_plaintext',
            'sasl.mechanisms':
            "OAUTHBEARER",
            'oauth_cb':
            functools.partial(self._get_oauth_token, self._oauth_config),
            'logger':
            self._redpanda.logger,
        }

    def _get_sasl_config(self) -> dict[str, str]:
        c = self._security.simple_credentials()
        return {
            'security.protocol': self._security.security_protocol.name.lower(),
            'sasl.username': c.username,
            'sasl.password': c.password,
            'sasl.mechanism': c.mechanism
        } if c else {}

    def _get_oauth_token(self, conf: OAuthConfig, _):
        # Better to wrap this whole thing in a try block, since the context where
        # librdkafka invokes the callback seems to prevent exceptions from making
        # their way back up to ducktape. This way we get a log and librdkafka will
        # barf when we return the wrong thing.
        try:
            payload = {
                'client_id': conf.client_id,
                'client_secret': conf.client_secret,
                'audience': 'redpanda',
                'grant_type': 'client_credentials',
                'scope': ' '.join(conf.scopes),
            }
            self._redpanda.logger.info(
                f"GETTING TOKEN: {conf.token_endpoint}, payload: {payload}")

            resp = requests.post(
                conf.token_endpoint,
                headers={'content-type': 'application/x-www-form-urlencoded'},
                auth=(conf.client_id, conf.client_secret),
                data=payload,
                verify=conf.ca_cert if conf.ca_cert is not None else True)
            self._redpanda.logger.info(
                f"response status: {resp.status_code}, body: {resp.content}")
            token = resp.json()
            self._oauth_count += 1
            return token['access_token'], time.time() + float(
                token['expires_in'])
        except Exception as e:
            self._redpanda.logger.error(f"Exception: {e}")
