# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from confluent_kafka.admin import AdminClient, NewTopic
from typing import Optional
from rptest.services import tls


class PythonLibrdkafka:
    """
    https://github.com/confluentinc/confluent-kafka-python
    """
    def __init__(self,
                 redpanda,
                 *,
                 username=None,
                 password=None,
                 algorithm=None,
                 tls_cert: Optional[tls.Certificate] = None):
        self._redpanda = redpanda
        self._username = username
        self._password = password
        self._algorithm = algorithm
        self._tls_cert = tls_cert

    def brokers(self):
        client = AdminClient(self._get_config())
        return client.list_topics(timeout=10).brokers

    def topics(self, topic=None):
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

    def _get_config(self):
        conf = {
            'bootstrap.servers': self._redpanda.brokers(),
        }
        if self._redpanda.sasl_enabled():
            if self._username:
                c = (self._username, self._password, self._algorithm)
            else:
                c = self._redpanda.SUPERUSER_CREDENTIALS
            conf.update({
                'sasl.mechanism': c[2],
                'security.protocol': 'sasl_plaintext',
                'sasl.username': c[0],
                'sasl.password': c[1],
            })
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
        self._redpanda.logger.info(conf)
        return conf
