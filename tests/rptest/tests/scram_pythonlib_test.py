# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import SecurityConfig
from kafka import KafkaAdminClient
from kafka.admin import NewTopic


class ScramPythonLibTest(RedpandaTest):
    def __init__(self, test_context):
        security = SecurityConfig()
        security.enable_sasl = True
        super(ScramPythonLibTest,
              self).__init__(test_context,
                             num_brokers=3,
                             security=security,
                             extra_node_conf={'developer_mode': True})

    def make_superuser_client(self, password_override=None):
        username, password, algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        password = password_override or password
        return KafkaAdminClient(bootstrap_servers=self.redpanda.brokers_list(),
                                security_protocol='SASL_PLAINTEXT',
                                sasl_mechanism=algorithm,
                                sasl_plain_username=username,
                                sasl_plain_password=password,
                                request_timeout_ms=30000,
                                api_version_auto_timeout_ms=3000)

    @cluster(num_nodes=3)
    def test_scram_pythonlib(self):
        topic = TopicSpec()
        client = self.make_superuser_client()
        client.create_topics([
            NewTopic(name=topic.name, num_partitions=1, replication_factor=1)
        ])
        topics = set(client.list_topics())
        self.redpanda.logger.info(f"Topics {topics}")
        assert set([topic.name]) == topics
