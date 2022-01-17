# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools
from kafka import KafkaAdminClient


class DefaultClient:
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def create_topic(self, specs):
        if isinstance(specs, TopicSpec):
            specs = [specs]
        client = KafkaCliTools(self._redpanda)
        for spec in specs:
            client.create_topic(spec)

    def delete_topic(self, name):
        client = KafkaCliTools(self._redpanda)
        client.delete_topic(name)

    def describe_topics(self, topics=None):
        """
        Describe topics. Pass topics=None to describe all topics, or a pass a
        list of topic names to restrict the call to a set of specific topics.

        Sample return value:
            [
              {'error_code': 0,
               'topic': 'topic-kabn',
               'is_internal': False,
               'partitions': [
                 {'error_code': 0,
                  'partition': 0,
                  'leader': 1,
                  'replicas': [1],
                  'isr': [1],
                  'offline_replicas': []}
               }
            ]
        """
        client = KafkaAdminClient(
            bootstrap_servers=self._redpanda.brokers_list(),
            **self._redpanda.security_config())
        return client.describe_topics(topics)
