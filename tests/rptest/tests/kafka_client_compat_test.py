# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools


class KafkaClientCompatTest(RedpandaTest):
    def test_create_topic(self):
        for client_factory in KafkaCliTools.instances():
            client = client_factory(self.redpanda)
            topics = [TopicSpec() for _ in range(3)]
            for topic in topics:
                client.create_topic(topic)
            for topic in topics:
                spec = client.describe_topic(topic.name)
                assert spec == topic
