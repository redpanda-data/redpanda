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


class KafkaCliToolsTest(RedpandaTest):
    """
    Verify that kafka cli tools works for all supported versions.
    """
    @matrix(version=KafkaCliTools.VERSIONS)
    def test_create_topic(self, version):
        tools = KafkaCliTools(self.redpanda, version)
        topics = ["v{}.{}".format(version, i) for i in range(3)]
        for topic in topics:
            tools.create_topic(TopicSpec(name=topic))
        assert set(topics) <= set(tools.list_topics())
