# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random

from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkException, RpkTool
from rptest.services import redpanda
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
import string


def _timed_out(e: RpkException):
    return "timed out" in e.msg


def _message_to_large(e: RpkException):
    return "MESSAGE_TOO_LARGE" in e.stderr


class LimitsTest(RedpandaTest):
    def _rand_string(sz):
        return ''.join(
            random.choice(string.ascii_letters + string.digits)
            for _ in range(sz))

    def assert_produce_result(self, topic, size, failure_predicate=None):
        rpk = RpkTool(self.redpanda)

        def expected_result():
            try:
                rpk.produce(topic=topic,
                            key=LimitsTest._rand_string(64),
                            msg=LimitsTest._rand_string(size))
            except RpkException as e:
                return failure_predicate and failure_predicate(e)

            return failure_predicate is None

        wait_until(expected_result, 30, 1)

    @cluster(num_nodes=3)
    def test_kafka_request_max_size(self):
        topic = TopicSpec(partition_count=1)
        # create topic
        DefaultClient(self.redpanda).create_topic(topic)
        # produce 512KB message
        self.assert_produce_result(topic.name, 512 * 1024)
        # set request max bytes to 50KB
        new_limit = 50 * 1024
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("kafka_request_max_bytes", str(new_limit))

        self.assert_produce_result(topic.name,
                                   512 * 1024,
                                   failure_predicate=_timed_out)

    @cluster(num_nodes=3)
    def test_batch_max_size(self):
        # create topic
        topics = [TopicSpec(name=f"limits-test-{n}") for n in range(3)]
        DefaultClient(self.redpanda).create_topic(topics)
        # produce 512KB message to each topic
        for t in topics:
            self.assert_produce_result(t.name, 128 * 1024)
        # change the default value for batch max bytes
        rpk = RpkTool(self.redpanda)
        new_limit = 50 * 1024
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set('kafka_batch_max_bytes', str(50 * 1024))

        # validate if all topics have a default value set
        for t in topics:
            cfg = rpk.describe_topic_configs(t.name)
            v, src = cfg['max.message.bytes']
            assert v == str(new_limit)
            assert src == "DEFAULT_CONFIG"

        # it should not be possible to produce to any topic as the message is to large
        for t in topics:
            self.assert_produce_result(t.name,
                                       128 * 1024,
                                       failure_predicate=_message_to_large)

        # change setting for single topic, we should be able to produce to that topic
        changed = topics[0]
        tp_limit = 1024 * 1024
        rpk.alter_topic_config(changed.name, "max.message.bytes", tp_limit)

        # validate topic configuration
        cfg = rpk.describe_topic_configs(changed.name)
        v, src = cfg['max.message.bytes']
        assert v == str(tp_limit)
        assert src == "DYNAMIC_TOPIC_CONFIG"
        # produce
        self.assert_produce_result(changed.name, 128 * 1024)
