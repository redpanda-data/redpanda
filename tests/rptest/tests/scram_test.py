# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
from ducktape.errors import DucktapeError

from ducktape.tests.test import Test
from rptest.services.redpanda import NoSaslRedpandaService
from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools


# test that starts a redpanda service without using clients that would otherwise
# require sasl auth support
class NoSaslRedpandaTest(Test):
    def __init__(self,
                 test_context,
                 num_brokers=3,
                 extra_rp_conf=dict(),
                 log_level='info'):
        super(NoSaslRedpandaTest, self).__init__(test_context)

        self.redpanda = NoSaslRedpandaService(test_context,
                                              num_brokers,
                                              KafkaCliTools,
                                              extra_rp_conf=extra_rp_conf,
                                              log_level=log_level)

    def setUp(self):
        self.redpanda.start()


class ScramTest(NoSaslRedpandaTest):
    def __init__(self, test_context):
        extra_rp_conf = dict(
            developer_mode=True,
            enable_sasl=True,
            static_scram_user="redpanda_user",
            static_scram_pass="redpanda_pass",
        )
        super(ScramTest, self).__init__(test_context=test_context,
                                        extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_scram(self):
        topic = TopicSpec()
        client = KafkaCliTools(self.redpanda,
                               user="redpanda_user",
                               passwd="redpanda_pass")
        client.create_topic(topic)

        client = KafkaCliTools(self.redpanda,
                               user="redpanda_user",
                               passwd="bad_password")
        try:
            client.list_topics()
            assert False, "Listing topics should fail"
        except Exception:
            pass

        client = KafkaCliTools(self.redpanda,
                               user="redpanda_user",
                               passwd="redpanda_pass")
        topics = client.list_topics()
        print(topics)
        assert topic.name in topics
