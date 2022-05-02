# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from collections import defaultdict
from time import sleep
from rptest.clients.kafka_cli_tools import KafkaCliTools

from rptest.services.cluster import cluster
from ducktape.mark import parametrize

from rptest.clients.kcl import KCL
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from rptest.tests.redpanda_test import RedpandaTest


class FetchTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        super(FetchTest, self).__init__(test_ctx,
                                        num_brokers=3,
                                        *args,
                                        extra_rp_conf={},
                                        **kwargs)

    @cluster(num_nodes=4)
    def fetch_long_poll_test(self):
        """
        Test if redpanda is able to debounce fetches when consumer requests
        to wait for data
        """
        partition_count = 20
        total_messages = 10
        topic = TopicSpec(partition_count=partition_count,
                          replication_factor=3)

        # create topic
        self.client().create_topic(specs=topic)
        rpk = RpkTool(self.redpanda)

        consumer = KafkaCliConsumer(self.test_context,
                                    self.redpanda,
                                    topic=topic.name,
                                    group='test-gr-1',
                                    from_beginning=True,
                                    consumer_properties={
                                        'fetch.min.bytes': 1024,
                                        'fetch.max.wait.ms': 1000,
                                    })
        consumer.start()
        for p in range(0, total_messages + 1):
            rpk.produce(topic.name,
                        f"k-{p}",
                        f"v-test-{p}",
                        partition=p % partition_count)
            # sleep for 2 seconds every each message
            # to prevent fetch handler from returning fast
            sleep(2)

        consumer.wait_for_messages(10)
        consumer.stop()
        consumer.wait()
