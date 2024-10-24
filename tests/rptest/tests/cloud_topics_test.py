# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import SISettings, get_cloud_storage_type
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool


class CloudTopicsTest(RedpandaTest):
    def __init__(self, test_context):
        si_settings = SISettings(test_context=test_context)
        super(CloudTopicsTest, self).__init__(test_context=test_context,
                                              si_settings=si_settings)
        self.s3_bucket_name = si_settings.cloud_storage_bucket

    def __create_initial_topics(self):
        """
        Create initial initial test topics with cloud topic enabled. This needs
        to be done after development feature support has been enabled, and nodes
        have been restarted so that development services start at bootup.
        """
        self.redpanda.enable_development_feature_support()
        self.redpanda.set_cluster_config(
            values={
                "development_enable_cloud_topics": True,
            })
        self.redpanda.restart_nodes(self.redpanda.nodes)
        rpk = RpkTool(self.redpanda)
        for spec in self.topics:
            rpk.create_topic(spec.name,
                             spec.partition_count,
                             spec.replication_factor,
                             config={"redpanda.cloud_topic.enabled": "true"})

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_reconciler_uploads(self, cloud_storage_type):
        def count_l1_objects():
            objects = self.redpanda.get_objects_from_si()
            return sum(1 for o in objects if "l1_" in o.key)

        def produce_and_count():
            kafka_tools = KafkaCliTools(self.redpanda)
            kafka_tools.produce(self.topic, 100, 1, batch_size=10)
            return count_l1_objects()

        self.topics = (TopicSpec(partition_count=5), )
        self.__create_initial_topics()

        wait_until(lambda: produce_and_count() >= 5,
                   backoff_sec=12,
                   timeout_sec=60)
