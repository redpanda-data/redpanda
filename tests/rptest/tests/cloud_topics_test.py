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
from rptest.services.redpanda import SISettings, CloudStorageType
from rptest.clients.kafka_cli_tools import KafkaCliTools


class CloudTopicsTest(RedpandaTest):
    topics = (TopicSpec(name="cloud_topics_test_ct", partition_count=5), )

    def __init__(self, test_context):
        super(CloudTopicsTest,
              self).__init__(test_context=test_context,
                             si_settings=SISettings(test_context=test_context))

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=[CloudStorageType.S3])
    def test_reconciler_uploads(self, cloud_storage_type):
        def count_l1_objects():
            objects = self.redpanda.get_objects_from_si()
            return sum(1 for o in objects if "l1_" in o.key)

        def produce_and_count():
            kafka_tools = KafkaCliTools(self.redpanda)
            kafka_tools.produce(self.topic, 100, 1, batch_size=10)
            return count_l1_objects()

        # enable cloud topics feature
        self.redpanda.enable_development_feature_support()
        self.redpanda.set_cluster_config(
            values={
                "development_enable_cloud_topics": True,
            })
        self.redpanda.restart_nodes(self.redpanda.nodes)

        wait_until(lambda: produce_and_count() >= 5,
                   backoff_sec=12,
                   timeout_sec=60)
