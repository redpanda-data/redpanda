# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from rptest.services.action_injector import random_process_kills
from rptest.services.cluster import cluster
from rptest.services.redpanda import RedpandaService, CHAOS_LOG_ALLOW_LIST
from rptest.services.redpanda import SISettings
from rptest.tests.end_to_end import EndToEndTest
from rptest.util import Scale
from rptest.util import (
    produce_until_segments,
    wait_for_segments_removal,
)


class EndToEndShadowIndexingBase(EndToEndTest):
    segment_size = 1048576  # 1 Mb
    s3_topic_name = "panda-topic"

    num_brokers = 3
    override_default_topic_replication = None

    topics = (TopicSpec(
        name=s3_topic_name,
        partition_count=1,
        replication_factor=3,
    ), )

    def __init__(self, test_context):
        super(EndToEndShadowIndexingBase,
              self).__init__(test_context=test_context)

        self.topic = EndToEndShadowIndexingBase.s3_topic_name

        si_settings = SISettings(
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            log_segment_size=EndToEndShadowIndexingBase.segment_size,  # 1MB
        )
        self.s3_bucket_name = si_settings.cloud_storage_bucket

        si_settings.load_context(self.logger, test_context)

        if self.override_default_topic_replication:
            self._extra_rp_conf[
                'default_topic_replications'] = self.override_default_topic_replication

        self.scale = Scale(test_context)
        self.redpanda = RedpandaService(
            context=test_context,
            num_brokers=self.num_brokers,
            si_settings=si_settings,
        )

        self.kafka_tools = KafkaCliTools(self.redpanda)

    def setUp(self):
        self.redpanda.start()
        for topic in EndToEndShadowIndexingBase.topics:
            self.kafka_tools.create_topic(topic)

    def tearDown(self):
        self.s3_client.empty_bucket(self.s3_bucket_name)


class EndToEndShadowIndexingTest(EndToEndShadowIndexingBase):
    @cluster(num_nodes=5)
    def test_write(self):
        """Write at least 10 segments, set retention policy to leave only 5
        segments, wait for segments removal, consume data and run validation,
        that everything that is acked is consumed."""
        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )

        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_BYTES:
                    5 * EndToEndShadowIndexingTest.segment_size,
            },
        )
        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.topic,
                                  partition_idx=0,
                                  count=6)
        self.start_consumer()
        self.run_validation()


class EndToEndShadowIndexingTestWithDisruptions(EndToEndShadowIndexingBase):
    override_default_topic_replication = EndToEndShadowIndexingBase.num_brokers

    @cluster(num_nodes=5,
             log_allow_list=CHAOS_LOG_ALLOW_LIST,
             allow_missing_process=True)
    def test_write_with_node_failures(self):
        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )

        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_BYTES:
                    5 * EndToEndShadowIndexingTest.segment_size
            },
        )

        with random_process_kills(self.redpanda) as ctx:
            wait_for_segments_removal(redpanda=self.redpanda,
                                      topic=self.topic,
                                      partition_idx=0,
                                      count=6)
            self.start_consumer()
            self.run_validation()

        action_log = ctx.action_log()
        assert action_log

        # At least one process kill action is present
        assert action_log[0].is_reverse_action is False

        # If there was another action after process kill, it was revival,
        # because we are running in reverse after action mode
        if len(action_log) > 1:
            assert action_log[1].is_reverse_action and action_log[
                1].node == action_log[0].node
