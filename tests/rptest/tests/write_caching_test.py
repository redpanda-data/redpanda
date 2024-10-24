# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.rpk_producer import RpkProducer
from enum import Enum
from random import randint
from rptest.services.metrics_check import MetricCheck
from rptest.util import expect_exception


# no StrEnum support in test python version
class WriteCachingMode(str, Enum):
    TRUE = "true"
    FALSE = "false"
    DISABLED = "disabled"

    def __bool__(self):
        return self.value == self.TRUE

    def __str__(self):
        return self.value


class WriteCachingPropertiesTest(RedpandaTest):

    WRITE_CACHING_DEBUG_KEY = "write_caching_enabled"
    FLUSH_MS_DEBUG_KEY = "flush_ms"
    FLUSH_BYTES_DEBUG_KEY = "flush_bytes"

    def __init__(self, test_context):
        super(WriteCachingPropertiesTest,
              self).__init__(test_context=test_context, num_brokers=3)

        self.rpk = RpkTool(self.redpanda)

    def configs_converged(self, write_caching: WriteCachingMode, flush_ms: int,
                          flush_bytes: int):
        """Checks that the desired write caching configuration is set in topic & partition (raft) states"""
        assert self.rpk
        assert self.admin

        configs = self.rpk.describe_topic_configs(self.topic_name)
        assert TopicSpec.PROPERTY_WRITE_CACHING in configs.keys(), configs
        assert TopicSpec.PROPERTY_FLUSH_MS in configs.keys(), configs
        assert TopicSpec.PROPERTY_FLUSH_BYTES in configs.keys(), configs

        properties_check = configs[TopicSpec.PROPERTY_WRITE_CACHING][0] == str(
            write_caching) and configs[TopicSpec.PROPERTY_FLUSH_MS][0] == str(
                flush_ms) and configs[
                    TopicSpec.PROPERTY_FLUSH_BYTES][0] == str(flush_bytes)

        if not properties_check:
            return False

        partition_state = self.admin.get_partition_state(
            "kafka", self.topic_name, 0)
        replicas = partition_state["replicas"]
        assert len(replicas) == 3

        def validate_flush_ms(val: int) -> bool:
            # account for jitter
            return flush_ms - 5 <= val <= flush_ms + 5

        for replica in replicas:
            raft_state = replica["raft_state"]
            assert self.WRITE_CACHING_DEBUG_KEY in raft_state.keys(
            ), raft_state
            assert self.FLUSH_MS_DEBUG_KEY in raft_state.keys(), raft_state
            assert self.FLUSH_BYTES_DEBUG_KEY in raft_state.keys(), raft_state

            replica_check = raft_state[self.WRITE_CACHING_DEBUG_KEY] == bool(
                write_caching) and validate_flush_ms(
                    raft_state[self.FLUSH_MS_DEBUG_KEY]) and raft_state[
                        self.FLUSH_BYTES_DEBUG_KEY] == flush_bytes

            if not replica_check:
                return False

        return True

    def validate_topic_configs(self, write_caching: WriteCachingMode,
                               flush_ms: int, flush_bytes: int):
        self.redpanda.wait_until(
            lambda: self.configs_converged(write_caching, flush_ms, flush_bytes
                                           ),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Write caching configuration failed to converge")

    def set_cluster_config(self, key: str, value):
        self.rpk.cluster_config_set(key, value)

    def set_topic_properties(self, key: str, value):
        self.rpk.alter_topic_config(self.topic_name, key, value)

    @cluster(num_nodes=3)
    def test_properties(self):
        write_caching_conf = "write_caching_default"
        flush_ms_conf = "raft_replica_max_flush_delay_ms"
        flush_bytes_conf = "raft_replica_max_pending_flush_bytes"

        self.admin = self.redpanda._admin

        # Topic with custom properties at creation.
        topic = TopicSpec()
        self.topic_name = topic.name
        self.rpk.create_topic(topic=topic.name,
                              partitions=1,
                              replicas=3,
                              config={
                                  TopicSpec.PROPERTY_WRITE_CACHING: "true",
                                  TopicSpec.PROPERTY_FLUSH_MS: 123,
                                  TopicSpec.PROPERTY_FLUSH_BYTES: 9999
                              })

        self.validate_topic_configs(WriteCachingMode.TRUE, 123, 9999)

        # New topic with defaults
        topic = TopicSpec()
        self.topic_name = topic.name
        self.rpk.create_topic(topic=topic.name, partitions=1, replicas=3)

        # Validate cluster defaults
        self.validate_topic_configs(WriteCachingMode.FALSE, 100, 262144)

        # Changing cluster level configs
        self.set_cluster_config(write_caching_conf, WriteCachingMode.TRUE)
        self.validate_topic_configs(WriteCachingMode.TRUE, 100, 262144)
        self.set_cluster_config(flush_ms_conf, 200)
        self.validate_topic_configs(WriteCachingMode.TRUE, 200, 262144)
        self.set_cluster_config(flush_bytes_conf, 32768)
        self.validate_topic_configs(WriteCachingMode.TRUE, 200, 32768)

        # Turn off write caching at topic level
        self.set_topic_properties(TopicSpec.PROPERTY_WRITE_CACHING,
                                  WriteCachingMode.FALSE)
        self.validate_topic_configs(WriteCachingMode.FALSE, 200, 32768)

        # Turn off write caching at cluster level but enable at topic level
        # topic properties take precedence
        self.set_cluster_config(write_caching_conf, WriteCachingMode.FALSE)
        self.set_topic_properties(TopicSpec.PROPERTY_WRITE_CACHING,
                                  WriteCachingMode.TRUE)
        self.validate_topic_configs(WriteCachingMode.TRUE, 200, 32768)

        # Kill switch test, disable write caching feature globally,
        # should override topic level property
        self.set_cluster_config(write_caching_conf, WriteCachingMode.DISABLED)
        self.validate_topic_configs(WriteCachingMode.DISABLED, 200, 32768)

        # Try to update the topic property now, should throw an error
        try:
            self.set_topic_properties(TopicSpec.PROPERTY_WRITE_CACHING,
                                      WriteCachingMode.DISABLED)
            assert False, "No exception thrown when updating topic propertes in disabled mode."
        except RpkException as e:
            assert "INVALID_CONFIG" in str(e)

        # Enable again
        self.set_cluster_config(write_caching_conf, WriteCachingMode.TRUE)
        self.set_topic_properties(TopicSpec.PROPERTY_WRITE_CACHING,
                                  WriteCachingMode.TRUE)
        self.validate_topic_configs(WriteCachingMode.TRUE, 200, 32768)

    @cluster(num_nodes=3)
    def test_bad_properties(self):
        topic = TopicSpec()

        with expect_exception(
                RpkException,
                lambda e: "Unsupported write caching configuration." in e.msg
                and "INVALID_CONFIG" in e.msg):
            self.rpk.create_topic(topic=topic.name,
                                  partitions=topic.partition_count,
                                  replicas=topic.replication_factor,
                                  config={
                                      TopicSpec.PROPERTY_WRITE_CACHING: "asd",
                                  })


class WriteCachingMetricsTest(RedpandaTest):
    def __init__(self, test_context):
        super(WriteCachingMetricsTest,
              self).__init__(test_context=test_context, num_brokers=1)
        self._ctx = test_context
        self.topic_name = "test"
        self.topics = [TopicSpec(name=self.topic_name, replication_factor=1)]

    @cluster(num_nodes=2)
    def test_request_metrics(self):
        def produce_events(num: int):
            producer = RpkProducer(self._ctx,
                                   self.redpanda,
                                   self.topic,
                                   16384,
                                   num,
                                   acks=-1)
            producer.start()
            producer.stop()
            producer.free()

        self.rpk = RpkTool(self.redpanda)

        metric_flush = 'vectorized_raft_replicate_ack_all_requests_total'
        metric_no_flush = 'vectorized_raft_replicate_ack_all_requests_no_flush_total'

        checker = MetricCheck(self.redpanda.logger,
                              self.redpanda,
                              self.redpanda.partitions(self.topic)[0].leader,
                              [metric_flush, metric_no_flush],
                              labels={
                                  'namespace': 'kafka',
                                  'topic': self.topic,
                                  'partition': '0',
                              },
                              reduce=sum)

        def validators(value_flush: int, value_no_flush: int):
            nonlocal metric_flush
            nonlocal metric_no_flush
            return [(metric_flush, lambda _, metric: metric == value_flush),
                    (metric_no_flush,
                     lambda _, metric: metric == value_no_flush)]

        # produce some events without write caching
        num_events_with_flush = randint(99, 299)
        produce_events(num_events_with_flush)
        checker.evaluate(validators(num_events_with_flush, 0))

        # Enable write caching
        self.rpk.cluster_config_set("write_caching_default", "true")

        num_events_without_flush = randint(199, 299)
        checker.evaluate(
            validators(num_events_with_flush, num_events_without_flush))
