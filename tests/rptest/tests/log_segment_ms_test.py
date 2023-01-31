from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.services.verifiable_producer import VerifiableProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import segments_count


def is_approximately_equal(lhs, rhs, margin):
    return abs(lhs - rhs) <= margin


# NOTE this value could be read from redpanda
# for example this should be a reasonable way
# int(Admin(self.redpanda).get_cluster_config_schema()
#                       ['properties']['log_segment_ms_min']['example'])
SERVER_SEGMENT_MS = 60000
SERVER_ROLLING_SECONDS = SERVER_SEGMENT_MS / 1000
SERVER_HOUSEKEEPING_LOOP = 10


def messages_to_generate_segments(num_segments, period):
    return int(period / 1000 * (num_segments + 1) + SERVER_HOUSEKEEPING_LOOP +
               1)


class SegmentMsTest(RedpandaTest):
    TEST_NUM_SEGMENTS = 10

    def __init__(self, test_context):
        super(SegmentMsTest, self).__init__(test_context=test_context,
                                            num_brokers=1)
        self.kafka_tools = KafkaCliTools(self.redpanda)

    def _total_segments_count(self, topic_spec: TopicSpec):
        assert topic_spec.partition_count == 1, "only supports one partition"
        return next(segments_count(self.redpanda, topic_spec.name, 0))

    def generate_workload(self, server_cfg, topic_cfg, use_alter_cfg: bool,
                          rolling_period, segments_to_produce):
        self.redpanda.set_cluster_config({'log_segment_ms': server_cfg},
                                         expect_restart=False)

        topic = TopicSpec(
            partition_count=1,
            replication_factor=1) if not use_alter_cfg else TopicSpec(
                partition_count=1, replication_factor=1, segment_ms=topic_cfg)
        self.client().create_topic(topic)

        start_count = self._total_segments_count(topic)

        if use_alter_cfg:
            self.client().alter_topic_config(topic.name,
                                             TopicSpec.PROPERTY_SEGMENT_MS,
                                             topic_cfg)

        to_produce = messages_to_generate_segments(segments_to_produce,
                                                   rolling_period)

        producer = VerifiableProducer(context=self.test_context,
                                      num_nodes=1,
                                      redpanda=self.redpanda,
                                      topic=topic.name,
                                      max_messages=to_produce,
                                      throughput=1)
        consumer = VerifiableConsumer(context=self.test_context,
                                      num_nodes=1,
                                      redpanda=self.redpanda,
                                      topic=topic.name,
                                      group_id=0)
        consumer.start()
        producer.start()
        wait_until(lambda: producer.num_acked >= to_produce,
                   timeout_sec=to_produce + 30,
                   err_msg=f"producer failed to produce {to_produce=} msgs")
        wait_until(lambda: consumer.total_consumed() >= to_produce,
                   timeout_sec=30,
                   err_msg=f"consumer failed to consume {to_produce=} msgs")
        consumer.stop()
        producer.stop()
        assert producer.num_acked == consumer.total_consumed(), \
            f"failed to preserve the messages across segment rolling"
        stop_count = self._total_segments_count(topic)

        assert stop_count >= start_count

        return start_count, stop_count

    @cluster(num_notes=1)
    @matrix(server_topic_cfg=[(None, None), (SERVER_SEGMENT_MS, -1)],
            use_alter_cfg=[False, True])
    def test_segment_not_rolling(self, server_topic_cfg, use_alter_cfg: bool):
        server_cfg, topic_cfg = server_topic_cfg
        start_count, stop_count = self.generate_workload(
            server_cfg, topic_cfg, use_alter_cfg, SERVER_SEGMENT_MS,
            self.TEST_NUM_SEGMENTS)
        assert is_approximately_equal(start_count, stop_count, margin=2), f"{start_count=} != {stop_count=} +-2"

    @cluster(num_nodes=1)
    @matrix(server_topic_cfg=[(None, 90000), (SERVER_SEGMENT_MS, None),
                              (SERVER_SEGMENT_MS, 120000)],
            use_alter_cfg=[False, True])
    def test_segment_rolling(self, server_topic_cfg, use_alter_cfg):
        server_cfg, topic_cfg = server_topic_cfg
        rolling_period = topic_cfg if topic_cfg is not None else server_cfg
        start_count, stop_count = self.generate_workload(
            server_cfg, topic_cfg, use_alter_cfg, rolling_period,
            self.TEST_NUM_SEGMENTS)
        assert is_approximately_equal(stop_count - start_count,
                                      self.TEST_NUM_SEGMENTS,
                                      margin=3), f"{stop_count=}-{start_count=} !={self.TEST_NUM_SEGMENTS} +-3"

