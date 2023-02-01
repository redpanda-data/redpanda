from ducktape.mark import matrix, parametrize, defaults
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.services.verifiable_producer import VerifiableProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import segments_count

# NOTE this value could be read from redpanda
# for example this should be a reasonable way
# int(Admin(self.redpanda).get_cluster_config_schema()
#                       ['properties']['log_segment_ms_min']['example'])
SERVER_SEGMENT_MS = 60000
SERVER_ROLLING_SECONDS = SERVER_SEGMENT_MS / 1000
SERVER_HOUSEKEEPING_LOOP = 10


def messages_to_generate_segments(num_segments, period):
    return int(period / 1000 * (num_segments) + SERVER_HOUSEKEEPING_LOOP + 1)


class SegmentMsTest(RedpandaTest):
    TEST_NUM_SEGMENTS = 3
    ERROR_MARGIN = 1

    def _total_segments_count(self, topic_spec: TopicSpec, partition=0):
        return next(segments_count(self.redpanda, topic_spec.name, partition))

    def generate_workload(self, server_cfg, topic_cfg, use_alter_cfg: bool,
                          rolling_period, segments_to_produce):
        self.redpanda.set_cluster_config({'log_segment_ms': server_cfg},
                                         expect_restart=False)

        topic = TopicSpec()
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic.name,
                         partitions=1,
                         config={TopicSpec.PROPERTY_SEGMENT_MS: topic_cfg} if
                         topic_cfg is not None and not use_alter_cfg else None)

        if use_alter_cfg:
            if topic_cfg:
                rpk.alter_topic_config(topic.name,
                                       TopicSpec.PROPERTY_SEGMENT_MS,
                                       topic_cfg)
            else:
                rpk.delete_topic_config(topic.name,
                                        TopicSpec.PROPERTY_SEGMENT_MS)

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
        start_count = self._total_segments_count(topic)
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
    @defaults(use_alter_cfg=[False, True])
    @parametrize(server_cfg=None, topic_cfg=None)
    @parametrize(server_cfg=SERVER_SEGMENT_MS, topic_cfg=-1)
    def test_segment_not_rolling(self, server_cfg, topic_cfg,
                                 use_alter_cfg: bool):
        start_count, stop_count = self.generate_workload(
            server_cfg, topic_cfg, use_alter_cfg, SERVER_SEGMENT_MS * 2,
            self.TEST_NUM_SEGMENTS)
        assert start_count == stop_count, f"{start_count=} != {stop_count=}"

    @cluster(num_nodes=1)
    @defaults(use_alter_cfg=[False, True])
    @parametrize(server_cfg=None, topic_cfg=90000)
    @parametrize(server_cfg=SERVER_SEGMENT_MS, topic_cfg=None)
    @parametrize(server_cfg=SERVER_SEGMENT_MS, topic_cfg=120000)
    def test_segment_rolling(self, server_cfg, topic_cfg, use_alter_cfg):
        rolling_period = topic_cfg if topic_cfg is not None else server_cfg
        start_count, stop_count = self.generate_workload(
            server_cfg, topic_cfg, use_alter_cfg, rolling_period,
            self.TEST_NUM_SEGMENTS)
        assert abs((stop_count - start_count) - self.TEST_NUM_SEGMENTS) <= self.ERROR_MARGIN, \
                                       f"{stop_count=}-{start_count=} != {self.TEST_NUM_SEGMENTS=} +-{self.ERROR_MARGIN=}"
