import typing

from ducktape.mark import parametrize, defaults
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
SERVER_HOUSEKEEPING_LOOP = 10


def messages_to_generate_segments(num_segments,
                                  rolling_period,
                                  message_throughput=1):
    return int(
        ((rolling_period / 1000) * num_segments + 1) * message_throughput)


TEST_NUM_SEGMENTS = 5
ERROR_MARGIN = 1
TEST_LOG_SEGMENT_MIN = 60000
TEST_LOG_SEGMENT_MAX = 150000


class SegmentMsTest(RedpandaTest):
    def __init__(self, test_context):
        super().__init__(test_context=test_context,
                         num_brokers=1,
                         extra_rp_conf={"enable_leader_balancer": False})

    def _total_segments_count(self, topic_spec: TopicSpec, partition=0):
        return next(segments_count(self.redpanda, topic_spec.name, partition))

    def generate_workload(self,
                          server_cfg: typing.Optional[int],
                          topic_cfg: typing.Optional[int],
                          use_alter_cfg: bool,
                          num_messages: int,
                          extra_cluster_cfg={}) -> tuple[int, int]:
        """
        Create some workload to trigger time-based segment rolling. setup cluster, create a topic with config
        (either at creation time or with alter_config),
        then starts a producer at 1hz of small #num_messages.
        returns the number of segment before and after the workload
        """
        self.redpanda.set_cluster_config({'log_segment_ms': server_cfg}
                                         | extra_cluster_cfg,
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

        producer = VerifiableProducer(context=self.test_context,
                                      num_nodes=1,
                                      redpanda=self.redpanda,
                                      topic=topic.name,
                                      max_messages=num_messages,
                                      throughput=1)
        consumer = VerifiableConsumer(context=self.test_context,
                                      num_nodes=1,
                                      redpanda=self.redpanda,
                                      topic=topic.name,
                                      group_id=0)
        start_count = self._total_segments_count(topic)
        consumer.start()
        producer.start()
        producer.wait(num_messages + 30)
        wait_until(lambda: consumer.total_consumed() >= num_messages,
                   timeout_sec=30,
                   err_msg=f"consumer failed to consume {num_messages=} msgs")
        consumer.stop()
        producer.stop()
        stop_count = self._total_segments_count(topic)
        assert producer.num_acked == consumer.total_consumed(), \
            f"failed to preserve the messages across segment rolling"

        assert stop_count >= start_count

        return start_count, stop_count

    @cluster(num_nodes=3)
    @defaults(use_alter_cfg=[False, True])
    @parametrize(server_cfg=None, topic_cfg=None)
    @parametrize(server_cfg=SERVER_SEGMENT_MS, topic_cfg=-1)
    def test_segment_not_rolling(self, server_cfg, topic_cfg,
                                 use_alter_cfg: bool):
        """
        Tests under several conditions that a segment will not roll,
        either because there is no config or the topic property is set to disable.
        The topic is created with the configuration or an alter config is issued to configure it
        """

        num_messages = messages_to_generate_segments(TEST_NUM_SEGMENTS,
                                                     SERVER_SEGMENT_MS)

        start_count, stop_count = self.generate_workload(
            server_cfg, topic_cfg, use_alter_cfg, num_messages)
        assert start_count == stop_count, f"{start_count=} != {stop_count=}"

    @cluster(num_nodes=3)
    @defaults(use_alter_cfg=[False, True])
    # these are outside TEST_LOG_SEGMENT_MIN/MAX range, they will be clamped
    @parametrize(server_cfg=None, topic_cfg=int(TEST_LOG_SEGMENT_MIN / 2))
    @parametrize(server_cfg=int(TEST_LOG_SEGMENT_MAX * 2), topic_cfg=None)
    @parametrize(server_cfg=None, topic_cfg=int(TEST_LOG_SEGMENT_MAX * 2))
    # topic_cfg will be used
    @parametrize(server_cfg=None, topic_cfg=90000)
    @parametrize(server_cfg=SERVER_SEGMENT_MS, topic_cfg=120000)
    # server_cfg will be used as a fallback
    @parametrize(server_cfg=SERVER_SEGMENT_MS, topic_cfg=None)
    def test_segment_rolling(self, server_cfg, topic_cfg, use_alter_cfg):
        """
        Tests under several conditions that a segment will roll,
        either for the topic config or if none, for the server config.
        Tests also that the number of rolls is congruent (within an error margin) with the configuration.
        The topic is created with the configuration or an alter config is issued to configure it
        """

        # rolling is clamped by redpanda in the [log_segment_ms_min, log_segment_ms_max] range, so it's replicated here
        rolling_period = sorted(
            (TEST_LOG_SEGMENT_MIN, topic_cfg if topic_cfg else server_cfg,
             TEST_LOG_SEGMENT_MAX))[1]
        num_messages = messages_to_generate_segments(TEST_NUM_SEGMENTS,
                                                     rolling_period)
        start_count, stop_count = self.generate_workload(
            server_cfg,
            topic_cfg,
            use_alter_cfg,
            num_messages,
            extra_cluster_cfg={
                'log_segment_ms_min': TEST_LOG_SEGMENT_MIN,
                'log_segment_ms_max': TEST_LOG_SEGMENT_MAX
            })
        assert abs((stop_count - start_count) - TEST_NUM_SEGMENTS) <= ERROR_MARGIN, \
                                       f"{stop_count=}-{start_count=} != {TEST_NUM_SEGMENTS=} +-{ERROR_MARGIN=}"

    @cluster(num_nodes=2)
    def test_segment_timely_rolling_after_change(self):
        """
        Tests that an old enough segment will roll in a finite amount of time,
        once an alter-config sets segment.ms low enough
        """
        # ensure that the cluster do not have a fallback value
        self.redpanda.set_cluster_config({"log_segment_ms": None})
        topic = TopicSpec()
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic=topic.name, partitions=1)

        # write data to a topic, wait for a period of time T, check that the number of segments did not increase,
        # stop the messages, set segment.ms to less than T, watch the segment rolling
        message_threshold = messages_to_generate_segments(1, SERVER_SEGMENT_MS)

        producer = VerifiableProducer(context=self.test_context,
                                      num_nodes=1,
                                      redpanda=self.redpanda,
                                      topic=topic.name,
                                      throughput=1)

        start_count = self._total_segments_count(topic)
        producer.start()
        wait_until(
            lambda: producer.num_acked >= message_threshold,
            timeout_sec=message_threshold + 30,
            err_msg=f"producer failed to produce {message_threshold=} msgs")

        middle_count = self._total_segments_count(topic)
        assert middle_count == start_count, f"segment rolled, but it was not supposed to. {middle_count=} {start_count=}"

        # rolling should happen independently of writes
        producer.stop()

        rpk.alter_topic_config(topic.name, "segment.ms", SERVER_SEGMENT_MS)

        wait_until(lambda: self._total_segments_count(topic) > middle_count,
                   timeout_sec=SERVER_HOUSEKEEPING_LOOP * 2,
                   err_msg=f"failed to roll a segment in a timely manner")
