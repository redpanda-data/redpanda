import typing

from ducktape.mark import defaults, parametrize
from ducktape.utils.util import wait_until
import time

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from rptest.services.verifiable_consumer import VerifiableConsumer
from rptest.services.verifiable_producer import VerifiableProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import segments_count
from rptest.services.redpanda import RedpandaService

# NOTE this value could be read from redpanda
# for example this should be a reasonable way
# int(Admin(self.redpanda).get_cluster_config_schema()
#                       ['properties']['log_segment_ms_min']['example'])
SERVER_SEGMENT_MS = 60000
SERVER_HOUSEKEEPING_LOOP = 10


def messages_to_generate_segments(num_segments, rolling_period, message_throughput=1):
    return int(((rolling_period / 1000) * num_segments + 1) * message_throughput)


TEST_NUM_SEGMENTS = 5
ERROR_MARGIN = 1
TEST_LOG_SEGMENT_MIN = 60000
TEST_LOG_SEGMENT_MAX = 150000


class TopicManager:
    """
    A helper to manage the testing context of a topic.
    """

    def __init__(
        self,
        redpanda: RedpandaService,
        topic_segment_sec: None | int,
        segment_sec: int,
        use_alter_config: bool,
    ):
        self.redpanda = redpanda
        self.rpk = RpkTool(self.redpanda)
        self.topic_segment_ms = (
            None if topic_segment_sec is None else topic_segment_sec * 1000
        )
        self._segment_ms = segment_sec * 1000
        self.use_alter_config = use_alter_config
        self.topic = TopicSpec()

    def create(self):
        # set the config at topic creation time?
        create_config = None
        if self.topic_segment_ms and not self.use_alter_config:
            create_config = {TopicSpec.PROPERTY_SEGMENT_MS: self.topic_segment_ms}

        self.rpk.create_topic(self.topic.name, config=create_config)

        # or after creation, with alter topic config rpc
        if self.topic_segment_ms and self.use_alter_config:
            self.rpk.alter_topic_config(
                self.topic.name, TopicSpec.PROPERTY_SEGMENT_MS, self.topic_segment_ms
            )

    def produce(self):
        self.rpk.produce(self.topic.name, "hello", "world")

    def num_segments(self):
        return next(segments_count(self.redpanda, self.topic.name, 0))

    def segment_ms(self):
        return self._segment_ms


class SegmentMsTest(RedpandaTest):
    def __init__(self, test_context):
        super().__init__(
            test_context=test_context,
            num_brokers=1,
            environment={"__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"},
            extra_rp_conf={
                "enable_leader_balancer": False,
                "log_compaction_interval_ms": 1000,
            },
        )

    def _total_segments_count(self, topic_spec: TopicSpec, partition=0):
        return next(segments_count(self.redpanda, topic_spec.name, partition))

    def generate_workload(
        self,
        server_cfg: typing.Optional[int],
        topic_cfg: typing.Optional[int],
        use_alter_cfg: bool,
        num_messages: int,
        extra_cluster_cfg={},
    ) -> tuple[int, int]:
        """
        Create some workload to trigger time-based segment rolling. setup cluster, create a topic with config
        (either at creation time or with alter_config),
        then starts a producer at 1hz of small #num_messages.
        returns the number of segment before and after the workload
        """
        self.redpanda.set_cluster_config(
            {"log_segment_ms": server_cfg} | extra_cluster_cfg, expect_restart=False
        )

        topic = TopicSpec()
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            topic.name,
            partitions=1,
            config={TopicSpec.PROPERTY_SEGMENT_MS: topic_cfg}
            if topic_cfg is not None and not use_alter_cfg
            else None,
        )

        if use_alter_cfg:
            if topic_cfg:
                rpk.alter_topic_config(
                    topic.name, TopicSpec.PROPERTY_SEGMENT_MS, topic_cfg
                )
            else:
                rpk.delete_topic_config(topic.name, TopicSpec.PROPERTY_SEGMENT_MS)

        producer = VerifiableProducer(
            context=self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=topic.name,
            max_messages=num_messages,
            throughput=1,
        )
        consumer = VerifiableConsumer(
            context=self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=topic.name,
            group_id=0,
        )
        start_count = self._total_segments_count(topic)
        consumer.start()
        producer.start()
        producer.wait(num_messages + 30)
        wait_until(
            lambda: consumer.total_consumed() >= num_messages,
            timeout_sec=30,
            err_msg=f"consumer failed to consume {num_messages=} msgs",
        )
        consumer.stop()
        producer.stop()
        stop_count = self._total_segments_count(topic)
        assert producer.num_acked == consumer.total_consumed(), (
            f"failed to preserve the messages across segment rolling"
        )

        assert stop_count >= start_count

        return start_count, stop_count

    @cluster(num_nodes=3)
    @defaults(use_alter_cfg=[False, True])
    @parametrize(server_cfg=None, topic_cfg=None)
    @parametrize(server_cfg=SERVER_SEGMENT_MS, topic_cfg=-1)
    def test_segment_not_rolling(self, server_cfg, topic_cfg, use_alter_cfg: bool):
        """
        Tests under several conditions that a segment will not roll,
        either because there is no config or the topic property is set to disable.
        The topic is created with the configuration or an alter config is issued to configure it
        """

        num_messages = messages_to_generate_segments(
            TEST_NUM_SEGMENTS, SERVER_SEGMENT_MS
        )

        # make the test fair by ensuring that the range segment.ms is clamped too is low enough
        start_count, stop_count = self.generate_workload(
            server_cfg,
            topic_cfg,
            use_alter_cfg,
            num_messages,
            extra_cluster_cfg={"log_segment_ms_min": "60000"},
        )
        assert start_count == stop_count, f"{start_count=} != {stop_count=}"

    @cluster(num_nodes=1)
    def test_segment_rolling(self):
        # Setup the cluster configs min=5s, max=50s, default=20s
        self.redpanda.set_cluster_config(
            dict(
                log_segment_ms_min=5000, log_segment_ms_max=45000, log_segment_ms=20000
            )
        )

        topics: list[TopicManager] = []
        start_segment_counts: list[int] = []

        # define topics with segment_ms configs. each one is repeated with
        # use_alter_config true/false controlling if segment_ms is set via
        # craete topic property or post-create with alter topic config rpc.

        # 20s (default)
        topics.append(TopicManager(self.redpanda, None, 20, False))
        topics.append(TopicManager(self.redpanda, None, 20, True))
        # 5s (1s clamped to min of 5s)
        topics.append(TopicManager(self.redpanda, 1, 5, False))
        topics.append(TopicManager(self.redpanda, 1, 5, True))
        # 35s (no clamping)
        topics.append(TopicManager(self.redpanda, 35, 35, False))
        topics.append(TopicManager(self.redpanda, 35, 35, True))
        # 50s (100s clamped to max of 50s)
        topics.append(TopicManager(self.redpanda, 100, 45, False))
        topics.append(TopicManager(self.redpanda, 100, 45, True))

        # initialize topics and initial segment counts
        for topic in topics:
            topic.create()
            start_segment_counts.append(topic.num_segments())

        # run for some time, producing a little bit
        runtime_sec = 5 * 60
        start_sec = time.time()
        while time.time() - start_sec < runtime_sec:
            for topic in topics:
                topic.produce()
            time.sleep(1)

        failures: list[str] = []
        for topic, start_segment_count in zip(topics, start_segment_counts):
            end_segment_count = topic.num_segments()
            created_segments = end_segment_count - start_segment_count

            # add on a couple to account for the initial segment and if there
            # were any triggers other than time
            max_expected_segments = 2 + runtime_sec * 1000 // topic.segment_ms()

            # calculate the expected as if the segment roll was always 1.5s too
            # late. in reality its certainly not going to be exactly on time,
            # and 2 seconds should be plenty of grace period.
            min_expected_segments = runtime_sec * 1000 // (2000 + topic.segment_ms())

            failed = (
                created_segments < min_expected_segments
                or created_segments > max_expected_segments
            )
            if failed:
                failures.append(
                    f"failed segment_ms {topic.segment_ms()} min={min_expected_segments} actual={created_segments} max={max_expected_segments}"
                )

        assert len(failures) == 0, f"{failures}"

    @cluster(num_nodes=2)
    def test_segment_timely_rolling_after_change(self):
        """
        Tests that an old enough segment will roll in a finite amount of time,
        once an alter-config sets segment.ms low enough
        """
        # ensure that the cluster do not have a fallback value, and that the clamp range of segment.ms is low enough
        self.redpanda.set_cluster_config(
            {"log_segment_ms": None, "log_segment_ms_min": "60000"}
        )
        topic = TopicSpec()
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic=topic.name, partitions=1)

        # write data to a topic, wait for a period of time T, check that the number of segments did not increase,
        # stop the messages, set segment.ms to less than T, watch the segment rolling
        message_threshold = messages_to_generate_segments(1, SERVER_SEGMENT_MS)

        producer = VerifiableProducer(
            context=self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=topic.name,
            throughput=1,
        )

        start_count = self._total_segments_count(topic)
        producer.start()
        wait_until(
            lambda: producer.num_acked >= message_threshold,
            timeout_sec=message_threshold + 30,
            err_msg=f"producer failed to produce {message_threshold=} msgs",
        )

        middle_count = self._total_segments_count(topic)
        assert middle_count == start_count, (
            f"segment rolled, but it was not supposed to. {middle_count=} {start_count=}"
        )

        # rolling should happen independently of writes
        producer.stop()

        rpk.alter_topic_config(topic.name, "segment.ms", SERVER_SEGMENT_MS)

        wait_until(
            lambda: self._total_segments_count(topic) > middle_count,
            timeout_sec=SERVER_HOUSEKEEPING_LOOP * 2,
            err_msg=f"failed to roll a segment in a timely manner",
        )

    @cluster(num_nodes=3)
    def test_segment_rolling_with_retention(self):
        self.redpanda.set_cluster_config(
            {"log_segment_ms": None, "log_segment_ms_min": 10000}
        )
        topic = TopicSpec(
            segment_bytes=(1024 * 1024), replication_factor=1, partition_count=1
        )
        self.client().create_topic(topic)

        producer = VerifiableProducer(
            context=self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=topic.name,
            throughput=10000,
        )

        producer.start()
        wait_until(
            lambda: self._total_segments_count(topic) >= 5,
            timeout_sec=120,
            err_msg="producer failed to produce enough messages to create 5 segments",
        )
        # stop producer
        producer.stop()
        producer.clean()
        producer.free()
        del producer
        start_count = self._total_segments_count(topic)
        self.client().alter_topic_config(topic.name, "segment.ms", "15000")

        # wait for the segment.ms policy to roll the segment
        wait_until(
            lambda: self._total_segments_count(topic) > start_count,
            timeout_sec=60,
            err_msg=f"failed waiting for the segment to roll",
        )

        self.client().alter_topic_config(
            topic.name, "retention.local.target.ms", "10000"
        )
        self.client().alter_topic_config(topic.name, "retention.ms", "10000")

        # wait for retention policy to trigger
        wait_until(
            lambda: self._total_segments_count(topic) <= 1,
            timeout_sec=60,
            err_msg=f"failed waiting for retention policy",
        )

        producer = VerifiableProducer(
            context=self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=topic.name,
            throughput=10000,
        )
        producer.start()
        wait_until(
            lambda: self._total_segments_count(topic) >= 2,
            timeout_sec=120,
            err_msg=f"producer failed to produce enough messages to create 5 segments",
        )
        producer.stop()

        consumer = VerifiableConsumer(
            context=self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=topic.name,
            group_id="test-group",
        )
        consumer.start()
        # Wait for any messages to be consumed,
        # (if there is an issue in the offsets handling it will result in consumer being stuck)
        wait_until(
            lambda: consumer.total_consumed() >= 1000,
            timeout_sec=120,
            err_msg=f"Failed to consume messages",
        )
        consumer.stop()

    @cluster(num_nodes=4)
    def test_segment_rolling_with_retention_consumer(self):
        self.redpanda.set_cluster_config(
            {"log_segment_ms": None, "log_segment_ms_min": 10000}
        )
        topic = TopicSpec(
            segment_bytes=(1024 * 1024), replication_factor=1, partition_count=1
        )

        self.client().create_topic(topic)

        producer = VerifiableProducer(
            context=self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=topic.name,
            throughput=10000,
        )

        producer.start()
        wait_until(
            lambda: self._total_segments_count(topic) >= 5,
            timeout_sec=120,
            err_msg="producer failed to produce enough messages to create 5 segments",
        )
        # stop producer
        producer.stop()
        producer.clean()
        producer.free()
        del producer
        start_count = self._total_segments_count(topic)
        self.client().alter_topic_config(topic.name, "segment.ms", "15000")

        # wait for the segment.ms policy to roll the segment
        wait_until(
            lambda: self._total_segments_count(topic) > start_count,
            timeout_sec=60,
            err_msg=f"failed waiting for the segment to roll",
        )
        group_id = "test-group"

        consumer = VerifiableConsumer(
            context=self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=topic.name,
            group_id=group_id,
        )
        consumer.start()

        self.client().alter_topic_config(
            topic.name, "retention.local.target.ms", "10000"
        )
        self.client().alter_topic_config(topic.name, "retention.ms", "10000")

        # wait for retention policy to trigger
        wait_until(
            lambda: self._total_segments_count(topic) <= 1,
            timeout_sec=60,
            err_msg=f"failed waiting for retention policy",
        )
        consumer.stop()

        producer = VerifiableProducer(
            context=self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=topic.name,
            throughput=10000,
        )
        producer.start()
        wait_until(
            lambda: self._total_segments_count(topic) >= 2,
            timeout_sec=120,
            err_msg=f"producer failed to produce enough messages to create 5 segments",
        )
        producer.stop()

        rpk = RpkTool(self.redpanda)

        def no_lag_present():
            group = rpk.group_describe(group_id)
            self.logger.info(f"group: {group}")
            if len(group.partitions) != topic.partition_count:
                return False

            return all([p.lag == 0 for p in group.partitions])

        consumer_2 = KafkaCliConsumer(
            context=self.test_context,
            redpanda=self.redpanda,
            topic=topic.name,
            group=group_id,
        )
        consumer_2.start()
        # # Wait for any messages to be consumed,
        # # (if there is an issue in the offsets handling it will result in consumer being stuck)
        wait_until(
            no_lag_present,
            timeout_sec=120,
            err_msg=f"Failed to consume all messages from {topic.name}",
        )
        consumer_2.stop()
