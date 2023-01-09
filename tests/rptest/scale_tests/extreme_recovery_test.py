from logging import Logger
from time import time
from typing import Callable, Sequence

from ducktape.mark import ignore
from ducktape.tests.test import TestContext
from rptest.archival.s3_client import S3Client
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.franz_go_verifiable_services import \
    FranzGoVerifiableProducer, \
    FranzGoVerifiableSeqConsumer
from rptest.tests.topic_recovery_test import BaseCase, TopicRecoveryTest
from rptest.utils.si_utils import TRANSIENT_ERRORS


class RecoveryScale(BaseCase):
    """
    This test case covers normal recovery process at a larger scale, using a
    bigger cluster and creating more partitions.
    """
    # TODO until we have weekly/bi-weekly schedules, this test
    # is scaled down to run with CDT nightlies.
    NIGHTLY_SCALE_DOWN = 2

    # TODO rookie numbers
    NODE_COUNT = 5
    # More partitions cause the test take a long time to run, hitting some
    # bottlenecks in the existing test, such as serial computation of file
    # checksums, etc.
    PARTITONS = NODE_COUNT * 200 // NIGHTLY_SCALE_DOWN
    NUM_TOPICS = 2
    PRODUCERS_PER_TOPIC = 1
    MESSAGE_COUNT = 50 * 1000 * PARTITONS // NIGHTLY_SCALE_DOWN
    MSG_SIZE = 1023

    # Scale tests run ducktape on dedicated nodes, so we can expect some
    # minimum bandwidth, and set a test timeout based on that.
    _min_bandwidth_bps = 2**26  # 64 MiB/sec
    _total_bytes = MSG_SIZE * MESSAGE_COUNT
    expected_transfer_sec = _total_bytes // (_min_bandwidth_bps) + 30
    _msg_per_topic = MESSAGE_COUNT // NUM_TOPICS
    part_per_topic = PARTITONS // NUM_TOPICS

    _status_log: list[str] = []

    def __init__(self, test_context: TestContext, s3_client: S3Client,
                 kafka_tools: KafkaCliTools, rpk_client: RpkTool,
                 s3_bucket: str, logger: Logger, topics: Sequence[TopicSpec],
                 rpk_producer_maker: Callable):
        self.topics = topics
        self.ctx = test_context
        self.logger = logger
        self.redpanda = kafka_tools._redpanda
        super(RecoveryScale,
              self).__init__(s3_client, kafka_tools, rpk_client, s3_bucket,
                             logger, rpk_producer_maker)

    @property
    def verify_s3_content_after_produce(self):
        # Data verification is good here, can catch missing uploads.
        return True

    def _status(self, message: str) -> None:
        self.logger.info(message)
        self._status_log.append(message)

    def log_status(self):
        self.logger.info("=====  \u2696  Scale Test Summary  \u2696  =====")
        for l in self._status_log:
            self.logger.info(l)
        self.logger.info("=====  \u2696                      \u2696  =====")

    def generate_baseline(self):
        producers: list[FranzGoVerifiableProducer] = []
        self._status(
            f"{len(self.topics)} topics, {RecoveryScale.PARTITONS} partitions")
        t0 = time()
        # start multiple producers per topic
        for topic in self.topics:
            for i in range(self.PRODUCERS_PER_TOPIC):
                self.logger.info(
                    f"gen baseline: make producer {i+1} of "
                    f"{self.PRODUCERS_PER_TOPIC} for {topic.name}")
                p = FranzGoVerifiableProducer(
                    self.ctx, self.redpanda, topic.name, self.MSG_SIZE,
                    self._msg_per_topic // self.PRODUCERS_PER_TOPIC)
                p.start()
                producers.append(p)

        # wait for completion
        for i, p in enumerate(producers):
            self.redpanda.logger.info(
                f"gen baseline: wait for producer {i+1}.. ")
            p.wait(self.expected_transfer_sec)
        t1 = time()
        for p in producers:
            p.free()
        delta_t = t1 - t0
        gib = self._total_bytes / 2**30
        mib = self._total_bytes / 2**20
        self._status(f"Produced {gib:0.3f} GiB in {delta_t:0.3f}s, "
                     f"{mib/delta_t:0.3f} MiB/s.")

    def validate_cluster(self, baseline, restored):
        """Validate all data after recovery."""
        self.logger.info(
            f"vallidate_cluster base:restored: {baseline}:{restored}")

        self._validate_partition_last_offset()
        expected_topics = [
            topic.name for topic in self.expected_recovered_topics
        ]

        # consume and validate all data
        t0 = time()
        consumers: list[FranzGoVerifiableSeqConsumer] = list()
        for topic in self.topics:
            self.logger.info(
                f'validate_cluster: starting consumer for "{topic}"')
            c = FranzGoVerifiableSeqConsumer(self.ctx, self.redpanda, topic,
                                             self.MSG_SIZE)
            c.start()
            consumers.append(c)

        total_bytes = 0
        for c in consumers:
            self.logger.info(
                f'validate_cluster: waiting on consumer for "{c._topic}"')
            c.shutdown()
            c.wait(self.expected_transfer_sec)
            total_bytes += c.consumer_status.valid_reads * self.MSG_SIZE
            assert c.consumer_status.invalid_reads == 0, "No invalid reads expected."
            assert c.consumer_status.valid_reads == self._msg_per_topic

        delta_t = time() - t0
        self._status(
            f'validate_cluster: consumed {total_bytes/2**20:0.1f} MB in {delta_t:0.3f} sec;'
            f' {(total_bytes/delta_t)/2**20:0.3f} MB/s.')

    @property
    def second_restart_needed(self):
        """Return True if the restart is needed afer first validation steps"""
        # XXX TODO
        return False

    def after_restart_validation(self):
        """Check that topic is writable after restart"""
        # XXX TODO
        pass


# "It's like, totally extreme, bro."
# Try 1: hack topic recovery test into being a scale test.
class ExtremeRecoveryTest(TopicRecoveryTest):
    def __init__(self, test_context: TestContext):
        extra_rp_conf = {"blah": "bloo"}
        super(ExtremeRecoveryTest,
              self).__init__(test_context,
                             num_brokers=RecoveryScale.NODE_COUNT)
        assert (self.si_settings and
                self.si_settings.cloud_storage_max_connections > 10), \
                    "Need more connections for scale test"
        self.initial_cleanup_needed = True
        self.ctx = test_context

    def tearDown(self):
        super(ExtremeRecoveryTest, self).tearDown()

    @cluster(num_nodes=8, log_allow_list=TRANSIENT_ERRORS)
    def test_recovery_scale(self):
        # This test requires dedicated system resources
        assert self.redpanda.dedicated_nodes

        assert self.cloud_storage_client
        topics = [
            TopicSpec(partition_count=RecoveryScale.part_per_topic,
                      replication_factor=3)
            for i in range(RecoveryScale.NUM_TOPICS)
        ]

        extreme_recovery = RecoveryScale(self.test_context,
                                         self.cloud_storage_client,
                                         self.kafka_tools, self.rpk,
                                         self.s3_bucket, self.logger, topics,
                                         self.rpk_producer_maker)
        # Expected transer sec is usually accurate in my testing,
        # but it takes more time for s3 object metadata to reflect the changes
        # made to the bucket. This can be observed by watching cloud storage
        # upload metrics while the test is running.
        self.do_run(extreme_recovery, RecoveryScale.expected_transfer_sec * 2)
        extreme_recovery.log_status()

    # XXX TODO this inheritence is silly, do some refactoring.
    @ignore
    def test_fast1(self):
        pass

    @ignore
    def test_fast2(self):
        pass

    @ignore
    def test_fast3(self):
        pass

    @ignore
    def test_missing_partition(self):
        pass

    @ignore
    def test_missing_segment(self):
        pass

    @ignore
    def test_missing_topic_manifest(self):
        pass

    @ignore
    def test_no_data(self):
        pass

    @ignore
    def test_size_based_retention(self):
        pass

    @ignore
    def test_time_based_retention(self):
        pass

    @ignore
    def test_time_based_retention_with_legacy_manifest(self):
        pass

    @ignore
    def test_empty_segments(self):
        pass
