# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
from time import sleep, time

import requests
from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from kafka import KafkaProducer
from kafka.errors import BrokerNotAvailableError, NotLeaderForPartitionError

from rptest.clients.default import DefaultClient
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer
from rptest.services.redpanda import LoggingConfig, RedpandaService, SISettings
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import produce_total_bytes, search_logs_with_timeout
from rptest.utils.expect_rate import ExpectRate, RateTarget
from rptest.utils.full_disk import FullDiskHelper
from rptest.utils.partition_metrics import PartitionMetrics
from rptest.utils.si_utils import quiesce_uploads

# reduce this?
MAX_MSG: int = 600
MAX_MSG_PER_SEC = 10
FDT_LOG_ALLOW_LIST = [".*cluster - storage space alert: free space.*"]
LOOP_ITERATIONS = 3


# XXX This test really needs a raw protocol client (i.e. not librdkafka # based)
# to test cleanly. That, or figure out how to make kafka-python act like one.
class WriteRejectTest(RedpandaTest):

    NUM_TOPICS = 1
    topics = [TopicSpec() for _ in range(NUM_TOPICS)]
    topic_names = [spec.name for spec in topics]

    def __init__(self, test_context: TestContext):
        super().__init__(test_context,
                         log_config=LoggingConfig(
                             'info', logger_levels={'cluster': 'trace'}))
        self.producers = None
        self.full_disk = FullDiskHelper(self.logger, self.redpanda)

    def _get_producers(self, refresh=False) -> list[KafkaProducer]:
        if not self.producers:
            self.producers = [
                KafkaProducer(acks="all",
                              bootstrap_servers=self.redpanda.brokers_list(),
                              value_serializer=str.encode,
                              retries=10) for _ in range(len(self.topics))
            ]
        return self.producers

    def _setup(self) -> None:
        self.logger.debug(f"brokers: {self.redpanda.brokers_list()}")
        self.admin = Admin(self.redpanda)

        # python kafka client doesn't seem to recover from no leader for partition,
        # despite retries, when it encounters ECONNREFUSED. The sleep here works
        # around this.
        sleep(5)

        self.NUM_MESSAGES = MAX_MSG // len(self.topics)
        self.PAUSE_S = len(self.topics) * 1.0 / MAX_MSG_PER_SEC

    def _send_all_topics(self, msg: str, expect_blocked=False):
        """ Send `msg` to all topics, retrying a fixed number of times for
        expected success or failure. """
        futures = []
        num_topics = self.NUM_MESSAGES
        pause: float = self.PAUSE_S
        was_blocked = False
        success = False
        t0 = time()
        i = 0
        producers = self._get_producers(refresh=True)
        for _ in range(self.NUM_MESSAGES):
            sleep(pause)
            try:
                for j, topic in enumerate(self.topic_names):
                    producer = producers[j]
                    self.logger.debug(f"send msg {i},{j}, t={time()-t0:.2f}")
                    i = i + 1
                    f = producer.send(topic, msg)
                    f.get(30)
            except BrokerNotAvailableError as e1:
                was_blocked = True
                if expect_blocked:
                    self.logger.info(f"Write rejected as expected: {e1}")
                    break
                else:
                    # health monitor reports take time to propagate
                    self.logger.debug("Still being blocked, retrying.")
                    continue
            except NotLeaderForPartitionError as e2:
                self.logger.debug(f"Not leader for partition {e2}--retrying.")
                continue
            # no exception, success
            if not expect_blocked:
                success = True
                break
        if expect_blocked:
            assert was_blocked, "Failed to block write for full disk"
        else:
            assert success, "Failed to produce"

    # XXX see class comment      /|\
    @cluster(num_nodes=3, log_allow_list=FDT_LOG_ALLOW_LIST)
    def test_write_rejection(self):
        """ Verify that we block external writers when free disk space on any
        node is below the threshold. """

        self._setup()

        for i in range(LOOP_ITERATIONS):
            self.logger.info(f"Iteration {i} of {LOOP_ITERATIONS}..")
            # 1. Confirm we can produce successfully.
            self._send_all_topics("Q: What do redpandas eat?")

            # 2. Set threshold below current free space, assert we get rejected.
            self.full_disk.trigger_low_space()
            self._send_all_topics(
                "A: I don't know, but they can do a lot of bites per second.",
                expect_blocked=True)
            self.full_disk.clear_low_space()

    @cluster(num_nodes=3, log_allow_list=FDT_LOG_ALLOW_LIST)
    def test_refresh_disk_health(self):
        """ Verify that health monitor frontend and backend have valid state after
        we manually trigger a refresh.
        """
        def check_health_monitor_frontend(disk_space_change: str):
            # Looking for a log statement about a change in disk space.
            # This is a check for the health monitor frontend because
            # that structure logs disk space alerts.
            pattern = f"Update disk health cache {disk_space_change}"
            wait_until(
                lambda: self.redpanda.search_log_any(pattern),
                timeout_sec=5,
                err_msg=f"Failed to find disk space change: {disk_space_change}"
            )

        def check_health_monitor_backend(fail_msg: str):
            # This is an indirect check for the health monitor backend because
            # RedpandaService.healthy() uses the prometheus metric, vectorized_cluster_partition_under_replicated_replicas,
            # which is measured in the health monitor backend.
            wait_until(lambda: self.redpanda.healthy(),
                       timeout_sec=20,
                       backoff_sec=1,
                       err_msg=fail_msg)

        self._setup()

        self._send_all_topics("Q: Why should you be scared of computers?")
        self.full_disk.trigger_low_space()
        self._send_all_topics("A: They byte!", expect_blocked=True)

        # Disk health should show degraded state
        check_health_monitor_frontend(disk_space_change="ok -> degraded")

        check_health_monitor_backend(
            fail_msg="Cluster is not healthy before health monitor refresh")

        self.logger.debug("Check health monitor refresh")
        for node in self.redpanda.nodes:
            result_raw = self.admin.refresh_disk_health_info(node=node)
            self.logger.debug(result_raw)
            search_logs_with_timeout(self.redpanda,
                                     "Refreshing disk health info")
            assert result_raw.status_code == requests.codes.ok

        check_health_monitor_backend(
            fail_msg="Cluster is not healthy after health monitor refresh")

        # RP should still reject writes before clearing space
        self._send_all_topics("Q: What animal does a computer like to watch?",
                              expect_blocked=True)

        self.full_disk.clear_low_space()

        # RP should now accept writes after clearing space
        self._send_all_topics("A: A RAM!")

        # Expect the disk health check to tranistion from degraded to ok
        check_health_monitor_frontend(disk_space_change="degraded -> ok")


class FullDiskTest(EndToEndTest):
    def __init__(self, test_ctx):
        extra_rp_conf = {
            "health_monitor_max_metadata_age": 1000,
            "metrics_reporter_tick_interval": 2000,
            "metrics_reporter_report_interval": 1000,
        }
        super().__init__(test_context=test_ctx, extra_rp_conf=extra_rp_conf)
        self.start_redpanda(num_nodes=3)
        assert self.redpanda
        self.pm = PartitionMetrics(self.redpanda)
        self.full_disk = FullDiskHelper(self.logger, self.redpanda)
        self.bytes_prod = ExpectRate(lambda: self.pm.bytes_produced(),
                                     self.logger)

    @cluster(num_nodes=5, log_allow_list=FDT_LOG_ALLOW_LIST)
    def test_full_disk_no_produce(self):
        # Create topics, some extra, to exercise different placements
        topics = []
        for _ in range(0, 5):
            topics.append(TopicSpec(partition_count=random.randint(1, 10)))
        # chose one topic to run the main workload
        DefaultClient(self.redpanda).create_topic(topics)
        self.topic = random.choice(topics).name
        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()
        for i in range(LOOP_ITERATIONS):
            self.logger.info(f"Iteration {i} of {LOOP_ITERATIONS}..")

            # assert can produce to topic, by total partition bytes metric
            producing = RateTarget(max_total_sec=40,
                                   target_sec=5,
                                   target_min_rate=100,
                                   target_max_rate=2**40)
            self.bytes_prod.expect_rate(producing)

            self.full_disk.trigger_low_space()

            # confirm we were blocked from producing
            not_producing = RateTarget(
                max_total_sec=40 if not self.debug_mode else 100,
                target_sec=5,
                target_min_rate=0,
                target_max_rate=1)
            self.bytes_prod.expect_rate(not_producing)

            self.full_disk.clear_low_space()


class FullDiskReclaimTest(RedpandaTest):
    """
    Test that full disk alert triggers eager gc to reclaim space
    """
    partition_count = 10
    log_segment_size = 1048576

    topics = (TopicSpec(partition_count=partition_count,
                        retention_bytes=1,
                        retention_ms=1,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_ctx):
        extra_rp_conf = dict(
            log_compaction_interval_ms=24 * 60 * 60 * 1000,
            log_segment_size=self.log_segment_size,
            log_segment_size_jitter_percent=0,
        )
        super().__init__(test_context=test_ctx, extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3, log_allow_list=FDT_LOG_ALLOW_LIST)
    def test_full_disk_triggers_gc(self):
        """
        Test that GC is run promptly when a full disk alert is raised.

        The test works as follows:

        1. Create a topic with lots of data eligible for reclaim
        2. Configure Redpanda to not run compaction housekeeping
        3. Verify that after some amount of time no data is reclaimed
        4. Trigger a low disk space alert
        5. Observe that data is reclaimed from disk
        """
        nbytes = lambda mb: mb * 2**20
        node = self.redpanda.nodes[0]

        produce_size = 3 * self.partition_count * self.log_segment_size
        expected_size_after_gc = self.partition_count * self.log_segment_size + nbytes(
            1)
        assert expected_size_after_gc < produce_size

        def observed_data_size(pred):
            observed = self.redpanda.data_stat(node)
            observed_total = sum(s for path, s in observed
                                 if path.parts[0] == 'kafka')
            return pred(observed_total)

        # write into the topic
        produce_total_bytes(self.redpanda, self.topic, produce_size)

        # wait until all that data shows up. add some fuzz factor to avoid
        # timeouts due to placement skew or other such issues.
        wait_until(lambda: observed_data_size(lambda s: s >= produce_size),
                   timeout_sec=30,
                   backoff_sec=2)

        # reclaim shouldn't be running. we can't wait indefinitely to prove that
        # it isn't, but wait a little bit of time for stabilization.
        sleep(30)

        # wait until all that data shows up. add some fuzz factor to avoid
        # timeouts due to placement skew or other such issues.
        wait_until(lambda: observed_data_size(lambda s: s >= produce_size),
                   timeout_sec=30,
                   backoff_sec=2)

        # now trigger the disk space alert on the same node. unlike the 30
        # second delay above, we should almost immediately observe the data
        # be reclaimed from disk.
        full_disk = FullDiskHelper(self.logger, self.redpanda)
        full_disk.trigger_low_space(node=node)

        # now wait until the data drops
        # the expected size is at most one segment for each partition and a
        # bit extra for stm snapshots. although we expect the subsystem to
        # reclaim all segments there are other internal systems
        # (e.g.leadership balancer) which can trigger writes to the partitions.
        wait_until(
            lambda: observed_data_size(lambda s: s < expected_size_after_gc),
            timeout_sec=10,
            backoff_sec=2)


class LocalDiskReportTimeTest(RedpandaTest):
    topics = (TopicSpec(segment_bytes=2**20,
                        partition_count=1,
                        retention_bytes=-1,
                        retention_ms=60 * 60 * 1000,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_ctx):
        extra_rp_conf = dict(log_compaction_interval_ms=3600 * 1000)
        super().__init__(test_context=test_ctx, extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_target_min_capacity_wanted_time_based(self):
        admin = Admin(self.redpanda)
        default_segment_size = admin.get_cluster_config()["log_segment_size"]
        storage_reserve_min_segments = admin.get_cluster_config(
        )["storage_reserve_min_segments"]

        # produce roughly 30mb at 0.5mb/sec
        kafka_tools = KafkaCliTools(self.redpanda)
        kafka_tools.produce(self.topic,
                            30 * 1024,
                            1024,
                            throughput=500,
                            acks=-1,
                            linger_ms=50)

        node = self.redpanda.nodes[0]
        reported = admin.get_local_storage_usage(
            node)["target_min_capacity_wanted"]

        # The size is slightly larger than what was written, attributable to
        # per record overheads, indices, fallocation, etc... The expected size
        # is determined empirically by looking at trace log stats.
        size = 32664482
        time = 61
        retention = 3600
        expected = retention * (size / time)

        # Factor in the full segments worth of space for controller log.
        # This mirrors the math in disk_log_impl.cc
        controller_want_size = storage_reserve_min_segments * default_segment_size

        diff = reported - controller_want_size - expected

        # There is definitely going to be some fuzz factor needed here and may
        # need updated.
        diff_threshold = 100 * 2**20

        self.logger.info(
            f"{diff=} {diff_threshold=} {reported=} {expected=} {controller_want_size=}"
        )
        assert abs(
            diff
        ) <= diff_threshold, f"abs({diff=}) <= {diff_threshold=} {reported=} {expected=} {controller_want_size=}"


class LocalDiskReportTest(RedpandaTest):
    topics = (
        TopicSpec(segment_bytes=2**30,
                  retention_bytes=2 * 2**30,
                  cleanup_policy=TopicSpec.CLEANUP_COMPACT),
        TopicSpec(
            partition_count=4,
            # retention should be larger than default segment size
            retention_bytes=200 * 2**20,
            cleanup_policy=TopicSpec.CLEANUP_DELETE))

    def __init__(self, test_ctx):
        extra_rp_conf = dict(log_compaction_interval_ms=3600 * 1000)
        super().__init__(test_context=test_ctx, extra_rp_conf=extra_rp_conf)

    def check(self, threshold):
        admin = Admin(self.redpanda)
        for node in self.redpanda.nodes:
            reported = admin.get_local_storage_usage(node)
            reported_total = reported["data"] + reported["index"] + reported[
                "compaction"]
            observed = self.redpanda.data_stat(node)
            observed_total = sum(s for _, s in observed)
            diff = observed_total - reported_total
            pct_diff = abs(diff / reported_total)
            if pct_diff > threshold:
                return False
        return True

    @cluster(num_nodes=3)
    def test_target_min_capacity_wanted(self):
        """
        Test minimum capacity wanted calculation.
        """
        admin = Admin(self.redpanda)
        default_segment_size = admin.get_cluster_config()["log_segment_size"]
        node = self.redpanda.nodes[0]

        # minimum based on retention bytes policy
        reported = admin.get_local_storage_usage(
            node)["target_min_capacity_wanted"]
        expected = 4 * 200 * 2**20 + \
                2 * 1 * 2**30 # the compacted topic clamp min wanted at min needed but doesn't contain any data
        diff = abs(reported - expected)
        # the difference should be less than the one extra segment per
        # partitions since the reported size will try to round up to the nearest
        # segment.
        assert diff <= (
            4 * default_segment_size
        ), f"diff {diff} expected {expected} reported {reported}"

    @cluster(num_nodes=3)
    def test_target_min_capacity(self):
        """
        Test that the target min storage capcity reflects changes to reserved min
        segments configuration option.
        """
        admin = Admin(self.redpanda)
        default_segment_size = admin.get_cluster_config()["log_segment_size"]
        node = self.redpanda.nodes[0]

        for min_segments in (1, 2, 3):
            self.redpanda.set_cluster_config(
                dict(storage_reserve_min_segments=min_segments))
            reported = admin.get_local_storage_usage(
                node)["target_min_capacity"]

            # 1 partition with 1gb segment size
            # 4 partition with default segment size
            # controller partition has default segment size
            # reservation will be for min_segments
            expected = min_segments * ((1 * self.topics[0].segment_bytes) + \
                       (4 * default_segment_size) + \
                       (1 * default_segment_size))

            diff = abs(reported - expected)
            assert diff <= (
                0.001 * reported
            ), f"diff {diff} expected {expected} reported {reported}"

    @cluster(num_nodes=3)
    def test_basic_usage_report(self):
        # start-up
        wait_until(lambda: self.check(0.05), timeout_sec=10, backoff_sec=5)

        for _ in range(2):
            # write some data
            self.kafka_tools = KafkaCliTools(self.redpanda)
            for topic in self.topics:
                self.kafka_tools.produce(topic.name, 10000, 1024)

            wait_until(lambda: self.check(0.05), timeout_sec=10, backoff_sec=5)

            # restart the cluster
            self.redpanda.rolling_restart_nodes(self.redpanda.nodes)

            wait_until(lambda: self.check(0.05), timeout_sec=10, backoff_sec=5)


class DiskStatsOverrideTest(RedpandaTest):
    @cluster(num_nodes=3)
    def test_disk_stat_total_overrides(self):
        admin = Admin(self.redpanda)
        # current size
        orig_stat = admin.get_disk_stat("data", self.redpanda.nodes[0])
        # set total size to 16 KB smaller
        admin.set_disk_stat_override("data",
                                     self.redpanda.nodes[0],
                                     total_bytes=orig_stat["total_bytes"] -
                                     (16 * 1024))
        # updated current size
        stat = admin.get_disk_stat("data", self.redpanda.nodes[0])

        # the sizes aren't exact because there are conversion that involve
        # rounding to block size. use a 4K block threshold to test.
        delta = abs(orig_stat["total_bytes"] - (16 * 1024) -
                    stat["total_bytes"])
        assert delta <= 4096

    @cluster(num_nodes=3)
    def test_disk_stat_free_overrides(self):
        admin = Admin(self.redpanda)
        # current size
        orig_stat = admin.get_disk_stat("data", self.redpanda.nodes[0])
        # set a fixed free bytes
        admin.set_disk_stat_override("data",
                                     self.redpanda.nodes[0],
                                     free_bytes=orig_stat["total_bytes"])
        # updated current size
        stat = admin.get_disk_stat("data", self.redpanda.nodes[0])

        # the sizes aren't exact because there are conversion that involve
        # rounding to block size. use a 4K block threshold to test.
        delta = abs(stat["free_bytes"] - stat["total_bytes"])
        assert delta <= 4096

    @cluster(num_nodes=3)
    def test_disk_stat_free_delta_overrides(self):
        admin = Admin(self.redpanda)
        # current size
        orig_stat = admin.get_disk_stat("data", self.redpanda.nodes[0])
        # set a free bytes adjustment
        # fix free bytes too so this works reliably with noisy neighbors
        admin.set_disk_stat_override(
            "data",
            self.redpanda.nodes[0],
            free_bytes=orig_stat["free_bytes"],
            free_bytes_delta=(orig_stat["total_bytes"] -
                              orig_stat["free_bytes"]))
        # updated current size
        stat = admin.get_disk_stat("data", self.redpanda.nodes[0])

        # the sizes aren't exact because there are conversion that involve
        # rounding to block size. use a 4K block threshold to test.
        delta = abs(stat["free_bytes"] - stat["total_bytes"])
        assert delta <= 4096


class LogStorageMaxSizeSI(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context, *args, **kwargs)

    def setUp(self):
        # defer redpanda startup to the test
        pass

    def _kafka_size_on_disk(self, node):
        total_bytes = 0
        observed = list(self.redpanda.data_stat(node))
        for file, size in observed:
            if len(file.parents) == 1:
                continue
            if file.parents[-2].name == "kafka":
                total_bytes += size
        return total_bytes

    @cluster(num_nodes=4)
    @matrix(
        log_segment_size=[1024 * 1024],
        cleanup_policy=[TopicSpec.CLEANUP_COMPACT, TopicSpec.CLEANUP_DELETE])
    def test_stay_below_target_size(self, log_segment_size, cleanup_policy):
        """
        Tests that when a log storage target size is specified that data
        uploaded into s3 will become eligible for forced GC in order to meet the
        target size.
        """
        # start redpanda with specific config like segment size
        si_settings = SISettings(test_context=self.test_context,
                                 log_segment_size=log_segment_size,
                                 fast_uploads=True)
        extra_rp_conf = {
            'compacted_log_segment_size': log_segment_size,
            'disk_reservation_percent': 0,
            'retention_local_target_capacity_percent': 100,
            'retention_local_trim_interval': 1000,  # every second
        }
        self.redpanda.set_extra_rp_conf(extra_rp_conf)
        self.redpanda.set_si_settings(si_settings)
        self.redpanda.start()

        # test parameters
        topic_name = "target-size-topic"
        partition_count = 4
        replica_count = 3
        msg_size = 65536
        fuzz_size = 1 * 2**20

        # we will always try to latest 2 segments per partition so this would be
        # roughly the lowest size we'd be able to get to in best case
        target_size = partition_count * 2 * log_segment_size

        # we'll write 3x the target size, and do it twice
        data_size = target_size * 3

        # make the sink topic
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            topic_name,
            partitions=partition_count,
            replicas=replica_count,
            config={TopicSpec.PROPERTY_CLEANUP_POLICY: cleanup_policy})

        msg_count = data_size // msg_size

        # write `data_size` bytes
        t1 = time()
        KgoVerifierProducer.oneshot(self.test_context,
                                    self.redpanda,
                                    topic_name,
                                    msg_size=msg_size,
                                    msg_count=msg_count,
                                    batch_max_bytes=msg_size * 8)
        produce_duration = time() - t1
        self.logger.info(
            f"Produced {data_size} bytes in {produce_duration} seconds, {(data_size/produce_duration)/1000000.0:.2f}MB/s"
        )

        quiesce_uploads(self.redpanda, [t.name for t in self.topics],
                        timeout_sec=30)

        # verify approx same amount of data on disk. adds on some fuzz factor
        total = sum(self._kafka_size_on_disk(n) for n in self.redpanda.nodes)
        total += fuzz_size * len(self.redpanda.nodes)
        assert total > (data_size * replica_count)

        # set the log storage target size. system will try to meet this target.
        self.redpanda.set_cluster_config(
            dict(retention_local_target_capacity_bytes=target_size, ))

        # now go write another `data_size` bytes
        t1 = time()
        KgoVerifierProducer.oneshot(self.test_context,
                                    self.redpanda,
                                    topic_name,
                                    msg_size=msg_size,
                                    msg_count=msg_count,
                                    batch_max_bytes=msg_size * 8)
        produce_duration = time() - t1
        self.logger.info(
            f"Produced {data_size} bytes in {produce_duration} seconds, {(data_size/produce_duration)/1000000.0:.2f}MB/s"
        )

        # wait until space management kicks in. after data is uploaded into s3
        # it will become eligible for forced gc by space management despite
        # having infinte retention, at which point we should see the storage
        # usage drop back down. we end up writing 3x * 3x the target size, and
        # add a few segments per node on for fuzz factor
        #
        # Exception to this case are topics created with `cleanup.policy=compact`.
        def target_size_reached():
            total = sum(
                self._kafka_size_on_disk(n) for n in self.redpanda.nodes)
            target = ((target_size + 2 * log_segment_size) *
                      len(self.redpanda.nodes))
            below = total < target
            if not below:
                self.logger.debug(
                    f"Reported total across all nodes {total} still larger {target}"
                )
            return below

        # give it plenty of time. on debug it is hella slow
        wait_until(target_size_reached, timeout_sec=30, backoff_sec=5)
        assert min_local_start_offset(
            self.redpanda, topic_name
        ) > 0, "expecting disk storage to be reduced by advancing local offsets (local log prefix trim)"

        # Verify that all data is accessible. This assertion implies that the data
        # was successfully uploaded to the cloud prior to local eviction.
        consumer = KgoVerifierSeqConsumer.oneshot(
            self.test_context,
            self.redpanda,
            topic_name,
            loop=False,
        )
        assert consumer.consumer_status.validator.valid_reads == 2 * msg_count
        assert consumer.consumer_status.validator.invalid_reads == 0
        assert consumer.consumer_status.validator.out_of_scope_invalid_reads == 0


def min_local_start_offset(redpanda: RedpandaService, topic: str):
    min_offset = None
    for node in redpanda.nodes:
        for p in local_start_offsets(redpanda, node, topic):
            if min_offset is None:
                min_offset = p['local_log_start_offset']
            else:
                min_offset = max(min_offset, p['local_log_start_offset'])
    return min_offset


def local_start_offsets(redpanda: RedpandaService, node: ClusterNode,
                        topic: str):
    admin = Admin(redpanda, default_node=node)
    partitions = admin.get_partitions(topic)

    for p in partitions:
        status = admin.get_partition_cloud_storage_status(
            topic, p["partition_id"])

        yield {
            "partition": p["partition_id"],
            "local_log_start_offset": status["local_log_start_offset"]
        }
