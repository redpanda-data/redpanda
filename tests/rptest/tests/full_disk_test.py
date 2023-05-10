# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import requests
from pickletools import long1
from time import sleep, time, time_ns
from collections import defaultdict

from ducktape.cluster.cluster import ClusterNode
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from kafka import KafkaProducer
from kafka.errors import BrokerNotAvailableError, NotLeaderForPartitionError
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.default import DefaultClient
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import LoggingConfig
from rptest.services.storage import Topic
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import search_logs_with_timeout, produce_total_bytes
from rptest.utils.full_disk import FullDiskHelper
from rptest.utils.partition_metrics import PartitionMetrics
from rptest.utils.expect_rate import ExpectRate, RateTarget

# reduce this?
MAX_MSG: int = 600
MAX_MSG_PER_SEC = 10
FDT_LOG_ALLOW_LIST = [".*cluster - storage space alert: free space.*"]
LOOP_ITERATIONS = 3

import logging
import sys


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
            not_producing = RateTarget(max_total_sec=40,
                                       target_sec=5,
                                       target_min_rate=0,
                                       target_max_rate=1)
            self.bytes_prod.expect_rate(not_producing)

            self.full_disk.clear_low_space()


class FullDiskReclaimTest(RedpandaTest):
    """
    Test that full disk alert triggers eager gc to reclaim space
    """
    topics = (TopicSpec(partition_count=10,
                        retention_bytes=1,
                        retention_ms=1,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_ctx):
        extra_rp_conf = dict(
            log_compaction_interval_ms=24 * 60 * 60 * 1000,
            log_segment_size=1048576,
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

        def observed_data_size(pred):
            observed = self.redpanda.data_stat(node)
            observed_total = sum(s for _, s in observed)
            return pred(observed_total)

        # write around 30 megabytes into the topic
        produce_total_bytes(self.redpanda, self.topic, nbytes(30))

        # wait until all that data shows up. add some fuzz factor to avoid
        # timeouts due to placement skew or other such issues.
        wait_until(lambda: observed_data_size(lambda s: s > nbytes(25)),
                   timeout_sec=30,
                   backoff_sec=2)

        # reclaim shouldn't be running. we can't wait indefinitely to prove that
        # it isn't, but wait a little bit of time for stabilization.
        sleep(30)

        # wait until all that data shows up. add some fuzz factor to avoid
        # timeouts due to placement skew or other such issues.
        wait_until(lambda: observed_data_size(lambda s: s > nbytes(25)),
                   timeout_sec=30,
                   backoff_sec=2)

        # now trigger the disk space alert on the same node. unlike the 30
        # second delay above, we should almost immediately observe the data
        # be reclaimed from disk.
        full_disk = FullDiskHelper(self.logger, self.redpanda)
        full_disk.trigger_low_space(node=node)

        # now wait until the data drops below 1 mb
        wait_until(lambda: observed_data_size(lambda s: s < nbytes(1)),
                   timeout_sec=10,
                   backoff_sec=2)


class LocalDiskReportTest(RedpandaTest):
    topics = (TopicSpec(), TopicSpec(partition_count=4))

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
