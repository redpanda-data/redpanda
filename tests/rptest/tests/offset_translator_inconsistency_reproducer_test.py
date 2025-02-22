# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import time
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.tests.partition_movement import PartitionMovementMixin
import threading

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec


class ConsumerOffsetTopicLoadGenerator:
    def __init__(self, redpanda, num_groups, topics):
        self.rpk = RpkTool(redpanda)
        self.num_groups = num_groups
        self.topics = topics
        self._stop = threading.Event()

    def start(self):
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()
        self.start_time = time.time()

    def stop(self):
        self._stop.set()
        self.thread.join()

    def run(self):
        while not self._stop.is_set():
            group = f"group{random.randint(0,self.num_groups)}"
            try:
                self.rpk.group_seek_to(group, "start", self.topics, True)
            except:
                time.sleep(2)  # relax


class OffsetTranslatorInconsistencyReproducerTest(PartitionMovementMixin,
                                                  RedpandaTest):
    topics = (TopicSpec(partition_count=100), )

    def __init__(self, test_context):
        super(OffsetTranslatorInconsistencyReproducerTest,
              self).__init__(test_context=test_context,
                             num_brokers=5,
                             extra_rp_conf=dict(
                                 compacted_log_segment_size=2**20,
                                 enable_leader_balancer=False,
                                 log_compaction_interval_ms=10 * 1000,
                                 group_topic_partitions=2,
                             ))
        self.rpk = RpkTool(self.redpanda)

    def assert_consistency(self):
        metric_name = "vectorized_raft_offset_translator_inconsistency_errors_total"
        errors = self.redpanda.metric_sum(metric_name, expect_metric=True)
        assert errors == 0

    def _moves(self):
        """
        Random partition movement
        """
        start = time.time()
        while (time.time() - start) < 200:
            self._move_and_verify("__consumer_offsets")
            self.assert_consistency()
            time.sleep(20)

    def _perturb(self):
        start = time.time()
        while (time.time() - start) < 1000:
            self._moves()
            self.redpanda.restart_nodes(random.choice(self.redpanda.nodes))

    @cluster(num_nodes=5)
    def offset_translator_inconsistency_reproducer_test(self):
        # ensure consumer offsets topic exists
        self.rpk.group_seek_to("group0", "start", self.topics[0].name, True)
        wait_until(lambda: len(
            list(self.rpk.describe_topic("__consumer_offsets"))) > 0,
                   timeout_sec=10,
                   backoff_sec=1)

        # start load generators
        generators = [
            ConsumerOffsetTopicLoadGenerator(self.redpanda, 1000,
                                             [t.name for t in self.topics])
            for _ in range(10)
        ]
        for generator in generators:
            generator.start()

        try:
            self._perturb()
        finally:
            for generator in generators:
                generator.stop()
