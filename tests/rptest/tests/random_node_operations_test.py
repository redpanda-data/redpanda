# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import threading

from ducktape.mark import matrix
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient
from rptest.services.redpanda import RedpandaService, CHAOS_LOG_ALLOW_LIST, PREV_VERSION_LOG_ALLOW_LIST
from rptest.tests.end_to_end import EndToEndTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.node_operations import FailureInjectorBackgroundThread, NodeOpsExecutor, generate_random_workload


class RandomNodeOperationsTest(EndToEndTest):

    consumer_timeout = 180
    producer_timeout = 180

    def producer_throughput(self):
        return 1000 if self.debug_mode else 10000

    def min_producer_records(self):
        return 20 * self.producer_throughput()

    def _create_topics(self, count):
        max_partitions = 32
        topics = []
        for _ in range(0, count):
            spec = TopicSpec(partition_count=random.randint(1, max_partitions),
                             replication_factor=3)
            topics.append(spec)

        for spec in topics:
            DefaultClient(self.redpanda).create_topic(spec)

        self.topic = random.choice(topics).name

    @skip_debug_mode
    @cluster(num_nodes=7,
             log_allow_list=CHAOS_LOG_ALLOW_LIST + PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(enable_failures=[False, True])
    def test_node_operations(
        self,
        enable_failures,
    ):
        lock = threading.Lock()
        extra_rp_conf = {
            "default_topic_replications": 3,
            "raft_learner_recovery_rate": 512 * (1024 * 1024),
            "partition_autobalancing_mode": "node_add",
        }

        self.redpanda = RedpandaService(self.test_context,
                                        5,
                                        extra_rp_conf=extra_rp_conf)

        self.redpanda.set_seed_servers(self.redpanda.nodes)

        self.redpanda.start(auto_assign_node_id=True,
                            omit_seeds_on_idx_one=False)
        # create some topics
        self._create_topics(10)
        # start workload
        self.start_producer(1, throughput=self.producer_throughput())
        self.start_consumer(1)
        self.await_startup()

        # start admin operations fuzzer, it will provide a stream of
        # admin day 2 operations executed during the test
        admin_fuzz = AdminOperationsFuzzer(self.redpanda,
                                           min_replication=3,
                                           operations_interval=3)

        admin_fuzz.start()
        self.active_node_idxs = set(
            [self.redpanda.idx(n) for n in self.redpanda.nodes])

        executor = NodeOpsExecutor(self.redpanda, self.logger, lock)
        fi = None
        if enable_failures:
            fi = FailureInjectorBackgroundThread(self.redpanda, self.logger,
                                                 lock)
            fi.start()
        op_cnt = 10
        for i, op in enumerate(
                generate_random_workload(
                    available_nodes=self.active_node_idxs)):
            if i >= op_cnt:
                break
            self.logger.info(f"starting operation {i+1}/{op_cnt}")
            executor.execute_operation(op)

        admin_fuzz.wait(20, 180)
        admin_fuzz.stop()
        if enable_failures:
            fi.stop()
        self.run_validation(min_records=self.min_producer_records(),
                            enable_idempotence=False,
                            producer_timeout_sec=self.producer_timeout,
                            consumer_timeout_sec=self.consumer_timeout)
