# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import re
import threading
import time
import requests

from ducktape.mark import matrix
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kcl import KCL
from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient
from rptest.services.admin import Admin
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.redpanda import RedpandaService, CHAOS_LOG_ALLOW_LIST, PREV_VERSION_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.tests.end_to_end import EndToEndTest
from rptest.utils.node_operations import FailureInjectorBackgroundThread, NodeOpsExecutor, generate_random_workload

DECOMMISSION = "decommission"
ADD = "add"
ADD_NO_WAIT = "add_no_wait"

# Allow logs from previously allowed on old versions.
V22_1_CHAOS_ALLOW_LOGS = [
    re.compile("storage - Could not parse header"),
    re.compile("storage - Cannot continue parsing"),
    re.compile(
        ".*controller_backend.*exception while executing partition operation:.*"
    ),
    # rpc - Service handler threw an exception: std::exception (std::exception)
    re.compile("rpc - Service handler threw an exception: std"),
    # rpc - Service handler threw an exception: seastar::broken_promise (broken promise)"
    re.compile("rpc - Service handler threw an exception: seastar"),
]


class NodeOperationFuzzyTest(EndToEndTest):
    max_suspend_duration_seconds = 10
    min_inter_failure_time = 30
    max_inter_failure_time = 60
    producer_throughput = 1000
    min_produced_records = 20 * producer_throughput
    consumer_timeout = 180
    producer_timeout = 180

    def _create_random_topics(self, count, compacted):
        max_partitions = 10

        topics = []
        for i in range(0, count):
            spec = TopicSpec(partition_count=random.randint(1, max_partitions),
                             replication_factor=3,
                             cleanup_policy=TopicSpec.CLEANUP_COMPACT
                             if compacted else TopicSpec.CLEANUP_DELETE)

            topics.append(spec)

        for spec in topics:
            DefaultClient(self.redpanda).create_topic(spec)

        return topics

    @cluster(num_nodes=7,
             log_allow_list=CHAOS_LOG_ALLOW_LIST + V22_1_CHAOS_ALLOW_LOGS +
             PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(enable_failures=[True, False],
            num_to_upgrade=[0, 3],
            compacted_topics=[True, False])
    def test_node_operations(self, enable_failures, num_to_upgrade,
                             compacted_topics):
        lock = threading.Lock()
        # allocate 5 nodes for the cluster
        extra_rp_conf = {
            # make segments small to ensure that they are compacted during
            # the test (only sealed i.e. not being written segments are compacted)
            "compacted_log_segment_size": 5 * (2**20),
            "raft_learner_recovery_rate": 512 * (1024 * 1024)
        }
        if num_to_upgrade > 0:
            # Use the deprecated config to bootstrap older nodes.
            extra_rp_conf["enable_auto_rebalance_on_node_add"] = True
        else:
            extra_rp_conf["partition_autobalancing_mode"] = "node_add"

        # Older versions don't support automatic node ID assignment.
        self._auto_assign_node_ids = num_to_upgrade == 0

        self.redpanda = RedpandaService(self.test_context,
                                        5,
                                        extra_rp_conf=extra_rp_conf)
        if num_to_upgrade > 0:
            installer = self.redpanda._installer
            installer.install(
                self.redpanda.nodes,
                installer.highest_from_prior_feature_version(
                    RedpandaInstaller.HEAD))
            self.redpanda.start()
            installer.install(self.redpanda.nodes[:num_to_upgrade],
                              RedpandaInstaller.HEAD)
            self.redpanda.restart_nodes(self.redpanda.nodes[:num_to_upgrade])
        else:
            self.redpanda.start(auto_assign_node_id=True)

        # create some topics
        topics = self._create_random_topics(10, compacted_topics)
        self.redpanda.logger.info(f"using topics: {topics}")

        # select one of the topics to use in consumer/producer
        self.topic = random.choice(topics).name
        # start workload
        self.start_producer(1,
                            throughput=self.producer_throughput,
                            repeating_keys=100 if compacted_topics else None)
        self.start_consumer(1, verify_offsets=not compacted_topics)
        # wait for some records to be produced

        self.await_startup(min_records=3 * self.producer_throughput)

        # start admin ops fuzzer to simulate day 2 operations
        admin_fuzz = AdminOperationsFuzzer(self.redpanda,
                                           operations_interval=3,
                                           min_replication=3)

        admin_fuzz.start()
        active_nodes = set([self.redpanda.idx(n) for n in self.redpanda.nodes])
        fi = None
        if enable_failures:
            fi = FailureInjectorBackgroundThread(self.redpanda, self.logger,
                                                 lock)
            fi.start()

        # Versions below v22.3.x don't support omitting node ID, so use the old
        # style of configuration when old nodes are present.
        executor = NodeOpsExecutor(self.redpanda,
                                   self.logger,
                                   lock,
                                   has_pre_22_3_nodes=num_to_upgrade > 0)
        fi = None
        if enable_failures:
            fi = FailureInjectorBackgroundThread(self.redpanda, self.logger,
                                                 lock)
            fi.start()

        op_cnt = 30
        for i, op in enumerate(
                generate_random_workload(available_nodes=active_nodes)):
            if i >= op_cnt:
                break
            self.logger.info(f"starting operation {i+1}/{op_cnt}")
            executor.execute_operation(op)

        if fi:
            fi.stop()

        admin_fuzz.wait(20, 180)
        admin_fuzz.stop()
        self.run_validation(min_records=self.min_produced_records,
                            enable_idempotence=False,
                            producer_timeout_sec=self.producer_timeout,
                            consumer_timeout_sec=self.consumer_timeout,
                            enable_compaction=compacted_topics)
