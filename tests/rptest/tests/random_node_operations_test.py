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
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.tests.end_to_end import EndToEndTest
from rptest.utils.mode_checks import cleanup_on_early_exit, skip_debug_mode
from rptest.utils.node_operations import FailureInjectorBackgroundThread, NodeOpsExecutor, generate_random_workload

from rptest.clients.offline_log_viewer import OfflineLogViewer


class RandomNodeOperationsTest(EndToEndTest):
    def __init__(self, test_context):
        self.admin_fuzz = None
        super(RandomNodeOperationsTest,
              self).__init__(test_context=test_context)

    def min_producer_records(self):
        return 20 * self.producer_throughput

    def _create_topics(self, count):

        topics = []
        for _ in range(0, count):
            spec = TopicSpec(partition_count=random.randint(
                1, self.max_partitions),
                             replication_factor=3)
            topics.append(spec)

        for spec in topics:
            DefaultClient(self.redpanda).create_topic(spec)

        self.topic = random.choice(topics).name

    def tearDown(self):
        if self.admin_fuzz is not None:
            self.admin_fuzz.stop()

        return super().tearDown()

    def early_exit_hook(self):
        """
        Hook for `skip_debug_mode` decorator
        """
        if self.redpanda:
            self.redpanda.set_skip_if_no_redpanda_log(True)

    @skip_debug_mode
    @cluster(num_nodes=7,
             log_allow_list=CHAOS_LOG_ALLOW_LIST + PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(enable_failures=[True, False],
            num_to_upgrade=[0, 3],
            compacted_topics=[True, False])
    def test_node_operations(self, enable_failures, num_to_upgrade,
                             compacted_topics):

        lock = threading.Lock()

        extra_rp_conf = {
            "compacted_log_segment_size": 5 * (2**20),
            "default_topic_replications": 3,
            "raft_learner_recovery_rate": 512 * (1024 * 1024),
            "partition_autobalancing_mode": "node_add",
            # set disk timeout to value greater than max suspend time
            # not to emit spurious errors
            "raft_io_timeout_ms": 20000,
        }

        self.redpanda = RedpandaService(self.test_context,
                                        5,
                                        extra_rp_conf=extra_rp_conf)

        # test setup
        self.producer_timeout = 180
        self.consumer_timeout = 180

        if self.redpanda.dedicated_nodes:
            # scale test setup
            self.max_partitions = 32
            self.producer_throughput = 20000
            self.node_operations = 30
        else:
            # container test setup
            # skip compacted topics and upgrade tests when running in container
            if compacted_topics or num_to_upgrade > 0:

                cleanup_on_early_exit(self)
                return
            self.max_partitions = 32
            self.producer_throughput = 1000 if self.debug_mode else 10000
            self.node_operations = 10

        self.redpanda.set_seed_servers(self.redpanda.nodes)
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
            self.redpanda.start(auto_assign_node_id=True,
                                omit_seeds_on_idx_one=False)
        # create some topics
        self._create_topics(10)
        # start workload
        self.start_producer(1,
                            throughput=self.producer_throughput,
                            repeating_keys=100 if compacted_topics else None)
        self.start_consumer(1, verify_offsets=not compacted_topics)

        self.await_startup(min_records=3 * self.producer_throughput)

        # start admin operations fuzzer, it will provide a stream of
        # admin day 2 operations executed during the test
        self.admin_fuzz = AdminOperationsFuzzer(self.redpanda,
                                                min_replication=3,
                                                operations_interval=3)

        self.admin_fuzz.start()
        self.active_node_idxs = set(
            [self.redpanda.idx(n) for n in self.redpanda.nodes])

        executor = NodeOpsExecutor(self.redpanda, self.logger, lock)
        fi = None
        if enable_failures:
            fi = FailureInjectorBackgroundThread(self.redpanda, self.logger,
                                                 lock)
            fi.start()

        for i, op in enumerate(
                generate_random_workload(
                    available_nodes=self.active_node_idxs)):
            if i >= self.node_operations:
                break
            self.logger.info(
                f"starting operation {i+1}/{self.node_operations}")
            executor.execute_operation(op)

        self.admin_fuzz.wait(20, 180)
        self.admin_fuzz.stop()

        if enable_failures:
            fi.stop()
        self.run_validation(min_records=self.min_producer_records(),
                            enable_idempotence=False,
                            producer_timeout_sec=self.producer_timeout,
                            consumer_timeout_sec=self.consumer_timeout,
                            enable_compaction=compacted_topics)

        # Validate that the controller log written during the test is readable by offline log viewer
        log_viewer = OfflineLogViewer(self.redpanda)
        for node in self.redpanda.started_nodes():
            # stop node before reading controller log to make sure it is stable
            self.redpanda.stop_node(node)
            controller_records = log_viewer.read_controller(node=node)
            self.logger.info(
                f"Read {len(controller_records)} controller records from node {node.name} successfully"
            )
