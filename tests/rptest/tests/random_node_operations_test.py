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
from rptest.services.admin import Admin
from rptest.tests.prealloc_nodes import PreallocNodesTest

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST, PREV_VERSION_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.utils.mode_checks import cleanup_on_early_exit, skip_debug_mode
from rptest.utils.node_operations import FailureInjectorBackgroundThread, NodeOpsExecutor, generate_random_workload

from rptest.clients.offline_log_viewer import OfflineLogViewer


class RandomNodeOperationsTest(PreallocNodesTest):
    def __init__(self, test_context, *args, **kwargs):
        self.admin_fuzz = None
        self.should_skip = False
        super().__init__(
            test_context=test_context,
            num_brokers=5,
            extra_rp_conf={
                "default_topic_replications": 3,
                "raft_learner_recovery_rate": 512 * (1024 * 1024),
                "partition_autobalancing_mode": "node_add",
                # set disk timeout to value greater than max suspend time
                # not to emit spurious errors
                "raft_io_timeout_ms": 20000,
            },
            # 2 nodes for kgo producer/consumer workloads
            node_prealloc_count=2,
            *args,
            **kwargs)

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

    def setUp(self):
        # defer starting redpanda to test body
        pass

    def _setup_test_scale(self, num_to_upgrade):
        # test setup
        self.producer_timeout = 180
        self.consumer_timeout = 180

        if num_to_upgrade > 0:
            # we can only enable controller snapshot if all nodes are the newest version
            cleanup_on_early_exit(self)
            return

        if self.redpanda.dedicated_nodes:
            # scale test setup
            self.max_partitions = 32
            self.producer_throughput = 20000
            self.node_operations = 30
            self.msg_size = 1024  # 1KiB
            self.rate_limit = 100 * 1024 * 1024  # 100 MBps
            self.total_data = 5 * 1024 * 1024 * 1024
        else:
            # container test setup
            if num_to_upgrade > 0:
                self.should_skip = True

            self.max_partitions = 32
            self.producer_throughput = 1000 if self.debug_mode else 10000
            self.node_operations = 10
            self.msg_size = 128
            self.rate_limit = 1024 * 1024
            self.total_data = 50 * 1024 * 1024

        self.consumers_count = int(self.max_partitions / 4)
        self.msg_count = int(self.total_data / self.msg_size)

        self.logger.info(
            f"running test with: [message_size {self.msg_size},  total_bytes: {self.total_data}, message_count: {self.msg_count}, rate_limit: {self.rate_limit}, cluster_operations: {self.node_operations}]"
        )

    def _start_redpanda(self, number_nodes_to_upgrade,
                        enable_controller_snapshots):
        self.redpanda.set_seed_servers(self.redpanda.nodes)
        if number_nodes_to_upgrade > 0:
            installer = self.redpanda._installer
            installer.install(
                self.redpanda.nodes,
                installer.highest_from_prior_feature_version(
                    RedpandaInstaller.HEAD))
            self.redpanda.start()
            installer.install(self.redpanda.nodes[:number_nodes_to_upgrade],
                              RedpandaInstaller.HEAD)
            self.redpanda.restart_nodes(
                self.redpanda.nodes[:number_nodes_to_upgrade])
        else:
            self.redpanda.start(auto_assign_node_id=True,
                                omit_seeds_on_idx_one=False)

    class producer_consumer:
        def __init__(self, test_context, logger, topic_spec, redpanda, nodes,
                     msg_size, rate_limit_bps, msg_count, consumers_count):
            self.test_context = test_context
            self.logger = logger
            self.topic = topic_spec
            self.redpanda = redpanda
            self.nodes = nodes
            self.msg_size = msg_size
            self.rate_limit_bps = rate_limit_bps
            self.msg_count = msg_count
            self.consumer_count = consumers_count

        def _start_producer(self):
            self.producer = KgoVerifierProducer(
                self.test_context,
                self.redpanda,
                self.topic.name,
                self.msg_size,
                self.msg_count,
                custom_node=self.nodes,
                rate_limit_bps=self.rate_limit_bps)

            self.producer.start(clean=False)

            wait_until(lambda: self.producer.produce_status.acked > 10,
                       timeout_sec=120,
                       backoff_sec=1)

        def _start_consumer(self):

            self.consumer = KgoVerifierConsumerGroupConsumer(
                self.test_context,
                self.redpanda,
                self.topic.name,
                self.msg_size,
                readers=self.consumer_count,
                nodes=self.nodes)

            self.consumer.start(clean=False)

        def start(self):
            DefaultClient(self.redpanda).create_topic(self.topic)
            self.logger.info(
                f"starting workload: topic: {self.topic}, with [rate_limit: {self.rate_limit_bps}, message size: {self.msg_size}, message count: {self.msg_count}]"
            )

            self._start_producer()
            self._start_consumer()

        def verify(self):
            self.logger.info(
                f"verifying workload: topic: {self.topic}, with [rate_limit: {self.rate_limit_bps}, message size: {self.msg_size}, message count: {self.msg_count}]"
            )
            self.producer.wait()

            # Await the consumer that is reading only the subset of data that
            # was written before it started.
            self.consumer.wait()

            assert self.consumer.consumer_status.validator.invalid_reads == 0, f"Invalid reads in topic: {self.topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"
            del self.consumer

            # Start a new consumer to read all data written
            self._start_consumer()
            self.consumer.wait()
            if self.topic.cleanup_policy != TopicSpec.CLEANUP_COMPACT:
                assert self.consumer.consumer_status.validator.valid_reads >= self.producer.produce_status.acked, f"Missing messages from topic: {self.topic}. valid reads: {self.consumer.consumer_status.validator.valid_reads}, acked messages: {self.producer.produce_status.acked}"

            assert self.consumer.consumer_status.validator.invalid_reads == 0, f"Invalid reads in topic: {self.topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"

    @skip_debug_mode
    @cluster(num_nodes=7,
             log_allow_list=CHAOS_LOG_ALLOW_LIST + PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(enable_failures=[False, True],
            num_to_upgrade=[0],
            enable_controller_snapshots=[True, False])
    def test_node_operations(self, enable_failures, num_to_upgrade,
                             enable_controller_snapshots):

        lock = threading.Lock()

        # setup test case scale parameters
        self._setup_test_scale(num_to_upgrade)
        if self.should_skip:
            cleanup_on_early_exit(self)
            return

        # start redpanda process
        self._start_redpanda(num_to_upgrade, enable_controller_snapshots)

        if enable_controller_snapshots:
            admin = Admin(self.redpanda)
            admin.put_feature("controller_snapshots", {"state": "active"})
            self.redpanda.await_feature_active("controller_snapshots",
                                               timeout_sec=30)
            self.redpanda.set_cluster_config(
                {"controller_snapshot_max_age_sec": 1})

        # create some initial topics
        self._create_topics(10)

        regular_producer_consumer = RandomNodeOperationsTest.producer_consumer(
            test_context=self.test_context,
            logger=self.logger,
            topic_spec=TopicSpec(partition_count=self.max_partitions,
                                 replication_factor=3,
                                 cleanup_policy=TopicSpec.CLEANUP_DELETE),
            redpanda=self.redpanda,
            nodes=[self.preallocated_nodes[0]],
            msg_size=self.msg_size,
            rate_limit_bps=self.rate_limit,
            msg_count=self.msg_count,
            consumers_count=self.consumers_count)

        compacted_producer_consumer = RandomNodeOperationsTest.producer_consumer(
            test_context=self.test_context,
            logger=self.logger,
            topic_spec=TopicSpec(partition_count=self.max_partitions,
                                 cleanup_policy=TopicSpec.CLEANUP_COMPACT,
                                 segment_bytes=1 * 1024 * 1024),
            redpanda=self.redpanda,
            nodes=[self.preallocated_nodes[1]],
            msg_size=self.msg_size,
            rate_limit_bps=self.rate_limit,
            msg_count=self.msg_count,
            consumers_count=self.consumers_count)

        regular_producer_consumer.start()
        compacted_producer_consumer.start()

        # start admin operations fuzzer, it will provide a stream of
        # admin day 2 operations executed during the test
        self.admin_fuzz = AdminOperationsFuzzer(self.redpanda,
                                                min_replication=3,
                                                operations_interval=3)

        self.admin_fuzz.start()
        self.active_node_idxs = set(
            [self.redpanda.idx(n) for n in self.redpanda.nodes])

        fi = None
        if enable_failures:
            fi = FailureInjectorBackgroundThread(self.redpanda, self.logger,
                                                 lock)
            fi.start()

        # main workload loop
        executor = NodeOpsExecutor(self.redpanda, self.logger, lock)
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

        # stop producer and consumer and verify results
        regular_producer_consumer.verify()
        compacted_producer_consumer.verify()

        # Validate that the controller log written during the test is readable by offline log viewer
        log_viewer = OfflineLogViewer(self.redpanda)
        for node in self.redpanda.started_nodes():
            # stop node before reading controller log to make sure it is stable
            self.redpanda.stop_node(node)
            controller_records = log_viewer.read_controller(node=node)
            self.logger.info(
                f"Read {len(controller_records)} controller records from node {node.name} successfully"
            )
