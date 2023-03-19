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


class TestSpec:
    def __init__(self,
                 num_brokers=3,
                 enable_failures=False,
                 num_to_upgrade=0,
                 compacted_topics=False,
                 num_topics=10,
                 max_partitions=10,
                 node_fuzz_ops=30,
                 admin_fuzz_ops=20):
        self.num_brokers = num_brokers
        self.enable_failures = enable_failures
        self.num_to_upgrade = num_to_upgrade
        self.compacted_topics = False
        self.num_topics = num_topics
        self.max_partitions = max_partitions
        self.compacted_topics = compacted_topics
        self.node_fuzz_ops = node_fuzz_ops
        self.admin_fuzz_ops = admin_fuzz_ops


class RandomNodeOperationsTestBase(EndToEndTest):
    """Implements the driver logic that maintains the life cycle of
    op fuzzers. Calls start_workload and end_workload methods that child
    classes can implement to plugin a custom workload."""
    def __init__(self, test_context):
        super(RandomNodeOperationsTestBase,
              self).__init__(test_context=test_context)
        self.test_topics = None
        self.test_spec = None
        self.fi = None
        self.admin_fuzz = None

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
        if self.fi:
            self.fi.stop()
        if self.admin_fuzz:
            self.admin_fuzz.stop()
        return super().tearDown()

    def early_exit_hook(self):
        """
        Hook for `skip_debug_mode` decorator
        """
        if self.redpanda:
            self.redpanda.set_skip_if_no_redpanda_log(True)

    def _create_random_topics(self):
        assert self.test_spec
        assert self.redpanda
        topics = []
        for _ in range(0, self.test_spec.num_topics):
            spec = TopicSpec(
                partition_count=random.randint(1,
                                               self.test_spec.max_partitions),
                replication_factor=3,
                cleanup_policy=TopicSpec.CLEANUP_COMPACT if
                self.test_spec.compacted_topics else TopicSpec.CLEANUP_DELETE)

            topics.append(spec)

        for spec in topics:
            DefaultClient(self.redpanda).create_topic(spec)

        return topics

    def start_redpanda(self):
        extra_rp_conf = {
            # make segments small to ensure that they are compacted during
            # the test (only sealed i.e. not being written segments are compacted)
            "compacted_log_segment_size": 5 * (2**20),
            "raft_learner_recovery_rate": 512 * (1024 * 1024),
            # set disk timeout to value greater than max suspend time
            # not to emit spurious errors
            "raft_io_timeout_ms": 20000,
        }

        self.redpanda = RedpandaService(self.test_context,
                                        self.test_spec.num_brokers,
                                        extra_rp_conf=extra_rp_conf)

        self.redpanda.set_seed_servers(self.redpanda.nodes)

        num_to_upgrade = self.test_spec.num_to_upgrade
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
        topics = self._create_random_topics()
        self.redpanda.logger.info(f"using topics: {topics}")
        self.topic = random.choice(topics).name

    def start_workload(self):
        raise NotImplementedError("Not implemented in child class")

    def end_workload(self):
        raise NotImplementedError("Not implemented in child class")

    def run_fuzzer(self, test_spec: TestSpec):
        self.test_spec = test_spec

        self.start_redpanda()
        assert self.redpanda

        self.start_workload()

        # fuzzer
        self.admin_fuzz = AdminOperationsFuzzer(self.redpanda,
                                                operations_interval=3,
                                                min_replication=3)
        self.admin_fuzz.start()

        self.lock = threading.Lock()
        # optional failure injector
        if self.test_spec.enable_failures:
            self.fi = FailureInjectorBackgroundThread(self.redpanda,
                                                      self.logger, self.lock)
            self.fi.start()

        executor = NodeOpsExecutor(self.redpanda, self.logger, self.lock)
        active_nodes = set([self.redpanda.idx(n) for n in self.redpanda.nodes])
        op_cnt = self.test_spec.node_fuzz_ops
        for i, op in enumerate(
                generate_random_workload(available_nodes=active_nodes)):
            if i >= op_cnt:
                break
            self.logger.info(f"starting operation {i+1}/{op_cnt}")
            executor.execute_operation(op)

        if self.fi:
            self.fi.stop()

        self.admin_fuzz.wait(self.test_spec.admin_fuzz_ops, 180)
        self.admin_fuzz.stop()

        self.end_workload()


class RandomNodeOperationsTest(RandomNodeOperationsTestBase):
    """Random node operations fuzzer with default e2e verifier."""
    def __init__(self, test_context):
        super(RandomNodeOperationsTest,
              self).__init__(test_context=test_context)

    def min_produced_records(self):
        return 20 * self.producer_throughput

    def start_workload(self):
        compacted_topics = self.test_spec.compacted_topics
        self.start_producer(1,
                            throughput=self.producer_throughput,
                            repeating_keys=100 if compacted_topics else None)
        self.start_consumer(1, verify_offsets=not compacted_topics)
        # wait for some records to be produced
        self.await_startup(min_records=3 * self.producer_throughput)

    def end_workload(self):
        compacted_topics = self.test_spec.compacted_topics
        self.run_validation(min_records=self.min_produced_records(),
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

    @skip_debug_mode
    @cluster(num_nodes=7,
             log_allow_list=CHAOS_LOG_ALLOW_LIST + PREV_VERSION_LOG_ALLOW_LIST)
    @matrix(enable_failures=[True, False],
            num_to_upgrade=[0, 3],
            compacted_topics=[True, False])
    def test_node_operations(self, enable_failures, num_to_upgrade,
                             compacted_topics):

        self.producer_timeout = 180
        self.consumer_timeout = 180

        dedicated_nodes = self.test_context.globals.get(
            RedpandaService.DEDICATED_NODE_KEY, False)
        self.logger.info(f"Running with dedicated nodes: {dedicated_nodes}")
        if dedicated_nodes:
            # scale test setup
            max_partitions = 32
            self.producer_throughput = 20000
            node_operations = 30
        else:
            # container test setup
            # skip compacted topics and upgrade tests when running in container
            if compacted_topics or num_to_upgrade > 0:
                cleanup_on_early_exit(self)
                return

            max_partitions = 32
            self.producer_throughput = 1000 if self.debug_mode else 10000
            node_operations = 10

        test_spec = TestSpec(num_brokers=5,
                             enable_failures=enable_failures,
                             num_to_upgrade=num_to_upgrade,
                             compacted_topics=compacted_topics,
                             max_partitions=max_partitions,
                             node_fuzz_ops=node_operations)

        self.run_fuzzer(test_spec=test_spec)
