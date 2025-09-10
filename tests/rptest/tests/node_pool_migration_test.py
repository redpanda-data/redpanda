# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import re
from concurrent.futures import ThreadPoolExecutor
from enum import Enum

import ducktape.errors
import requests
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierConsumerGroupConsumer,
    KgoVerifierProducer,
)
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SISettings
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception
from rptest.utils.mode_checks import cleanup_on_early_exit
from rptest.utils.node_operations import NodeDecommissionWaiter

TS_LOG_ALLOW_LIST = [
    re.compile("archival_metadata_stm.*Replication wait for archival STM timed out"),
]


class TestMode(str, Enum):
    NO_TIRED_STORAGE = "no_tiered_storage"
    TIRED_STORAGE = "tiered_storage"
    FAST_MOVES = "tiered_storage_fast_moves"

    @property
    def has_tiered_storage(self):
        return self.value == self.TIRED_STORAGE or self.value == self.FAST_MOVES


class NodePoolMigrationTestBase(PreallocNodesTest):
    def __init__(self, *args, **kwargs):
        self._topic = None
        super(NodePoolMigrationTestBase, self).__init__(*args, **kwargs)

    def setup(self):
        # defer starting redpanda to test body
        pass

    @property
    def admin(self):
        # retry on timeout and service unavailable
        return Admin(self.redpanda, retry_codes=[503, 504])

    def _create_topics(self, replication_factors=[1, 3]):
        """
        :return: total number of partitions in all topics
        """
        total_partitions = 0
        topics = []
        for i in range(10):
            partitions = random.randint(1, 10)
            spec = TopicSpec(
                name=f"migration-test-{i}",
                partition_count=partitions,
                replication_factor=random.choice(replication_factors),
            )
            topics.append(spec)
            total_partitions += partitions

        for spec in topics:
            self.client().create_topic(spec)

        return total_partitions

    def _create_workload_topic(self, cleanup_policy):
        spec = TopicSpec(
            name="migration-test-workload",
            partition_count=8,
            replication_factor=3,
            cleanup_policy=cleanup_policy,
            segment_bytes=self.segment_size,
        )

        self.client().create_topic(spec)
        self._topic = spec.name

    # after node was removed the state should be consistent on all other not removed nodes
    def _check_state_consistent(self, decommissioned_id):
        not_decommissioned = [
            n
            for n in self.redpanda.started_nodes()
            if self.redpanda.node_id(n) != decommissioned_id
        ]

        def _state_consistent():
            for n in not_decommissioned:
                cfg_status = self.admin.get_cluster_config_status(n)
                brokers = self.admin.get_brokers(n)
                config_ids = sorted([s["node_id"] for s in cfg_status])
                brokers_ids = sorted([b["node_id"] for b in brokers])
                self.logger.info(f"brokers: {brokers_ids}, from config: {config_ids}")
                if brokers_ids != config_ids:
                    return False
                if decommissioned_id in brokers_ids:
                    return False

            return True

        wait_until(
            _state_consistent,
            90,
            1,
            "Error waiting for all the nodes to report consistent list of brokers in the cluster health and configuration.",
        )

    def _wait_for_node_removed(self, node_id, decommissioned_ids):
        waiter = NodeDecommissionWaiter(
            self.redpanda,
            node_id,
            self.logger,
            progress_timeout=120,
            decommissioned_node_ids=decommissioned_ids,
        )
        waiter.wait_for_removal()
        return True

    def _wait_for_nodes_removed(self, decommissioned_ids):
        with ThreadPoolExecutor(max_workers=len(decommissioned_ids)) as executor:
            result = executor.map(
                lambda id: self._wait_for_node_removed(id, decommissioned_ids),
                decommissioned_ids,
            )

            return [r for r in result]

    def _decommission(self, node_id, decommissioned_ids=[]):
        def decommissioned():
            try:
                results = []
                for n in self.redpanda.nodes:
                    # do not query decommissioned nodes
                    if self.redpanda.node_id(n) in decommissioned_ids:
                        continue

                    brokers = self.admin.get_brokers(node=n)
                    for b in brokers:
                        if b["node_id"] == node_id:
                            results.append(b["membership_status"] == "draining")

                if all(results):
                    return True

                self.admin.decommission_broker(node_id)
            except requests.exceptions.RetryError:
                return False
            except requests.exceptions.ConnectionError:
                return False
            except requests.exceptions.HTTPError:
                return False

        wait_until(
            decommissioned,
            30,
            1,
            err_msg=f"Timeout waiting for node {node_id} to start decommissioning",
        )

    @property
    def msg_size(self):
        return 4096

    @property
    def msg_count(self):
        return int(100 if self.debug_mode else 1000 * self.segment_size / self.msg_size)

    @property
    def producer_throughput(self):
        return 1024 if self.debug_mode else 50 * 1024 * 1024

    @property
    def segment_size(self):
        return 1024 * 1024

    @property
    def local_retention_bytes(self):
        return 4 * self.segment_size

    def start_producer(self):
        self.logger.info(
            f"starting kgo-verifier producer with {self.msg_count} messages of size {self.msg_size} and throughput: {self.producer_throughput} bps"
        )
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self._topic,
            self.msg_size,
            self.msg_count,
            key_set_cardinality=10000,
            rate_limit_bps=self.producer_throughput,
            custom_node=self.preallocated_nodes,
            debug_logs=True,
        )

        self.producer.start(clean=False)

        wait_until(
            lambda: self.producer.produce_status.acked > 10,
            timeout_sec=120,
            backoff_sec=1,
        )

    def start_consumer(self):
        self.consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            self._topic,
            self.msg_size,
            readers=1,
            nodes=self.preallocated_nodes,
        )
        self.consumer.start(clean=False)

    def verify(self):
        self.logger.info(
            f"verifying workload: topic: {self._topic}, with [rate_limit: {self.producer_throughput}, message size: {self.msg_size}, message count: {self.msg_count}]"
        )
        self.producer.wait()

        # Await the consumer that is reading only the subset of data that
        # was written before it started.
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.invalid_reads == 0, (
            f"Invalid reads in topic: {self._topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"
        )
        del self.consumer

        # Start a new consumer to read all data written
        self.start_consumer()
        self.consumer.wait()

        assert self.consumer.consumer_status.validator.invalid_reads == 0, (
            f"Invalid reads in topic: {self._topic}, invalid reads count: {self.consumer.consumer_status.validator.invalid_reads}"
        )

    def _replicas_per_node(self):
        kafkacat = KafkaCat(self.redpanda)
        node_replicas = {}
        md = kafkacat.metadata()
        self.redpanda.logger.debug(f"metadata: {md}")
        for topic in md["topics"]:
            for p in topic["partitions"]:
                for r in p["replicas"]:
                    id = r["id"]
                    if id not in node_replicas:
                        node_replicas[id] = 0
                    node_replicas[id] += 1

        return node_replicas


class NodePoolMigrationTest(NodePoolMigrationTestBase):
    """
    Basic nodes decommissioning test.
    """

    def __init__(self, test_context):
        self._topic = None

        super(NodePoolMigrationTest, self).__init__(
            test_context=test_context,
            num_brokers=10,
            node_prealloc_count=1,
            si_settings=SISettings(
                test_context,
                cloud_storage_enable_remote_read=True,
                cloud_storage_enable_remote_write=True,
                fast_uploads=True,
            ),
        )

    def setup(self):
        # defer starting redpanda to test body
        pass

    @cluster(num_nodes=11, log_allow_list=RESTART_LOG_ALLOW_LIST + TS_LOG_ALLOW_LIST)
    @matrix(
        balancing_mode=["off", "node_add"],
        test_mode=[
            TestMode.NO_TIRED_STORAGE,
            TestMode.TIRED_STORAGE,
            TestMode.FAST_MOVES,
        ],
        cleanup_policy=["compact", "compact,delete"],
    )
    def test_migrating_redpanda_nodes_to_new_pool(
        self, balancing_mode, test_mode: TestMode, cleanup_policy
    ):
        """
        This test executes migration of 3 nodes redpanda cluster from one
        set of nodes to the other, during this operation nodes from target pool
        are first added to the cluster and then the old pool of nodes is decommissioned.
        """

        if self.debug_mode:
            self.redpanda._si_settings = None
            cleanup_on_early_exit(self)
            return

        initial_pool = self.redpanda.nodes[0:5]
        new_pool = self.redpanda.nodes[5:]

        self.redpanda.set_seed_servers(initial_pool)

        # start redpanda on initial pool of nodes
        self.redpanda.start(
            nodes=initial_pool, auto_assign_node_id=True, omit_seeds_on_idx_one=False
        )

        cfg = {"partition_autobalancing_mode": balancing_mode}
        if test_mode.has_tiered_storage:
            cfg["cloud_storage_enable_remote_write"] = True
            cfg["cloud_storage_enable_remote_read"] = True
            # we want data to be actually deleted
            cfg["retention_local_strict"] = True

        if test_mode == TestMode.FAST_MOVES:
            self.redpanda.set_cluster_config(
                {"initial_retention_local_target_bytes_default": 3 * self.segment_size}
            )

        self.admin.patch_cluster_config(upsert=cfg)

        self._create_topics()

        self._create_workload_topic(cleanup_policy=cleanup_policy)
        if test_mode.has_tiered_storage:
            rpk = RpkTool(self.redpanda)
            rpk.alter_topic_config(
                self._topic,
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
                self.local_retention_bytes,
            )

        self.start_producer()
        self.start_consumer()

        # wait for some messages before executing actions (50 segments)
        self.producer.wait_for_acks(
            50 * self.segment_size // self.msg_size, timeout_sec=60, backoff_sec=2
        )
        # add new nodes to the cluster
        self.redpanda.for_nodes(
            new_pool, lambda n: self.redpanda.start_node(n, auto_assign_node_id=True)
        )

        def all_nodes_present():
            for n in self.redpanda.nodes:
                brokers = self.admin.get_brokers(node=n)
                if len(brokers) != len(initial_pool) + len(new_pool):
                    self.logger.info(
                        f"Node: {n.account.hostname}(node_id: {self.redpanda.node_id(n)}) contains {len(brokers)} while we expect it to have {len(initial_pool) + len(new_pool)} brokers"
                    )
                    return False
            return True

        wait_until(
            all_nodes_present,
            60,
            1,
            err_msg="Not all nodes that were supposed to join the cluster are members",
        )
        decommissioned_ids = [
            self.redpanda.node_id(to_decommission) for to_decommission in initial_pool
        ]

        for to_decommission_id in decommissioned_ids:
            self.logger.info(
                f"decommissioning node: {to_decommission_id}",
            )
            self._decommission(
                to_decommission_id, decommissioned_ids=decommissioned_ids
            )

        self._wait_for_nodes_removed(decommissioned_ids)

        def _all_nodes_balanced():
            r_per_node = self._replicas_per_node()
            self.logger.info(f"finished with {r_per_node} replicas per node")
            total_replicas = sum([r for r in r_per_node.values()])
            tolerance = total_replicas * 0.1
            min_expected = total_replicas / len(new_pool) - tolerance
            max_expected = total_replicas / len(new_pool) + tolerance

            return all([min_expected <= v <= max_expected for v in r_per_node.values()])

        wait_until(_all_nodes_balanced, 60, 1, "Partitions are not balanced correctly")

        def _quiescent_state():
            pb_status = self.admin.get_partition_balancer_status(
                node=random.choice(new_pool)
            )
            reconfigurations = self.admin.list_reconfigurations(
                node=random.choice(new_pool)
            )
            return (
                len(reconfigurations) == 0
                and pb_status["status"] == "ready"
                or pb_status["status"] == "off"
            )

        wait_until(
            _quiescent_state,
            120,
            1,
            "Cluster reached quiescent state (no partition movement)",
            retry_on_exc=True,
        )

        for n in initial_pool:
            self.redpanda.stop_node(n)

        self.verify()


class DisableTestMode(str, Enum):
    DISABLE = "disable tiered storage"
    PAUSE = "pause uploads"

    def do_disable(self, test: RedpandaTest, topic_name: str):
        if self.value == self.DISABLE:
            test.client().alter_topic_config(
                topic_name, "redpanda.remote.read", "false"
            )
            test.client().alter_topic_config(
                topic_name, "redpanda.remote.write", "false"
            )
        elif self.value == self.PAUSE:
            test.client().alter_topic_config(
                topic_name, "redpanda.remote.allowgaps", "true"
            )
            test.redpanda.set_cluster_config(
                {"cloud_storage_enable_segment_uploads": False}
            )


class DisableTieredStorageTest(NodePoolMigrationTestBase):
    def __init__(self, test_context):
        self._topic = None

        super(DisableTieredStorageTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            node_prealloc_count=1,
            si_settings=SISettings(
                test_context,
                cloud_storage_enable_remote_read=True,
                cloud_storage_enable_remote_write=True,
                fast_uploads=True,
            ),
        )

    def setup(self):
        # defer starting redpanda to test body
        pass

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST + TS_LOG_ALLOW_LIST)
    @matrix(
        disable_mode=[
            DisableTestMode.DISABLE,
            DisableTestMode.PAUSE,
        ]
    )
    def test_disable_tiered_storage(self, disable_mode: DisableTestMode):
        """
        This test performs the following actions:
          - Create a tiered storage topic
          - Produce some data and wait for cloud storage upload
          - Disable tiered storage on the topic
          - Produce some more data (note no additional upload)
          - Decommission leader to force leadership transfer
          - Check that start offset and high watermark on the new leader reflect
            the full content of the original leader's raft log prior to decom.
        """

        self.redpanda.start()
        cfg = {"partition_autobalancing_mode": "node_add"}
        cfg["cloud_storage_enable_remote_write"] = True
        cfg["cloud_storage_enable_remote_read"] = True
        # we want data to be actually deleted
        cfg["retention_local_strict"] = True

        # we need to configure a small amount of initial local retention,
        # otherwise we get the hwm, batch boundary adjustment fails, and we
        # fall back to  setting the learner to start at offset 0
        self.redpanda.set_cluster_config(
            {"initial_retention_local_target_bytes_default": self.segment_size * 2}
        )

        self.admin.patch_cluster_config(upsert=cfg)

        spec = TopicSpec(
            name="migration-test",
            partition_count=1,
            replication_factor=1,
            cleanup_policy="compact",
            segment_bytes=self.segment_size,
        )
        self.client().create_topic(spec)
        self._topic = spec.name
        rpk = RpkTool(self.redpanda)

        def describe_topic():
            info = None
            while info is None:
                for i in rpk.describe_topic(spec.name):
                    info = i
            self.logger.debug(f"{info}")
            return info

        self.start_producer()
        self.producer.wait(timeout_sec=60)

        info = describe_topic()

        initial_start_offset = info.start_offset
        initial_hwm = info.high_watermark

        def pm_last_offset():
            v = self.admin.get_partition_manifest(spec.name, 0)["last_offset"]
            return v

        self.logger.debug("Wait until most of the topic is uploaded")

        wait_until(
            lambda: pm_last_offset() >= initial_hwm,
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Partition never uploaded",
        )

        self.logger.debug(
            f"Now {disable_mode} and produce some more to put HWM well above the last uploaded offset"
        )
        disable_mode.do_disable(self, spec.name)

        last_uploaded = pm_last_offset()

        self.start_producer()
        self.producer.wait(timeout_sec=60)

        info = describe_topic()
        second_hwm = info.high_watermark

        assert pm_last_offset() == last_uploaded, (
            f"Unexpectedly uploaded more data {pm_last_offset()} > {last_uploaded}"
        )

        self.logger.debug(
            "Decommission the partition's leader and wait for leadership transfer"
        )

        leader_id = self.admin.get_partition_leader(
            namespace="kafka", topic=spec.name, partition=0
        )

        self._decommission(leader_id, decommissioned_ids=[leader_id])

        def new_leader_id():
            partition_info = self.admin.get_partitions(
                topic=spec.name, partition=0, namespace="kafka", node=None
            )
            self.logger.debug(f"{partition_info=}")
            new_id = self.admin.get_partition_leader(
                namespace="kafka", topic=spec.name, partition=0
            )
            self.logger.debug(f"{new_id=}")
            return new_id

        wait_until(
            lambda: new_leader_id() not in [leader_id, -1],
            timeout_sec=60,
            backoff_sec=2,
            err_msg="Partition didn't move",
        )

        if disable_mode == DisableTestMode.DISABLE:
            self.logger.debug(
                "With tiered storage disabled, we should skip FPM truncation and transfer the whole log via raft"
            )
        elif disable_mode == DisableTestMode.PAUSE:
            self.logger.debug(
                "With uploads paused, FPM should truncate only up to the last uploaded offset to avoid introducing a gap in the log"
            )

        with expect_exception(ducktape.errors.TimeoutError, lambda e: True):
            wait_until(
                lambda: describe_topic().start_offset > initial_start_offset,
                timeout_sec=30,
                backoff_sec=2,
                err_msg="Start offset never jumped",
            )

        final_start_offset = describe_topic().start_offset
        final_hwm = describe_topic().high_watermark

        assert final_start_offset == initial_start_offset, (
            f"Expected final_start_offset == {initial_start_offset}, got {final_start_offset=}"
        )

        assert final_hwm == second_hwm, (
            f"Expected final_hwm == {second_hwm}, got {final_hwm=}"
        )
