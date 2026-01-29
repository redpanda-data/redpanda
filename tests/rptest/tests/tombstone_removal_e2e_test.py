# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from typing import Any

import requests

from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.partition_movement import PartitionMovementMixin
from ducktape.tests.test import TestContext
from ducktape.cluster.cluster import ClusterNode
from rptest.clients.offline_log_viewer import OfflineLogViewer
from ducktape.cluster.remoteaccount import RemoteCommandError


class TombstoneRemovalTest(RedpandaTest):
    topic_name = "tr_test_topic"
    data_msg_count = 256 * 1024
    data_msg_size = 1024

    def __init__(self, test_context: TestContext):
        extra_rp_conf = dict(
            enable_leader_balancer=False,
            partition_autobalancing_mode="off",
            health_monitor_max_metadata_age=100,  # ms
            raft_learner_recovery_rate=10 * 1024 * 1024,  # 10MB/s
            tombstone_retention_ms=1000,  # 1 second
        )
        super(TombstoneRemovalTest, self).__init__(
            num_brokers=5,
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
        )

    def produce_data(self, key_set_cardinality: int):
        # keys produced by KgoVerifierProducer are
        # "key-0", "key-1", ... f"key-{key_set_cardinality - 1}"
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size=self.data_msg_size,
            msg_count=self.data_msg_count,
            key_set_cardinality=key_set_cardinality,
            tombstone_probability=0.0,
            timeout_sec=100,
        )

    def leadership_transferred_to_node(self, node_id: int) -> bool:
        current_leader = self.admin.get_partition("kafka", self.topic_name, 0)[
            "leader_id"
        ]
        if current_leader == node_id:
            return True
        self.redpanda.logger.debug(
            f"Current leader is {current_leader}, trying to transfer to node {node_id}"
        )
        try:
            self.admin.transfer_leadership_to(
                namespace="kafka",
                topic=self.topic_name,
                partition=0,
                target_id=node_id,
            )
        except requests.exceptions.HTTPError as e:
            self.redpanda.logger.debug(
                f"Failed to transfer leadership to node {node_id}, retrying: {e}"
            )
        return False

    def transfer_leadership_to_node(self, node_id: int):
        wait_until(
            lambda: self.leadership_transferred_to_node(node_id),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"timed out waiting for leadership transfer to node {node_id}",
        )

    def read_kafka_records(self, node: ClusterNode, topic_name: str) -> Any:
        viewer = OfflineLogViewer(self.redpanda)
        while True:
            try:
                return viewer.read_kafka_records(node, self.topic_name)
            except RemoteCommandError as e:
                self.redpanda.logger.debug(
                    f"failed to read kafka records on node {node.name}, retrying: {e}"
                )
                # OfflineLogViewer may fail if log segments are being deleted concurrently
                assert "No such file or directory" in str(e), "unexpected error"
                time.sleep(1)
                continue

    def has_59_data_on_node(self, node: ClusterNode) -> bool:
        """Returns non-tombstone data records for keys 5-9 from given node"""
        partitions = self.read_kafka_records(node, self.topic_name)
        assert len(partitions) == 1, "expected single partition"

        def is_59_data_record(record_or_batch: dict[str, Any]) -> bool:
            return (
                "base_offset" not in record_or_batch
                and record_or_batch["k"] is not None
                and "key-5" <= record_or_batch["k"] <= "key-9"
                and record_or_batch["v"] is not None
            )

        return any(is_59_data_record(rob) for rob in partitions[0])

    # make sure it removed all data batches on the leader
    def get_first_offset(self, node: ClusterNode) -> int:
        partitions = self.read_kafka_records(node, self.topic_name)
        assert len(partitions) == 1, "expected single partition"
        current_offset = None
        for record_or_batch in partitions[0]:
            if "base_offset" in record_or_batch:
                # batch
                current_offset = record_or_batch["base_offset"]
            else:
                # record
                assert current_offset is not None
                if record_or_batch["k"] is not None:
                    self.redpanda.logger.debug(f"first offset is {current_offset}")
                    return current_offset
                current_offset += 1
        self.redpanda.logger.debug("partition is empty")
        return -1

    @cluster(num_nodes=6)  # 5 for cluster + 1 for producer
    def test_single_slow_follower(self):
        """
        This tests produces data for keys 0-9, then tombstones for keys 5-9, then more data for keys 0-4.
        A slow recovering replica is added to the partition while compaction is turned off.
        Make sure it prevents the leader from compacting away tombstones until it catches up.
        Once it catches up, make sure tombstones are removed from both replicas.
        """
        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        node1 = self.redpanda.get_node_by_id(1)
        node4 = self.redpanda.get_node_by_id(4)
        assert node1 is not None and node4 is not None, "node not found"

        # create topic on nodes 1,2,3
        self.client().create_topic_with_assignment(self.topic_name, [[1, 2, 3]])

        # turn off compaction and prefix-truncation
        self.client().alter_topic_config(self.topic_name, "cleanup.policy", "delete")
        self.client().alter_topic_config(self.topic_name, "retention.ms", "-1")
        self.client().alter_topic_config(self.topic_name, "retention.bytes", "-1")

        # produce a lot of data records for keys 0-9
        self.produce_data(10)

        # produce tombstones for keys 5-9 (one for each)
        tombstone_offsets = [
            self.rpk.produce(
                topic=self.topic_name,
                key="key-{}".format(key),
                msg="",
                tombstone=True,
            )
            for key in range(5, 10)
        ]
        offset_after_tombstones = max(tombstone_offsets) + 1
        first_tombstone_offset = min(tombstone_offsets)

        # produce a lot of data records for keys 0-4
        self.produce_data(5)

        # make node 1 the leader
        self.transfer_leadership_to_node(1)

        # init a replica on node 4
        self.admin.set_partition_replicas(
            self.topic_name,
            0,
            [
                {"node_id": n, "core": PartitionMovementMixin.INVALID_CORE}
                for n in [1, 2, 4]
            ],
        )
        wait_until(
            lambda: len(self.admin.list_reconfigurations(node=node1)) > 0,
            timeout_sec=10,
            backoff_sec=1,
            err_msg="timed out waiting for reconfiguration to start",
        )

        # let node 4 recover at least some data but no tombstones
        def data_recovered_halfway() -> bool:
            reconfigurations = self.admin.list_reconfigurations(node=node1)
            self.redpanda.logger.debug(f"reconfigurations: {reconfigurations}")
            if not reconfigurations:
                return False
            bytes_moved = reconfigurations[0]["bytes_moved"]
            total_bytes_to_move = self.data_msg_count * self.data_msg_size
            assert bytes_moved <= total_bytes_to_move * 0.75, "node recovered too fast"
            return bytes_moved >= total_bytes_to_move * 0.25

        # (256*1024*1024) / (10*1024*1024) ~= 26 seconds in ideal circumstances
        wait_until(
            data_recovered_halfway,
            timeout_sec=120,
            backoff_sec=1,
            err_msg="timed out waiting for partial recovery",
        )

        with self.redpanda.paused_node(node4):
            # unrestrict recovery rate
            self.redpanda.set_cluster_config(
                {"raft_learner_recovery_rate": 500 * (2**20)},
                tolerate_stopped_nodes=True,
            )

            # turn on compaction
            self.rpk.alter_topic_config(self.topic_name, "cleanup.policy", "compact")
            self.rpk.alter_topic_config(
                self.topic_name, "min.cleanable.dirty.ratio", "0.0"
            )

            # make sure compaction on node 1 removes data before tombstones
            wait_until(
                lambda: self.get_first_offset(node1) >= first_tombstone_offset,
                timeout_sec=60,
                backoff_sec=1,
                err_msg="compaction did not remove before 5-9 tombstones on node1",
            )
            # but make sure node 1 is not compacting away tombstones until node 4
            # catches up and compacts away correspondent data records first
            time.sleep(10)
            tombstones_incorrectly_removed = (
                self.get_first_offset(node1) >= offset_after_tombstones
            )
            if tombstones_incorrectly_removed:
                # We don't assert here right away for the rest of the test
                # to draw replicas into inconsistent state, to indicate the severity of the failure
                self.logger.error(
                    "coordinated compaction did not prevent tombstone deletion on node 1"
                )

        # make node 4 the leader, thus making sure it caught up
        self.transfer_leadership_to_node(4)

        # wait until node 4 pre-tombstones data is compacted
        wait_until(
            lambda: self.get_first_offset(node4) >= first_tombstone_offset,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="compaction did not remove before 5-9 tombstones on node4",
        )

        assert not self.has_59_data_on_node(node1), (
            "expected data with keys 5-9 to be fully compacted away on node 1"
        )
        assert not self.has_59_data_on_node(node4), (
            "expected data with keys 5-9 to be fully compacted away on node 4"
        )

        # make sure node 4 tombstones are also removed
        wait_until(
            lambda: self.get_first_offset(node4) >= offset_after_tombstones,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="timed out waiting for compaction to remove all tombstones on node 4",
        )

        # check node 1 tombstones are also removed
        wait_until(
            lambda: self.get_first_offset(node1) >= offset_after_tombstones,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="timed out waiting for compaction to remove all tombstones on node 1",
        )

        assert not tombstones_incorrectly_removed, (
            "abnormal compaction detected, see error in logs above"
        )
