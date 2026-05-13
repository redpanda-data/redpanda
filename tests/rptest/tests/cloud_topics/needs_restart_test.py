# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.cluster.cluster import ClusterNode
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import CLOUD_TOPICS_CONFIG_STR, SISettings
from rptest.tests.cluster_config_test import wait_for_version_status_sync
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_timeout, wait_until_result


class CloudTopicNeedsRestartTest(RedpandaTest):
    """
    cloud_topics_enabled is a needs_restart=yes property. A partial restart
    after enabling it leaves the cluster with the property in active on some
    nodes and only in pending on others. Cloud topic creation goes through the
    restarted controller leader and succeeds, but partition_manager on the
    unrestarted nodes must refuse to materialize the replica (no ctp_stm) until
    those nodes restart. This test exercises the node-local validator added in
    controller_backend::create_partition.
    """

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context, num_brokers=3, si_settings=SISettings(test_context)
        )
        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    def setUp(self):
        # Skip starting redpanda, so that the test can explicitly start
        # it with some override_cfg_params.
        pass

    def _local_replica_stms(
        self, node: ClusterNode, topic_name: str, partition: int
    ) -> set[str] | None:
        """
        Return the set of stm names registered for `topic_name`/`partition` on
        `node`'s local replica, or None if the replica has not materialized yet.
        Suitable for passing to wait_until_result.
        """
        node_id = self.redpanda.node_id(node)
        state = self.admin.get_partition_state(
            "kafka", topic_name, partition, node=node
        )
        for r in state.get("replicas", []):
            if r.get("raft_state", {}).get("node_id") == node_id:
                return {s["name"] for s in r["raft_state"].get("stms", [])}
        return None

    @cluster(num_nodes=3)
    def test_cloud_topic_with_partial_restart(self):
        all_nodes = self.redpanda.nodes
        self.redpanda.start(all_nodes)
        wait_until(
            lambda: len(self.admin.get_cluster_config_status()) == 3,
            timeout_sec=30,
            backoff_sec=1,
        )

        original_leader_id = self.admin.await_stable_leader(
            "controller",
            partition=0,
            namespace="redpanda",
            timeout_s=60,
            backoff_s=2,
        )
        original_leader = self.redpanda.get_node(original_leader_id)
        other_nodes = [n for n in all_nodes if n is not original_leader]

        # Enable cloud_topics_enabled cluster-wide. All three nodes
        # should observe restart=true since this is a
        # needs_restart=yes property.
        new_setting = (CLOUD_TOPICS_CONFIG_STR, True)
        patch_result = self.admin.patch_cluster_config(upsert=dict([new_setting]))
        new_version = patch_result["config_version"]
        wait_for_version_status_sync(
            self.admin, self.redpanda, new_version, nodes=all_nodes
        )
        status = self.admin.get_cluster_config_status()
        for s in status:
            assert s["restart"] is True, (
                f"Expected all nodes to need restart, got {status}"
            )

        # Restart ONLY the original controller leader. The other
        # two nodes intentionally stay with cloud_topics_enabled
        # pending.
        self.redpanda.restart_nodes(original_leader)
        wait_until(
            lambda: any(
                s["restart"] is False
                for s in self.admin.get_cluster_config_status()
                if s["node_id"] == original_leader_id
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="restart flag did not clear on restarted controller leader",
        )

        # Transfer controller leadership back to the (now
        # restarted) original leader so it serves the cloud topic
        # create RPC and the active-config validation passes there.
        self.admin.partition_transfer_leadership(
            namespace="redpanda",
            topic="controller",
            partition=0,
            target_id=original_leader_id,
        )
        new_leader_id = self.admin.await_stable_leader(
            "controller",
            partition=0,
            namespace="redpanda",
            timeout_s=60,
            backoff_s=2,
        )
        assert new_leader_id == original_leader_id, (
            f"Expected controller leadership back on node "
            f"{original_leader_id}, got {new_leader_id}"
        )

        # Create a cloud topic with rf=3 so every node hosts a
        # replica.
        topic_name = "tapioca_partial"
        self.rpk.create_topic(
            topic=topic_name,
            partitions=1,
            replicas=3,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
            },
        )

        # The restarted leader has cloud_topics_enabled in active,
        # so its replica should have ctp_stm registered.
        leader_stms = wait_until_result(
            lambda: self._local_replica_stms(original_leader, topic_name, 0),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"{topic_name} replica never materialized on "
            f"restarted controller leader {original_leader_id}",
        )
        assert "ctp_stm" in leader_stms, (
            f"ctp_stm missing on restarted controller leader "
            f"{original_leader_id}; got stms {leader_stms}"
        )

        # The unrestarted nodes still have cloud_topics_enabled
        # only in pending, so the partition_manager should refuse to manage the partition.
        for n in other_nodes:
            n_id = self.redpanda.node_id(n)
            with expect_timeout():
                wait_until_result(
                    lambda node=n: self._local_replica_stms(node, topic_name, 0),
                    timeout_sec=10,
                    backoff_sec=1,
                    err_msg=f"{topic_name} replica never materialized on "
                    f"unrestarted node {n_id}",
                )

        # Now restart the remaining nodes so cloud_topics_enabled moves
        # from pending to active there too. The reconciliation loop in
        # controller_backend will retry create_partition; this time the
        # validator passes and the partition_manager materializes the
        # replica with ctp_stm registered.
        self.redpanda.restart_nodes(other_nodes)
        wait_until(
            lambda: all(
                s["restart"] is False for s in self.admin.get_cluster_config_status()
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="restart flag did not clear on all nodes after restart",
        )

        for n in other_nodes:
            n_id = self.redpanda.node_id(n)
            stm_names = wait_until_result(
                lambda node=n: self._local_replica_stms(node, topic_name, 0),
                timeout_sec=30,
                backoff_sec=1,
                err_msg=f"{topic_name} replica never materialized on "
                f"restarted node {n_id}",
            )
            assert "ctp_stm" in stm_names, (
                f"ctp_stm missing on restarted node {n_id}; got stms {stm_names}"
            )
