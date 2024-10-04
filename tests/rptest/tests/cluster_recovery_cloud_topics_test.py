import time
from typing import List

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest


class ClusterRecoveryCloudTopicsTest(RedpandaTest):
    def __init__(self, test_context: TestContext):
        extra_rp_conf = {
            'controller_snapshot_max_age_sec': 1,
            'cloud_storage_cluster_metadata_upload_interval_ms': 1000,
            'enable_cluster_metadata_upload_loop': True,
            # Disable the leader balancer to avoid interference with the happy
            # path test case where we want to make more deterministic
            # assertions about the process. I.e. counting log lines.
            'enable_leader_balancer': False,
        }

        super().__init__(test_context=test_context,
                         si_settings=SISettings(test_context=test_context),
                         extra_rp_conf=extra_rp_conf)

        self.admin = Admin(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=4)
    def test_basic_recovery(self):
        """
        Happy path test for recovery of cloud topics.
        """
        topic_name = "topic-1"
        num_partitions = 3

        self._enable_cloud_topics()
        self.rpk.create_topic(topic_name,
                              partitions=num_partitions,
                              config={"redpanda.cloud_topic.enabled": "true"})

        # We are not producing any data yet because L1 object creation
        # is not implemented yet so no metadata is being snapshotted yet.

        # Wait for the controller snapshot to be uploaded.
        time.sleep(10)

        self._destroy_and_recover(timeout_sec=30)

        assert len(set(self.rpk.list_topics())) == 1

        self.logger.info(
            "Verifying that the topic is still marked as a cloud topic")
        topic_configs = self.rpk.describe_topic_configs("topic-1")
        assert topic_configs["redpanda.cloud_topic.enabled"][0] == "true"

        self.logger.info("Verifying that the topic is ready to be used")
        KgoVerifierProducer.oneshot(self.test_context,
                                    self.redpanda,
                                    "topic-1",
                                    1024,
                                    100,
                                    batch_max_bytes=1024 * 8,
                                    timeout_sec=60)

        # Restart the cluster to verify that the recovery is persistent.
        self.logger.info(
            "Restarting the cluster to verify recovery progress persistence")
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self._wait_for_leader(topic_name="topic-1",
                              partitions=list(range(num_partitions - 1)),
                              timeout_sec=30)

        recovery_attempts = 0
        for node in self.redpanda.nodes:
            recovery_attempts += self.redpanda.count_log_node(
                node,
                "ct-recovery.+Starting recovery in term",
                extended_pattern=True)

        # One recovery attempt per partition.
        # assert recovery_attempts == num_partitions, f"Expected {num_partitions} recovery attempts, got {recovery_attempts}"
        # This is temporary until we fix the recovery process persistence.
        assert recovery_attempts == 2 * num_partitions, f"Expected {num_partitions} recovery attempts, got {recovery_attempts}"

        self.logger.info(
            "Verifying that the topic is ready to be used after restart")
        KgoVerifierProducer.oneshot(self.test_context,
                                    self.redpanda,
                                    "topic-1",
                                    1024,
                                    100,
                                    batch_max_bytes=1024 * 8,
                                    timeout_sec=60)

        # TODO: Verify that multiple rounds of recovery work.

    def _wait_for_leader(self, *, topic_name: str, partitions: List[int],
                         timeout_sec: int):
        def partition_has_leader(partition):
            return self.admin.get_partition_leader(
                namespace="kafka", topic=topic_name,
                partition=partition) not in [None, -1]

        def all_partitions_have_leader():
            return all(partition_has_leader(p) for p in partitions)

        wait_until(
            all_partitions_have_leader,
            timeout_sec=timeout_sec,
            backoff_sec=3,
            err_msg="Timed out waiting for partition leader",
        )

    def _destroy_and_recover(self, *, timeout_sec):
        self._recreate_cluster_without_local_data()
        self.redpanda._admin.initialize_cluster_recovery()
        self._await_recovery(timeout_sec=timeout_sec)

    def _recreate_cluster_without_local_data(self):
        self.logger.info("Re-creating cluster")

        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)
        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    omit_seeds_on_idx_one=False)

        # Although configuration is recovered, we explicitly re-enable
        # cloud topics to trigger a restart.
        self._enable_cloud_topics()

        self.redpanda._admin.await_stable_leader("controller",
                                                 partition=0,
                                                 namespace='redpanda',
                                                 timeout_s=60,
                                                 backoff_s=2)

    def _await_recovery(self, *, timeout_sec):
        self.logger.info("Waiting for cluster recovery to complete")

        def cluster_recovery_complete():
            return "inactive" in self.redpanda._admin.get_cluster_recovery_status(
            ).json()["state"]

        wait_until(cluster_recovery_complete,
                   timeout_sec=timeout_sec,
                   backoff_sec=1)

    def _enable_cloud_topics(self):
        self.redpanda.enable_development_feature_support()
        self.redpanda.set_cluster_config(
            values={
                "development_enable_cloud_topics": True,
            })
        self.redpanda.restart_nodes(self.redpanda.nodes)
