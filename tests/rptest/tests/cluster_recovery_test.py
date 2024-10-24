# Copyright 2023 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import random
import string
import time
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.services.redpanda import SISettings
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.si_utils import quiesce_uploads

KiB = 1024
MiB = KiB * KiB


def random_string(length):
    return ''.join(
        [random.choice(string.ascii_lowercase) for i in range(0, length)])


class ClusterRecoveryTest(RedpandaTest):
    segment_size = 1 * MiB
    message_size = 16 * KiB
    topics = [
        TopicSpec(name=f"topic-{n}",
                  partition_count=3,
                  replication_factor=1,
                  redpanda_remote_write=True,
                  redpanda_remote_read=True,
                  retention_bytes=-1,
                  retention_ms=-1,
                  segment_bytes=1 * MiB) for n in range(3)
    ]

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(test_context,
                                 log_segment_size=self.segment_size,
                                 fast_uploads=True)
        extra_rp_conf = {
            'controller_snapshot_max_age_sec': 1,
            'cloud_storage_cluster_metadata_upload_interval_ms': 1000,
            'enable_cluster_metadata_upload_loop': True,
        }
        si_settings = SISettings(
            test_context,
            log_segment_size=self.segment_size,
            fast_uploads=True,
        )
        self.s3_bucket = si_settings.cloud_storage_bucket
        super(ClusterRecoveryTest, self).__init__(test_context=test_context,
                                                  si_settings=si_settings,
                                                  extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=4)
    def test_basic_controller_snapshot_restore(self):
        """
        Tests that recovery of some fixed pieces of controller metadata get
        restored by cluster recovery.
        """
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("log_segment_size_max", 1000000)

        for t in self.topics:
            KgoVerifierProducer.oneshot(self.test_context,
                                        self.redpanda,
                                        t.name,
                                        self.message_size,
                                        100,
                                        batch_max_bytes=self.message_size * 8,
                                        timeout_sec=60)
        quiesce_uploads(self.redpanda, [t.name for t in self.topics],
                        timeout_sec=60)

        algorithm = "SCRAM-SHA-256"
        users = dict()
        users["admin"] = None  # Created by the RedpandaService.
        for _ in range(3):
            user = f'user-{random_string(6)}'
            password = f'user-{random_string(6)}'
            users[user] = password
            self.redpanda._admin.create_user(user, password, algorithm)
            rpk.acl_create_allow_cluster(user, op="describe")
        rpk.acl_create_allow_cluster("admin", op="describe")

        time.sleep(5)

        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)
        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    auto_assign_node_id=True,
                                    omit_seeds_on_idx_one=False)
        self.redpanda._admin.await_stable_leader("controller",
                                                 partition=0,
                                                 namespace='redpanda',
                                                 timeout_s=60,
                                                 backoff_s=2)

        self.redpanda._admin.initialize_cluster_recovery()

        def cluster_recovery_complete():
            return "inactive" in self.redpanda._admin.get_cluster_recovery_status(
            ).json()["state"]

        wait_until(cluster_recovery_complete, timeout_sec=30, backoff_sec=1)

        assert len(set(
            rpk.list_topics())) == 3, "Incorrect number of topics restored"
        segment_size_max_restored = rpk.cluster_config_get(
            "log_segment_size_max")
        assert "1000000" == segment_size_max_restored, f"1000000 vs {segment_size_max_restored}"
        restored_users = self.redpanda._admin.list_users()
        assert set(restored_users) == set(
            users.keys()), f"{restored_users} vs {users.keys()}"

        acls = rpk.acl_list()
        acls_lines = acls.splitlines()
        for u in users:
            found = False
            for l in acls_lines:
                if u in l and "ALLOW" in l and "DESCRIBE" in l:
                    found = True
            assert found, f"Couldn't find {u} in {acls_lines}"

    @cluster(num_nodes=4)
    def test_bootstrap_with_recovery(self):
        """
        Smoke test that configuring automated recovery at bootstrap will kick
        in as appropriate.
        """
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set(
            "cloud_storage_attempt_cluster_restore_on_bootstrap", True)
        for t in self.topics:
            KgoVerifierProducer.oneshot(self.test_context,
                                        self.redpanda,
                                        t.name,
                                        self.message_size,
                                        100,
                                        batch_max_bytes=self.message_size * 8,
                                        timeout_sec=60)
        quiesce_uploads(self.redpanda, [t.name for t in self.topics],
                        timeout_sec=60)
        time.sleep(5)

        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)

        # Restart the nodes, overriding the recovery bootstrap config.
        extra_rp_conf = dict(
            cloud_storage_attempt_cluster_restore_on_bootstrap=True)
        self.redpanda.set_extra_rp_conf(extra_rp_conf)
        self.redpanda.write_bootstrap_cluster_config()
        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    override_cfg_params=extra_rp_conf)

        # We should see a recovery begin automatically.
        self.redpanda._admin.await_stable_leader("controller",
                                                 partition=0,
                                                 namespace='redpanda',
                                                 timeout_s=60,
                                                 backoff_s=2)

        def cluster_recovery_complete():
            return "inactive" in self.redpanda._admin.get_cluster_recovery_status(
            ).json()["state"]

        wait_until(cluster_recovery_complete, timeout_sec=60, backoff_sec=1)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        assert len(set(rpk.list_topics())) == len(
            self.topics), "Incorrect number of topics restored"
