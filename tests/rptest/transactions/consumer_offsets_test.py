# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.services.cluster import cluster
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.tests.redpanda_test import RedpandaTest
from rptest.transactions.verifiers.consumer_offsets_verifier import (
    ConsumerOffsetsVerifier,
)


class VerifyConsumerOffsets(RedpandaTest):
    def __init__(self, test_context):
        super(VerifyConsumerOffsets, self).__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf={
                "group_topic_partitions": 1,
                "log_segment_size": 1024 * 1024,
                "log_segment_ms": 60000,
                "log_compaction_interval_ms": 10,
                "group_new_member_join_timeout": 3000,
                "group_initial_rebalance_delay": 0,
            },
        )

    @cluster(num_nodes=3)
    def test_consumer_group_offsets(self):
        verifier = ConsumerOffsetsVerifier(self.redpanda, self._client)
        verifier.verify()


class VerifyConsumerOffsetsThruUpgrades(RedpandaTest):
    def __init__(self, test_context):
        super(VerifyConsumerOffsetsThruUpgrades, self).__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf={
                "group_topic_partitions": 1,
                "log_segment_size": 1024 * 1024,
                "log_segment_ms": 60000,
                "log_compaction_interval_ms": 10,
                "group_new_member_join_timeout": 3000,
                "group_initial_rebalance_delay": 0,
            },
        )

    def rp_install_version(self, num_previous: int, version=RedpandaInstaller.HEAD):
        if num_previous == 0:
            return version
        previous = self.redpanda._installer.highest_from_prior_feature_version(version)
        return self.rp_install_version(num_previous=num_previous - 1, version=previous)

    def setUp(self):
        pass

    def ensure_compactible(self):
        def consumer_offsets_is_compactible():
            try:
                state = self.redpanda._admin.get_partition_state(
                    namespace="kafka", topic="__consumer_offsets", partition=0
                )
                collectible = []
                for replica in state["replicas"]:
                    for stm in replica["raft_state"]["stms"]:
                        if stm["name"] == "group_tx_tracker_stm.snapshot":
                            collectible.append(
                                stm["last_applied_offset"]
                                == stm["max_removable_local_log_offset"]
                            )
                return len(collectible) == 3 and all(collectible)
            except Exception as e:
                self.redpanda.logger.debug(f"failed to get partition state: {e}")

        wait_until(
            consumer_offsets_is_compactible,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timed out waiting for consumer offsets partition to be compactible",
        )

    @cluster(num_nodes=3)
    @matrix(versions_to_upgrade=[1, 2, 3])
    def test_consumer_group_offsets(self, versions_to_upgrade):
        """This test ensures consumer offset state remains correct during the following upgrade cycles"""

        # At the time of writing, this test checks the following version upgrades
        # Each upgrade also rolls logs ensures it is compacted
        # 24.3.x (initial_version) -> 25.1.x
        # 24.2.x (initial version) -> 24.3.x -> 24.4.x
        # 24.1.x (initial version) -> 24.2.x -> 24.3.x -> 24.4.x
        #
        # After the upgrade + compaction the following invariants are checked
        # - The state of group offets is correct as snapshotted prior to all upgrades
        # - The log is fully compactible.
        initial_version = self.rp_install_version(num_previous=versions_to_upgrade)
        self.redpanda._installer.install(self.redpanda.nodes, initial_version)
        super(VerifyConsumerOffsetsThruUpgrades, self).setUp()

        verifier = ConsumerOffsetsVerifier(self.redpanda, self._client)
        verifier.verify()

        versions = self.load_version_range(initial_version)
        for v in self.upgrade_through_versions(
            versions_in=versions, already_running=True
        ):
            self.logger.info(f"Updated to {v}")
            verifier.verify()

        self.ensure_compactible()
