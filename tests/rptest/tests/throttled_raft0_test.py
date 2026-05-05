# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
Regression tests which require a throttled raft0 recovery.
"""

import re
import signal
from dataclasses import dataclass
from enum import Enum
from typing import Any

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.node_operations import NodeDecommissionWaiter


class GroupConfigurationState(Enum):
    # no reconfiguration ongoing
    SIMPLE = "simple"
    # node being added
    TRANSITIONAL = "transitional"
    # node being removed
    JOINT = "joint"


# regex for determining group state, the cpp is inconsistent with spacing so made to be whitespace agnostic.
# An empty std::optional renders as "none" with fmt>=10 (dev) but as "{nullopt}"
# with the fmt 9.1 used on this release branch, so accept both spellings.
_EMPTY_OPTIONAL = r"(?:none|\{nullopt\})"
_GROUP_CFG_OLD_UNSET_PATTERN = re.compile(
    rf"old\s*:\s*{_EMPTY_OPTIONAL}\s*,\s*revision\s*:"
)
_GROUP_CFG_UPDATE_UNSET_PATTERN = re.compile(
    rf"update\s*:\s*{_EMPTY_OPTIONAL}\s*,\s*version\s*:"
)


def is_old_config_set(cfg: str) -> bool:
    """given a raft configuration, do we have an old configuration"""
    return _GROUP_CFG_OLD_UNSET_PATTERN.search(cfg) is None


def is_configuration_update_set(cfg: str) -> bool:
    """given a raft configuration is there an update (new nodes)"""
    return _GROUP_CFG_UPDATE_UNSET_PATTERN.search(cfg) is None


def raft_configuration_to_configuration_state(cfg: str) -> GroupConfigurationState:
    """parse a config into the group configuration state"""
    has_old_config = is_old_config_set(cfg)
    has_update = is_configuration_update_set(cfg)
    if has_old_config:
        return GroupConfigurationState.JOINT
    if has_update:
        return GroupConfigurationState.TRANSITIONAL
    return GroupConfigurationState.SIMPLE


@dataclass
class TimeoutConfig:
    timeout_s: int
    backoff_s: int


SHORT_TIMEOUT = TimeoutConfig(timeout_s=30, backoff_s=2)
MEDIUM_TIMEOUT = TimeoutConfig(timeout_s=60, backoff_s=2)
LONG_TIMEOUT = TimeoutConfig(timeout_s=120, backoff_s=2)


class StuckRaft0LearnerTest(RedpandaTest):
    """Decommissioning a dead learner cancels the in-flight raft0 add."""

    INITIAL_CLUSTER_SIZE = 3
    # Seeds are [1,2,3] joiner is then 4
    JOINER_NODE_ID = 4

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        # 4 nodes: 3 initial seeds + 1 joiner held in reserve.
        super().__init__(
            test_context,
            num_brokers=4,
            *args,
            **kwargs,
        )

    def setUp(self) -> None:
        # Manual start so we can hold the joiner in reserve.
        pass

    # ── helpers ─────────────────────────────────────────────────────────

    def _controller_state(self) -> GroupConfigurationState | None:
        """get the controller group configuration state from the controller leader"""
        for node in self.redpanda.started_nodes():
            try:
                state = self.redpanda._admin.get_partition_state(
                    "redpanda", "controller", 0, node=node
                )
            except Exception:
                continue

            for replica in state.get("replicas", []):
                raft_state = replica.get("raft_state", {})
                # only consider the leaders perspective
                if not raft_state.get("is_leader"):
                    continue
                cfg = raft_state.get("group_configuration", "")
                return raft_configuration_to_configuration_state(cfg)
        return None

    def _node_in_raft0(self, node_id: int) -> bool:
        """True if ``node_id`` is in the leader's raft0 group configuration"""
        for node in self.redpanda.started_nodes():
            try:
                state = self.redpanda._admin.get_partition_state(
                    "redpanda", "controller", 0, node=node
                )
            except Exception:
                continue
            for replica in state.get("replicas", []):
                rs = replica.get("raft_state", {})
                # only consider the leader's perspective
                if not rs.get("is_leader"):
                    continue
                cfg = rs.get("group_configuration", "")
                if not isinstance(cfg, str):
                    continue
                return f"id: {node_id}" in cfg
        return False

    # ── test ────────────────────────────────────────────────────────────

    @cluster(num_nodes=4)
    def test_decommission_cancels_in_flight_raft0_add(self):
        """
        Decommissioning a raft0 learner should cancel the underlying raft0 reconfiguration rather than waiting for it to complete and then decommissioning.
        Without this, a lost learner can lock membership changes.

        Steps:
        1. start a 3 node cluster with throttled raft0 learner rate
        2. push controller commands to fill the log past snapshot
        3. join node 4
        4. wait for / assert we see node 4 as a learner
        5. kill node 4
        6. decommission node 4
        7. wait for / assert raft0 returns to simple
        8. assert clean removal of 4
        """
        # 1. Start the first 3 of 4 allocated nodes; the 4th is the joiner.
        seed_nodes = self.redpanda.nodes[: self.INITIAL_CLUSTER_SIZE]
        joiner = self.redpanda.nodes[self.INITIAL_CLUSTER_SIZE]

        self.logger.info(
            f"[raft0-cancel] step 1: starting {len(seed_nodes)}-node "
            f"cluster (seeds: {[n.name for n in seed_nodes]}); "
            f"holding {joiner.name} in reserve"
        )
        self.redpanda.set_seed_servers(seed_nodes)

        self.redpanda.add_extra_rp_conf(
            {
                "internal_topic_replication_factor": self.INITIAL_CLUSTER_SIZE,
                "raft_learner_recovery_rate": 0,
                "controller_log_learner_recovery_rate_enabled": True,
            }
        )
        self.redpanda.start(nodes=seed_nodes, omit_seeds_on_idx_one=False)
        self.logger.info("[raft0-cancel] cluster up")

        # 2. Add some non-bootstrap state to the controller log so that
        #    catch-up actually has data to ship
        self.logger.info("[raft0-cancel] step 2: creating test topic")
        topic = TopicSpec(replication_factor=3, partition_count=10)
        self.client().create_topic(topic)

        # Sanity: raft0 is `simple` on a healthy 3-node cluster.
        wait_until(
            lambda: self._controller_state() == GroupConfigurationState.SIMPLE,
            timeout_sec=SHORT_TIMEOUT.timeout_s,
            backoff_sec=SHORT_TIMEOUT.backoff_s,
            err_msg="raft0 did not start in simple state",
        )
        self.logger.info("[raft0-cancel] raft0 confirmed `simple`")

        # 3. Start the joiner
        self.logger.info(
            f"[raft0-cancel] step 3: starting joiner {joiner.name} "
            f"with skip_readiness_check=True"
        )
        self.redpanda.start_node(joiner, skip_readiness_check=True)

        def joiner_in_brokers() -> bool:
            for survivor in seed_nodes:
                try:
                    brokers = self.redpanda._admin.get_brokers(node=survivor)
                except Exception:
                    continue
                return any(b.get("node_id") == self.JOINER_NODE_ID for b in brokers)
            return False

        wait_until(
            joiner_in_brokers,
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg="joiner never appeared in the leader's broker list",
        )
        joiner_id = self.JOINER_NODE_ID
        self.logger.info(
            f"[raft0-cancel] joiner appeared in cluster as node_id={joiner_id}"
        )

        # 4. Wait for raft0 to enter `transitional` with the joiner as
        #    the in-flight learner addition.
        self.logger.info(
            "[raft0-cancel] step 4: waiting for raft0 to enter `transitional`"
        )
        wait_until(
            lambda: (
                self._controller_state() == GroupConfigurationState.TRANSITIONAL
                and self._node_in_raft0(joiner_id)
            ),
            timeout_sec=MEDIUM_TIMEOUT.timeout_s,
            backoff_sec=MEDIUM_TIMEOUT.backoff_s,
            err_msg="raft0 never entered transitional state with joiner present",
        )
        self.logger.info(
            "[raft0-cancel] raft0 is `transitional` (learner pending promotion)"
        )

        # 5. kill the joiner while it is still a learner.
        self.logger.info(
            f"[raft0-cancel] step 5: SIGKILLing joiner node_id={joiner_id} "
            f"mid-promotion"
        )
        self.redpanda.remove_from_started_nodes(joiner)
        self.redpanda.signal_redpanda(joiner, signal=signal.SIGKILL, idempotent=True)

        # 6. Decommission the dead joiner, should un-add from learners
        self.logger.info(f"[raft0-cancel] step 6: decommissioning node_id={joiner_id}")
        survivor = self.redpanda.controller()
        assert survivor is not None, "no controller leader to send decommission to"
        self.redpanda._admin.decommission_broker(joiner_id, node=survivor)

        # 7. Wait for raft0 to return to `simple` with the joiner removed
        #    from raft0's group_configuration.
        self.logger.info(
            "[raft0-cancel] step 7: waiting for raft0 to return to `simple` "
            "with joiner removed"
        )
        wait_until(
            lambda: (
                self._controller_state() == GroupConfigurationState.SIMPLE
                and not self._node_in_raft0(joiner_id)
            ),
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg=(
                "raft0 did not return to simple with joiner removed — "
                "decommission appears stalled on configuration_change_in_progress"
            ),
        )
        self.logger.info(
            "[raft0-cancel] raft0 returned to `simple`; joiner removed from raft0"
        )

        # 8. And the broker should be fully removed from cluster
        #    membership.
        self.logger.info(
            "[raft0-cancel] step 8: waiting for broker removal from membership"
        )
        recovery_waiter = NodeDecommissionWaiter(
            self.redpanda,
            joiner_id,
            self.logger,
            progress_timeout=MEDIUM_TIMEOUT.timeout_s,
        )
        recovery_waiter.wait_for_removal()
        self.logger.info("[raft0-cancel] joiner removed from cluster membership")

        # Final sanity.
        assert self._controller_state() == GroupConfigurationState.SIMPLE
        assert not self._node_in_raft0(joiner_id), (
            f"joiner {joiner_id} still in raft0 after decommission completed"
        )
        assert not joiner_in_brokers(), (
            f"joiner {joiner_id} still in broker list after decommission completed"
        )
        self.logger.info("[raft0-cancel] all assertions passed — test PASSED")
