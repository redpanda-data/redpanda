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
import time
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


class _StuckRaft0LearnerBase(RedpandaTest):
    """
    Shared machinery for tests that need a throttled raft0 with a node wedged
    as an in-flight learner. Subclasses set ``INITIAL_CLUSTER_SIZE`` (the number
    of seed voters) and ``JOINER_NODE_ID`` (the node id the joiner registers as).
    Not a test itself: it has no ``test_`` methods so ducktape collects nothing
    here.
    """

    INITIAL_CLUSTER_SIZE = 3
    # Seeds are [1..INITIAL_CLUSTER_SIZE]; joiner is then INITIAL_CLUSTER_SIZE+1.
    JOINER_NODE_ID = 4
    # Replication factor for internal topics. Kept <= the number of live nodes
    # so a decommissioned voter's replicas always have somewhere to drain to.
    INTERNAL_TOPIC_RF = 3
    # Nodes held in reserve beyond the seed voters (joiners / enqueued adds).
    RESERVE_NODES = 1

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        # One ducktape node per seed voter plus RESERVE_NODES held back as
        # joiners. Joiner nodes are later reused (wiped + restarted) by some
        # tests, so no extra node is needed for a rejoin under a fresh node id.
        super().__init__(
            test_context,
            num_brokers=self.INITIAL_CLUSTER_SIZE + self.RESERVE_NODES,
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

    def _node_in_brokers(self, node_id: int) -> bool:
        """True if ``node_id`` is in the controller leader's broker list.

        Consults the controller leader (the authoritative membership view)
        rather than the first node that answers: nodes removed from the cluster
        keep running with a stale broker list, and joining nodes may not have an
        admin endpoint up yet, so polling an arbitrary node is unreliable.
        """
        leader = self.redpanda.controller()
        if leader is None:
            return False
        try:
            brokers = self.redpanda._admin.get_brokers(node=leader)
        except Exception:
            return False
        return any(b.get("node_id") == node_id for b in brokers)

    def _joiner_in_brokers(self) -> bool:
        """True if the joiner appears in any started node's broker list"""
        return self._node_in_brokers(self.JOINER_NODE_ID)

    def _voter_ids(self) -> list[int]:
        """Sequential node ids of the seed voters (RedpandaService assigns by
        index, so 1..INITIAL_CLUSTER_SIZE)."""
        return list(range(1, self.INITIAL_CLUSTER_SIZE + 1))

    def _assert_only_voters_remain(self, gone_ids: list[int]) -> None:
        """Wait until raft0 is `simple` with exactly the seed voters present and
        none of ``gone_ids``, then confirm it *stays* that way.

        A single point-in-time check could land in the brief `simple` window
        between a drained reconfiguration and the next queued add being
        ingested, so we require the settled state to hold over a window to prove
        the force actually nuked the whole queue.
        """

        def settled() -> bool:
            return (
                self._controller_state() == GroupConfigurationState.SIMPLE
                and all(self._node_in_raft0(v) for v in self._voter_ids())
                and not any(self._node_in_raft0(g) for g in gone_ids)
            )

        wait_until(
            settled,
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg="raft0 did not settle to exactly the voters after force reconfig",
        )
        for _ in range(5):
            time.sleep(1)
            assert settled(), (
                "raft0 left the settled state after force reconfig; the queue "
                "was not fully nuked"
            )

    def _start_cluster(self, throttle: bool = True) -> None:
        """
        Bring up the seed voters and a test topic (so the controller log has
        non-bootstrap state to ship). With ``throttle`` set, learner recovery is
        pinned to 0 so a joining node stays stuck mid-reconfiguration.
        """
        seed_nodes = self.redpanda.nodes[: self.INITIAL_CLUSTER_SIZE]
        self.logger.info(
            f"[raft0] starting {len(seed_nodes)}-node cluster "
            f"(seeds: {[n.name for n in seed_nodes]})"
        )
        self.redpanda.set_seed_servers(seed_nodes)
        conf: dict[str, Any] = {
            "internal_topic_replication_factor": self.INTERNAL_TOPIC_RF,
        }
        if throttle:
            # throttle learner recovery to 0 so a joiner is stuck as a learner
            # and raft0 stays mid-reconfiguration.
            conf["raft_learner_recovery_rate"] = 0
            conf["controller_log_learner_recovery_rate_enabled"] = True
        self.redpanda.add_extra_rp_conf(conf)
        self.redpanda.start(nodes=seed_nodes, omit_seeds_on_idx_one=False)

        self.client().create_topic(TopicSpec(replication_factor=3, partition_count=10))

        wait_until(
            lambda: self._controller_state() == GroupConfigurationState.SIMPLE,
            timeout_sec=SHORT_TIMEOUT.timeout_s,
            backoff_sec=SHORT_TIMEOUT.backoff_s,
            err_msg="raft0 did not start in simple state",
        )

    def _start_stuck_raft0_learner(self) -> int:
        """
        Bring up a throttled cluster and join a node so it becomes an
        in-flight raft0 learner that can never finish catching up, then
        SIGKILL it. Leaves raft0 in `transitional` with a dead learner.
        Returns the joiner node id.
        """
        self._start_cluster(throttle=True)
        joiner = self.redpanda.nodes[self.INITIAL_CLUSTER_SIZE]
        self.redpanda.start_node(joiner, skip_readiness_check=True)
        wait_until(
            self._joiner_in_brokers,
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg="joiner never appeared in the leader's broker list",
        )
        joiner_id = self.JOINER_NODE_ID

        # Wait for raft0 to enter `transitional` with the joiner as the
        # in-flight learner addition.
        wait_until(
            lambda: (
                self._controller_state() == GroupConfigurationState.TRANSITIONAL
                and self._node_in_raft0(joiner_id)
            ),
            timeout_sec=MEDIUM_TIMEOUT.timeout_s,
            backoff_sec=MEDIUM_TIMEOUT.backoff_s,
            err_msg="raft0 never entered transitional state with joiner present",
        )

        # Kill the joiner while it is still a learner (intentionally killed
        # mid-promotion).
        self.redpanda.remove_from_started_nodes(joiner)
        self.redpanda.signal_redpanda(joiner, signal=signal.SIGKILL, idempotent=True)
        return joiner_id

    def _start_joining_node(self, node_idx: int) -> int:
        """
        Start the reserve node at ``node_idx`` so it registers with the cluster.
        Its raft0 add may be blocked (the cluster is wedged), so readiness is
        skipped and its own admin endpoint may never come up; registration is
        confirmed via the controller leader's broker list. RedpandaService
        assigns node ids sequentially by node index, so the id is node_idx + 1.
        Returns the node id.
        """
        node = self.redpanda.nodes[node_idx]
        node_id = node_idx + 1
        self.redpanda.start_node(node, skip_readiness_check=True)
        wait_until(
            lambda: self._node_in_brokers(node_id),
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg=f"joining node {node_id} never registered with the cluster",
        )
        return node_id

    def _join_and_kill_dead_learner(self, node_idx: int) -> int:
        """
        Start a reserve node so its raft0 add is enqueued, then SIGKILL it,
        leaving a dead in-flight/enqueued learner addition. Returns its id.
        """
        node = self.redpanda.nodes[node_idx]
        node_id = self._start_joining_node(node_idx)
        self.redpanda.remove_from_started_nodes(node)  # dead learner
        self.redpanda.signal_redpanda(node, signal=signal.SIGKILL, idempotent=True)
        return node_id

    def _cancel_controller_reconfiguration(self) -> bool:
        """
        Issue one admin cancel_reconfiguration against the controller partition,
        routed to the raft0 leader. Returns True if it was accepted.
        """
        leader = self.redpanda.controller()
        if leader is None:
            return False
        try:
            self.redpanda._admin.cancel_partition_move(
                namespace="redpanda",
                topic="controller",
                partition=0,
                node=leader,
            )
            return True
        except Exception as e:
            self.logger.debug(f"cancel reconfiguration not yet accepted: {e}")
            return False

    def _force_reconfigure_controller(self, replica_ids: list[int]) -> None:
        """
        Force the controller (raft0) configuration to ``replica_ids`` via the
        evil_mode admin endpoint, routed to the raft0 leader.
        """
        replicas = [{"node_id": nid, "core": 0} for nid in replica_ids]

        def _accepted() -> bool:
            leader = self.redpanda.controller()
            if leader is None:
                return False
            try:
                self.redpanda._admin.force_set_partition_replicas(
                    namespace="redpanda",
                    topic="controller",
                    partition=0,
                    replicas=replicas,
                    node=leader,
                    evil_mode=True,
                )
                return True
            except Exception as e:
                self.logger.debug(f"force reconfiguration not yet accepted: {e}")
                return False

        self.logger.info(f"[raft0] force-reconfiguring controller to {replica_ids}")
        wait_until(
            _accepted,
            timeout_sec=MEDIUM_TIMEOUT.timeout_s,
            backoff_sec=MEDIUM_TIMEOUT.backoff_s,
            err_msg="controller force reconfiguration was never accepted",
        )

    def _join_live_add(self, node_idx: int) -> int:
        """
        Start a live node so its raft0 add is enqueued behind the wedge; assert
        it has registered but is not yet in raft0. Returns the node id.
        """
        add_id = self._start_joining_node(node_idx)
        assert not self._node_in_raft0(add_id), (
            f"add of node {add_id} should be enqueued behind the wedge, not yet "
            f"applied to raft0"
        )
        return add_id

    def _raise_recovery_rate(self) -> None:
        """
        Lift the learner recovery throttle so live nodes can catch up. Uses a
        raw config upsert rather than set_cluster_config: the latter blocks
        until every node acks the new config version, which a not-yet-ready
        joining node never will.
        """
        self.redpanda._admin.patch_cluster_config(
            upsert={"raft_learner_recovery_rate": 100 * 1024 * 1024}
        )


class StuckRaft0LearnerTest(_StuckRaft0LearnerBase):
    """Decommissioning a dead learner cancels the in-flight raft0 add."""

    INITIAL_CLUSTER_SIZE = 3
    # Seeds are [1,2,3] joiner is then 4
    JOINER_NODE_ID = 4

    # ── tests ───────────────────────────────────────────────────────────

    @cluster(num_nodes=4)
    def test_decommission_cancels_in_flight_raft0_add(self):
        """
        Decommissioning a raft0 learner should cancel the underlying raft0 reconfiguration rather than waiting for it to complete and then decommissioning.
        Without this, a lost learner can lock membership changes.

        Steps:
        1-5. start a throttled cluster, join node 4 as a stuck learner, kill it
        6. decommission node 4
        7. wait for / assert raft0 returns to simple
        8. assert clean removal of 4
        """
        joiner_id = self._start_stuck_raft0_learner()

        # Decommission the dead joiner, should un-add from learners.
        self.logger.info(f"[raft0-cancel] decommissioning node_id={joiner_id}")
        survivor = self.redpanda.controller()
        assert survivor is not None, "no controller leader to send decommission to"
        self.redpanda._admin.decommission_broker(joiner_id, node=survivor)

        # Wait for raft0 to return to `simple` with the joiner removed from
        # raft0's group_configuration.
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

        # And the broker should be fully removed from cluster membership.
        recovery_waiter = NodeDecommissionWaiter(
            self.redpanda,
            joiner_id,
            self.logger,
            progress_timeout=MEDIUM_TIMEOUT.timeout_s,
        )
        recovery_waiter.wait_for_removal()

        # Final sanity.
        assert self._controller_state() == GroupConfigurationState.SIMPLE
        assert not self._node_in_raft0(joiner_id), (
            f"joiner {joiner_id} still in raft0 after decommission completed"
        )
        assert not self._joiner_in_brokers(), (
            f"joiner {joiner_id} still in broker list after decommission completed"
        )

    @cluster(num_nodes=4)
    def test_admin_cancel_in_flight_raft0_add(self):
        """
        The admin cancel_reconfiguration endpoint, when targeted at the
        controller partition (redpanda/controller/0), should delegate to the
        raft0 leader and cancel the in-flight controller reconfiguration
        instead of rejecting the request.

        Steps:
        1-5. start a throttled cluster, join node 4 as a stuck learner, kill it
        6. POST cancel_reconfiguration for redpanda/controller/0
        7. wait for / assert raft0 returns to simple with node 4 removed
        """
        joiner_id = self._start_stuck_raft0_learner()

        # Cancel the in-flight controller reconfiguration via the admin API.
        # The request is routed to the raft0 leader rather than rejected.
        def cancel_controller_reconfiguration() -> bool:
            controller = self.redpanda.controller()
            if controller is None:
                return False
            try:
                self.redpanda._admin.cancel_partition_move(
                    namespace="redpanda",
                    topic="controller",
                    partition=0,
                    node=controller,
                )
                return True
            except Exception as e:
                self.logger.debug(f"cancel reconfiguration not yet accepted: {e}")
                return False

        wait_until(
            cancel_controller_reconfiguration,
            timeout_sec=MEDIUM_TIMEOUT.timeout_s,
            backoff_sec=MEDIUM_TIMEOUT.backoff_s,
            err_msg="controller reconfiguration cancel was never accepted",
        )

        # raft0 should return to `simple` with the joiner removed and, because
        # the cancel bumps the configuration revision, the dead learner is not
        # re-added.
        wait_until(
            lambda: (
                self._controller_state() == GroupConfigurationState.SIMPLE
                and not self._node_in_raft0(joiner_id)
            ),
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg="raft0 did not return to simple after admin cancel",
        )


class Raft0CancelDrainsQueuedAddsTest(_StuckRaft0LearnerBase):
    """
    Cancelling a stuck learner drains the adds queued behind it.

    A dead learner wedges raft0; several live nodes then join, queuing their
    adds behind the stuck reconfiguration. One admin cancel reverts the stuck
    add, after which the queued adds apply and the nodes join. Guards against
    the cancel dropping queued entries, which would happen if it bumped the
    configuration revision past their offsets (see
    controller::cancel_raft0_reconfiguration).
    """

    INITIAL_CLUSTER_SIZE = 3
    JOINER_NODE_ID = 4
    # 1 stuck learner (id 4) + 2 live adds (ids 5, 6).
    RESERVE_NODES = 3

    @cluster(num_nodes=6)
    def test_cancel_drains_queued_adds(self):
        stuck_id = self._start_stuck_raft0_learner()
        add_ids = [
            self._join_live_add(self.INITIAL_CLUSTER_SIZE + 1 + i) for i in range(2)
        ]
        # Lift the throttle so the live adds can catch up once unblocked (the
        # dead learner stays stuck regardless, being dead).
        self._raise_recovery_rate()

        self.logger.info("[raft0] cancelling the stuck learner")
        wait_until(
            self._cancel_controller_reconfiguration,
            timeout_sec=MEDIUM_TIMEOUT.timeout_s,
            backoff_sec=MEDIUM_TIMEOUT.backoff_s,
            err_msg="controller reconfiguration cancel was never accepted",
        )

        wait_until(
            lambda: (
                self._controller_state() == GroupConfigurationState.SIMPLE
                and not self._node_in_raft0(stuck_id)
                and all(self._node_in_raft0(a) for a in add_ids)
            ),
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg=(
                "raft0 did not settle to simple with the stuck learner gone and "
                "the queued adds applied after the cancel"
            ),
        )


class Raft0ForceReconfigNukesQueueTest(_StuckRaft0LearnerBase):
    """
    A force reconfiguration of the controller nukes whatever is wedged or queued
    behind it in one shot, returning raft0 to exactly the requested replica set.
    """

    INITIAL_CLUSTER_SIZE = 3
    JOINER_NODE_ID = 4
    # 1 stuck learner (id 4) + 2 live adds (ids 5, 6).
    RESERVE_NODES = 3

    @cluster(num_nodes=6)
    def test_force_nukes_stuck_learner_and_queued_adds(self):
        """stuck, add, add -> force nukes all of them."""
        stuck_id = self._start_stuck_raft0_learner()
        add_ids = [
            self._join_live_add(self.INITIAL_CLUSTER_SIZE + 1 + i) for i in range(2)
        ]
        self._force_reconfigure_controller(self._voter_ids())
        self._assert_only_voters_remain([stuck_id, *add_ids])


class Raft0ForceNukesStuckLearnersTest(_StuckRaft0LearnerBase):
    """stuck, stuck -> a single force reconfiguration nukes both dead learners."""

    INITIAL_CLUSTER_SIZE = 3
    JOINER_NODE_ID = 4
    # Two dead learners (ids 4, 5).
    RESERVE_NODES = 2

    @cluster(num_nodes=5)
    def test_force_nukes_multiple_stuck_learners(self):
        stuck_ids = [self._start_stuck_raft0_learner()]
        stuck_ids.append(
            self._join_and_kill_dead_learner(self.INITIAL_CLUSTER_SIZE + 1)
        )
        self._force_reconfigure_controller(self._voter_ids())
        self._assert_only_voters_remain(stuck_ids)


class Raft0ForceRemoveVoterTest(_StuckRaft0LearnerBase):
    """
    A force reconfiguration can drop a healthy voter from the controller group.
    The dropped voter is ejected from raft0 but lingers in the members table; a
    decommission then cleanly removes it.
    """

    INITIAL_CLUSTER_SIZE = 4
    # No reserve nodes: the scenario only force-removes one of the four voters.
    RESERVE_NODES = 0

    @cluster(num_nodes=4)
    def test_force_remove_voter_repaired_by_decommission(self):
        # Healthy cluster, no throttle: the ejected voter's replicas must drain
        # when it is decommissioned below.
        self._start_cluster(throttle=False)

        controller = self.redpanda.controller()
        assert controller is not None, "no controller leader"
        controller_id = self.redpanda.node_id(controller)
        # Force-remove a voter other than the leader.
        victim_id = next(v for v in self._voter_ids() if v != controller_id)
        survivors = [v for v in self._voter_ids() if v != victim_id]

        self._force_reconfigure_controller(survivors)
        wait_until(
            lambda: (
                self._controller_state() == GroupConfigurationState.SIMPLE
                and all(self._node_in_raft0(s) for s in survivors)
                and not self._node_in_raft0(victim_id)
            ),
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg="raft0 did not drop the force-removed voter",
        )

        # The force-removed voter is now ejected from the controller group, so
        # it no longer receives the controller log and can't shed its own data
        # partitions; stop it (it is no longer a functioning member) so the
        # decommission below reconfigures those partitions via the live
        # majorities — the dead-node pattern the force-reconfiguration tests use.
        victim_node = self.redpanda.nodes[victim_id - 1]
        self.redpanda.stop_node(victim_node)

        # Decommissioning the ejected voter cleans it out of the members table.
        self.logger.info(f"[raft0] decommissioning force-removed voter {victim_id}")
        leader = self.redpanda.controller()
        assert leader is not None, "no controller leader"
        self.redpanda._admin.decommission_broker(victim_id, node=leader)
        NodeDecommissionWaiter(
            self.redpanda,
            victim_id,
            self.logger,
            progress_timeout=MEDIUM_TIMEOUT.timeout_s,
            decommissioned_node_ids=[victim_id],
        ).wait_for_removal()
        assert not self._node_in_brokers(victim_id), (
            f"force-removed voter {victim_id} still in the members table after "
            f"decommission"
        )

    @cluster(num_nodes=4)
    def test_force_remove_leader_transfers_leadership(self):
        """
        Force-reconfiguring the controller to a replica set that excludes the
        current raft0 leader must make that leader step down so a survivor takes
        over (consensus::force_replace_configuration_replicated steps down when
        the replicated configuration removes the local node from the voters).
        """
        self._start_cluster(throttle=False)

        old_leader = self.redpanda.controller()
        assert old_leader is not None, "no controller leader"
        old_leader_id = self.redpanda.node_id(old_leader)
        survivors = [v for v in self._voter_ids() if v != old_leader_id]

        self.logger.info(
            f"[raft0] force-removing controller leader {old_leader_id}, "
            f"survivors={survivors}"
        )
        self._force_reconfigure_controller(survivors)

        def _leadership_transferred() -> bool:
            leader = self.redpanda.controller()
            if leader is None:
                return False
            return self.redpanda.node_id(leader) in survivors

        wait_until(
            _leadership_transferred,
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg="controller leadership did not transfer to a survivor after "
            "force-removing the leader",
        )
        wait_until(
            lambda: (
                self._controller_state() == GroupConfigurationState.SIMPLE
                and all(self._node_in_raft0(s) for s in survivors)
                and not self._node_in_raft0(old_leader_id)
            ),
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg="raft0 did not settle to the survivors after force-removing "
            "the leader",
        )


class SuccessiveCancelsClearDeadAddsTest(_StuckRaft0LearnerBase):
    """
    A backlog of stuck raft0 learner adds can be cleared by running the admin
    cancel_reconfiguration repeatedly.

    Several dead nodes join a throttled cluster: the first add is in-flight and
    wedges raft0 (`transitional`), and the rest queue behind it. raft0 only
    reconfigures one at a time, so each admin cancel reverts the current
    in-flight add and lets the next dead add take its place. Running the cancel
    successively drains the whole backlog, leaving raft0 `simple` with none of
    the dead learners present.
    """

    INITIAL_CLUSTER_SIZE = 3
    JOINER_NODE_ID = 4
    # Three dead learners (ids 4,5,6), each held on its own reserve node.
    NUM_DEAD = 3
    RESERVE_NODES = 3

    def _dead_in_raft0(self, dead_ids: list[int]) -> set[int]:
        return {d for d in dead_ids if self._node_in_raft0(d)}

    @cluster(num_nodes=6)
    def test_successive_cancels_clear_dead_adds(self):
        """
        1.  start a throttled cluster and join the first dead learner, wedging
            raft0 with its in-flight add.
        2.  join and kill the remaining dead learners; their adds queue behind.
        3.  run the admin cancel repeatedly: each cancel clears the current
            in-flight dead add and the next one takes its place.
        4.  assert raft0 is `simple` with none of the dead learners present.
        """
        dead_ids = [self._start_stuck_raft0_learner()]
        for node_idx in range(
            self.INITIAL_CLUSTER_SIZE + 1, self.INITIAL_CLUSTER_SIZE + self.NUM_DEAD
        ):
            dead_ids.append(self._join_and_kill_dead_learner(node_idx))
        self.logger.info(f"[raft0-cancels] dead learners enqueued: {dead_ids}")

        # One dead add is in-flight (raft0 transitional); the rest are queued.
        assert len(self._dead_in_raft0(dead_ids)) == 1, (
            "expected exactly one in-flight dead add to start"
        )

        # Each cancel clears the current in-flight dead add; the next dead add
        # then becomes in-flight. Run one cancel per dead learner.
        for n in range(1, len(dead_ids) + 1):
            wait_until(
                lambda: len(self._dead_in_raft0(dead_ids)) >= 1,
                timeout_sec=MEDIUM_TIMEOUT.timeout_s,
                backoff_sec=MEDIUM_TIMEOUT.backoff_s,
                err_msg="no in-flight dead add to cancel",
            )
            in_flight = self._dead_in_raft0(dead_ids)
            self.logger.info(
                f"[raft0-cancels] cancel {n}/{len(dead_ids)}: clearing {in_flight}"
            )
            wait_until(
                self._cancel_controller_reconfiguration,
                timeout_sec=MEDIUM_TIMEOUT.timeout_s,
                backoff_sec=MEDIUM_TIMEOUT.backoff_s,
                err_msg="controller reconfiguration cancel was never accepted",
            )
            wait_until(
                lambda: not (in_flight & self._dead_in_raft0(dead_ids)),
                timeout_sec=MEDIUM_TIMEOUT.timeout_s,
                backoff_sec=MEDIUM_TIMEOUT.backoff_s,
                err_msg=f"cancel did not clear in-flight dead add(s) {in_flight}",
            )

        # The backlog is drained: raft0 is simple with no dead learners left.
        wait_until(
            lambda: (
                self._controller_state() == GroupConfigurationState.SIMPLE
                and not self._dead_in_raft0(dead_ids)
            ),
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg="raft0 did not settle to simple with all dead adds cleared",
        )
