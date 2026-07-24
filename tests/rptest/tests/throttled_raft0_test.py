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
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from ducktape.cluster.cluster import ClusterNode
from ducktape.errors import TimeoutError
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import LoggingConfig
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.node_operations import NodeDecommissionWaiter


class GroupConfigurationState(Enum):
    # no reconfiguration ongoing
    SIMPLE = "simple"
    # node being added
    TRANSITIONAL = "transitional"
    # node being removed
    JOINT = "joint"


# regex for determining group state, the cpp is inconsistent with spacing so made to be whitespace agnostic
_GROUP_CFG_OLD_UNSET_PATTERN = re.compile(r"old\s*:\s*none\s*,\s*revision\s*:")
_GROUP_CFG_UPDATE_UNSET_PATTERN = re.compile(r"update\s*:\s*none\s*,\s*version\s*:")

# A raft group_configuration renders as
#   {current: {voters: [...], learners: [...]}, old: ..., revision: ...,
#    update: ..., version: ...}
# where each vnode is "{id: N, revision: M}" and `old` is either `none` or
# another "{voters: [...], learners: [...]}" block. vnode lists use [] and
# vnodes use {}, so "[^\]]*" safely captures a single list.
_VNODE_ID_PATTERN = re.compile(r"id\s*:\s*(\d+)")
_GROUP_NODES_PATTERN = (
    r"{}\s*:\s*\{{voters\s*:\s*\[([^\]]*)\]\s*,\s*learners\s*:\s*\[([^\]]*)\]"
)
_GROUP_CFG_CURRENT_PATTERN = re.compile(_GROUP_NODES_PATTERN.format("current"))
_GROUP_CFG_OLD_NODES_PATTERN = re.compile(_GROUP_NODES_PATTERN.format("old"))


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
class GroupNodes:
    """voters and learners of a single raft configuration"""

    voters: list[int]
    learners: list[int]

    def contains(self, node_id: int) -> bool:
        return node_id in self.voters or node_id in self.learners


@dataclass
class RaftConfiguration:
    """parsed raft group_configuration: the reconfiguration ``state`` plus the
    ``current`` and (during a joint configuration) ``old`` voter/learner sets"""

    state: GroupConfigurationState
    current: GroupNodes
    old: GroupNodes | None

    def contains(self, node_id: int) -> bool:
        """True if ``node_id`` appears in the current or old configuration"""
        return self.current.contains(node_id) or (
            self.old is not None and self.old.contains(node_id)
        )


def _parse_group_nodes(pattern: re.Pattern[str], cfg: str) -> GroupNodes | None:
    m = pattern.search(cfg)
    if m is None:
        return None
    return GroupNodes(
        voters=[int(i) for i in _VNODE_ID_PATTERN.findall(m.group(1))],
        learners=[int(i) for i in _VNODE_ID_PATTERN.findall(m.group(2))],
    )


def parse_raft_configuration(cfg: str) -> RaftConfiguration:
    """parse a raft group_configuration string into its reconfiguration state
    and current/old voter and learner sets"""
    return RaftConfiguration(
        state=raft_configuration_to_configuration_state(cfg),
        current=_parse_group_nodes(_GROUP_CFG_CURRENT_PATTERN, cfg)
        or GroupNodes(voters=[], learners=[]),
        old=_parse_group_nodes(_GROUP_CFG_OLD_NODES_PATTERN, cfg),
    )


@dataclass
class TimeoutConfig:
    timeout_s: int
    backoff_s: int


SHORT_TIMEOUT = TimeoutConfig(timeout_s=30, backoff_s=2)
MEDIUM_TIMEOUT = TimeoutConfig(timeout_s=60, backoff_s=2)
LONG_TIMEOUT = TimeoutConfig(timeout_s=120, backoff_s=2)


# ── scripted membership operations ──────────────────────────────────────


@dataclass
class AddNode:
    """Start a node into the cluster. ``node_id`` pins the broker id (else it
    is auto-assigned); ``node`` pins which ducktape node to start (else the
    next free reserve node is used).

    ``expect_learner`` optionally asserts whether the node becomes a raft0
    learner after it registers: ``True`` waits for its (auto-assigned or
    pinned) id to appear in the controller group_configuration learners list;
    ``False`` asserts it does not become a learner within a short window (e.g.
    because raft0 is already stuck on another in-flight add). ``None`` skips
    the check.

    ``expect_voter`` waits for the node's raft0 join to fully complete: its id
    must appear in the voters list with the configuration back to `simple`.
    Use it when a later operation (e.g. ThrottleRaft0) must not catch this
    node's join mid-recovery."""

    node_id: int | None = None
    node: ClusterNode | None = None
    expect_learner: bool | None = None
    expect_voter: bool = False


@dataclass
class DecommissionNode:
    """Decommission a broker. Provide one of:

    - ``node_id``: decommission that broker id;
    - ``node``: decommission every id that ducktape node has been assigned;
    - ``select_id``: a callable invoked with the test instance that returns the
      broker id to decommission, computed from live cluster state (e.g.
      ``lambda t: next(iter(t._raft0_learner_ids()))``). When set it overrides
      both ``node_id`` and ``node``."""

    node_id: int | None = None
    node: ClusterNode | None = None
    select_id: Callable[[], int] | None = None


@dataclass
class StopNode:
    """Stop a running node. Exactly one of ``node_id`` / ``node`` must be
    provided to identify the target."""

    node_id: int | None = None
    node: ClusterNode | None = None


@dataclass
class DropNodeData:
    """Wipe a node's local data directory (the node should already be stopped).
    Clears cached broker metadata so a subsequent add gets a fresh id. Exactly
    one of ``node_id`` / ``node`` must be provided to identify the target."""

    node_id: int | None = None
    node: ClusterNode | None = None


@dataclass
class ThrottleRaft0:
    """Set the controller (raft0) learner recovery rate to 0."""


@dataclass
class UnthrottleRaft0:
    """Restore the controller (raft0) learner recovery rate."""


Operation = (
    AddNode
    | DecommissionNode
    | StopNode
    | DropNodeData
    | ThrottleRaft0
    | UnthrottleRaft0
)

# raft_learner_recovery_rate values applied by the throttle operations
_THROTTLED_RATE = 0
_UNTHROTTLED_RATE = 100 * 1024 * 1024  # redpanda default: 100 MB/s


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
        # Broker count is taken from each test's @cluster(num_nodes=...)
        # decorator, so it is configurable per test case (seeds plus whatever
        # reserve nodes the scenario needs as joiners / enqueued adds; joiner
        # nodes are later reused — wiped + restarted — by some tests).
        #
        # Trace the raft/membership machinery these tests exercise while
        # keeping everything else at INFO to limit log spam.
        super().__init__(
            test_context,
            num_brokers=test_context.expected_num_nodes,
            log_config=LoggingConfig(
                "info",
                {
                    "raft": "trace",
                    "cluster": "trace",
                },
            ),
            *args,
            **kwargs,
        )

    def setUp(self) -> None:
        # Manual start so we can hold the joiner in reserve.
        pass

    # ── helpers ─────────────────────────────────────────────────────────

    def _leader_group_configuration(self) -> str | None:
        """raft0 group_configuration string from the controller leader's view,
        or None if no leader's view is currently reachable.

        Read from the controller partition state
        (v1/debug/partitions/redpanda/controller/0)."""
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
                return cfg if isinstance(cfg, str) else None
        return None

    def _raft0_configuration(self) -> RaftConfiguration | None:
        """parsed raft0 group_configuration from the controller leader, or None
        if no leader's view is currently reachable"""
        cfg = self._leader_group_configuration()
        if cfg is None:
            return None
        return parse_raft_configuration(cfg)

    def _controller_state(self) -> GroupConfigurationState | None:
        """get the controller group configuration state from the controller leader"""
        config = self._raft0_configuration()
        return config.state if config is not None else None

    def _node_in_raft0(self, node_id: int) -> bool:
        """True if ``node_id`` is in the leader's raft0 group configuration
        (current or old)"""
        config = self._raft0_configuration()
        return config is not None and config.contains(node_id)

    def _raft0_learner_ids(self) -> set[int]:
        """node ids that are learners in the controller leader's current raft0
        configuration (empty while raft0 is `simple`)"""
        config = self._raft0_configuration()
        if config is None:
            return set()
        return set(config.current.learners)

    def _broker_ids(self) -> set[int]:
        """node ids currently registered as cluster members"""
        for node in self.redpanda.started_nodes():
            try:
                brokers = self.redpanda._admin.get_brokers(node=node)
            except Exception:
                continue
            return {b["node_id"] for b in brokers if "node_id" in b}
        return set()

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

        # Kill the joiner while it is still a learner.
        self.redpanda.remove_from_started_nodes(
            joiner, "intentionally killed mid-promotion"
        )
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
        self.redpanda.remove_from_started_nodes(node, "dead learner")
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

    @cluster(num_nodes=4)
    def test_repeated_new_id_learner_adds(self):
        """
        Recycle the same physical node into the cluster twice, wiping its disk
        between attempts so it rejoins with a brand-new node id each time.

        With a throttled (rate 0) controller learner recovery the first joiner
        gets stuck as a raft0 learner, leaving raft0 `transitional`. While it is
        stuck no further membership change can start, so the second joiner only
        registers as a broker and never becomes a learner. Finally we cancel the
        stuck add and clean up, asserting raft0 recovers to `simple`.

        The joiner's node id is never read from its own (not-yet-ready) admin
        endpoint; instead it is discovered from the controller leader's view:
        the group_configuration learners list for the first (learner) join and
        the broker list for the second.

        Steps:
        1. start a 3 node cluster with throttled raft0 learner rate
        2. push controller commands to fill the log past snapshot
        3. wipe the joiner's disk and start it with an auto-assigned id
        4. via the controller group_configuration, wait for / assert it was
           added as a raft0 learner; that learner id is the first joiner's id
        5. kill the stuck learner
        6. wipe + restart the joiner; it gets a second, distinct id. raft0 is
           still stuck, so discover this id from the controller broker list (it
           is not a learner). Kill it.
        7. decommission both dead joiners; assert raft0 returns to `simple` and
           both ids are gone
        """
        # 1. Start the first 3 of 4 allocated nodes; the 4th is the recycled
        #    joiner.
        seed_nodes = self.redpanda.nodes[: self.INITIAL_CLUSTER_SIZE]
        joiner = self.redpanda.nodes[self.INITIAL_CLUSTER_SIZE]

        self.logger.info(
            f"[raft0-recycle] step 1: starting {len(seed_nodes)}-node "
            f"cluster (seeds: {[n.name for n in seed_nodes]}); "
            f"recycling {joiner.name} twice"
        )
        self.redpanda.set_seed_servers(seed_nodes)

        self.redpanda.add_extra_rp_conf(
            {
                "internal_topic_replication_factor": self.INITIAL_CLUSTER_SIZE,
                "raft_learner_recovery_rate": 0,
                "controller_log_learner_recovery_rate_enabled": True,
            }
        )
        self.redpanda.start(
            nodes=seed_nodes, omit_seeds_on_idx_one=False, auto_assign_node_id=True
        )
        self.logger.info("[raft0-recycle] cluster up")

        # 2. Add some non-bootstrap state to the controller log so that
        #    catch-up actually has data to ship.
        self.logger.info("[raft0-recycle] step 2: creating test topic")
        topic = TopicSpec(replication_factor=3, partition_count=10)
        self.client().create_topic(topic)

        wait_until(
            lambda: self._controller_state() == GroupConfigurationState.SIMPLE,
            timeout_sec=SHORT_TIMEOUT.timeout_s,
            backoff_sec=SHORT_TIMEOUT.backoff_s,
            err_msg="raft0 did not start in simple state",
        )
        self.logger.info("[raft0-recycle] raft0 confirmed `simple`")

        def start_fresh_joiner() -> None:
            """wipe the joiner's disk and start it so it auto-assigns a new id"""
            self.redpanda.clean_node(joiner, preserve_current_install=True)
            self.redpanda.start_node(
                joiner,
                auto_assign_node_id=True,
                omit_seeds_on_idx_one=False,
                skip_readiness_check=True,
            )

        def kill_joiner() -> None:
            self.redpanda.remove_from_started_nodes(
                joiner, "intentionally killed mid-promotion"
            )
            self.redpanda.signal_redpanda(
                joiner, signal=signal.SIGKILL, idempotent=True
            )

        # ── first recycle: joiner gets stuck as a raft0 learner ────────────
        # 3. Wipe + start the joiner with an auto-assigned id.
        self.logger.info(
            f"[raft0-recycle] step 3: wiping {joiner.name} and starting it "
            f"with an auto-assigned node id (first join)"
        )
        start_fresh_joiner()

        # 4. The joiner's admin endpoint may not be up yet (skip_readiness),
        #    so discover its id from the controller leader's partition state:
        #    wait until raft0 is `transitional` and the group_configuration's
        #    current learners list is non-empty (it is empty while `simple`).
        self.logger.info(
            "[raft0-recycle] step 4: waiting for the joiner to be added as a "
            "raft0 learner (per controller group_configuration)"
        )

        # Capture the learner set inside the predicate so we assert on the same
        # snapshot that satisfied the wait (a re-fetch could transiently race
        # with leadership changes or return an empty set).
        captured_learners: set[int] = set()

        def first_learner_added() -> bool:
            nonlocal captured_learners
            if self._controller_state() != GroupConfigurationState.TRANSITIONAL:
                return False
            learners = self._raft0_learner_ids()
            if not learners:
                return False
            captured_learners = learners
            return True

        wait_until(
            first_learner_added,
            timeout_sec=MEDIUM_TIMEOUT.timeout_s,
            backoff_sec=MEDIUM_TIMEOUT.backoff_s,
            err_msg="joiner was never added to raft0 as a learner",
        )
        assert len(captured_learners) == 1, (
            f"expected exactly one raft0 learner, saw {sorted(captured_learners)}"
        )
        first_joiner_id = captured_learners.pop()
        self.logger.info(
            f"[raft0-recycle] step 4: joiner added as learner node_id={first_joiner_id}"
        )

        # 5. Kill the stuck learner.
        self.logger.info(
            f"[raft0-recycle] step 5: SIGKILLing stuck learner "
            f"node_id={first_joiner_id}"
        )
        kill_joiner()

        # ── second recycle: joiner rejoins with a new id while raft0 is stuck ─
        # raft0 is still `transitional` on the dead first learner, so the second
        # joiner cannot become a learner; it only registers as a broker. Hence
        # we discover its id from the controller broker list, not raft0.
        broker_ids_before = self._broker_ids()

        # 6. Wipe + restart the joiner; it gets a second, distinct id.
        self.logger.info(
            f"[raft0-recycle] step 6: wiping {joiner.name} and starting it "
            f"again with an auto-assigned node id (second join)"
        )
        start_fresh_joiner()

        self.logger.info(
            "[raft0-recycle] step 6: waiting for the second joiner to register "
            "as a cluster member (per broker list)"
        )

        def second_broker_registered() -> bool:
            return len(self._broker_ids() - broker_ids_before) >= 1

        wait_until(
            second_broker_registered,
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg="second joiner never registered in the cluster broker list",
        )
        new_broker_ids = self._broker_ids() - broker_ids_before
        assert len(new_broker_ids) == 1, (
            f"expected exactly one new broker, saw {sorted(new_broker_ids)}"
        )
        second_joiner_id = new_broker_ids.pop()
        assert second_joiner_id != first_joiner_id, (
            f"second joiner reused the first id {second_joiner_id}"
        )
        self.logger.info(
            f"[raft0-recycle] step 6: second joiner registered as "
            f"node_id={second_joiner_id}"
        )

        kill_joiner()

        # 7. Recover. Decommission both recycled ids. Issuing both decommissions
        #    up front stops the controller from trying to re-add either dead
        #    broker as a learner while we wait (which would flip raft0 back to
        #    `transitional`). Decommissioning the first — the stuck learner —
        #    cancels its in-flight raft0 add, which is the behaviour under test.
        dead_ids = (first_joiner_id, second_joiner_id)
        for dead_id in dead_ids:
            # re-resolve the controller each time: decommissioning can move
            # leadership.
            controller = self.redpanda.controller()
            assert controller is not None, (
                "no controller leader to send decommission to"
            )
            self.logger.info(
                f"[raft0-recycle] step 7: decommissioning node_id={dead_id}"
            )
            self.redpanda._admin.decommission_broker(dead_id, node=controller)

        for dead_id in dead_ids:
            NodeDecommissionWaiter(
                self.redpanda,
                dead_id,
                self.logger,
                progress_timeout=MEDIUM_TIMEOUT.timeout_s,
            ).wait_for_removal()

        # raft0 must recover to `simple` with both recycled ids gone — proving
        # the stuck learner's add was cancelled rather than locking membership.
        wait_until(
            lambda: (
                self._controller_state() == GroupConfigurationState.SIMPLE
                and not self._node_in_raft0(first_joiner_id)
                and not self._node_in_raft0(second_joiner_id)
            ),
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg=(
                "raft0 did not return to simple after decommissioning both "
                "recycled joiners — a stuck add was not cancelled"
            ),
        )
        broker_ids = self._broker_ids()
        assert first_joiner_id not in broker_ids, (
            f"first joiner {first_joiner_id} still a member after decommission"
        )
        assert second_joiner_id not in broker_ids, (
            f"second joiner {second_joiner_id} still a member after decommission"
        )

        self.logger.info("[raft0-recycle] all assertions passed — test PASSED")


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


class Raft0MembershipOpsTest(_StuckRaft0LearnerBase):
    """Drive a scripted list of membership operations (add / decommission /
    throttle / unthrottle raft0) and assert the cluster converges afterwards.

    Operations are issued fire-and-continue (no per-op waiting), so throttled
    additions intentionally pile up as stuck learners. Convergence is checked
    once at the end: recovery is unthrottled, raft0 must settle back to
    `simple`, decommissioned brokers must be gone, and the surviving membership
    must equal the seeds plus the net additions.

    The broker count is whatever each test's @cluster(num_nodes=...) reserves;
    INITIAL_CLUSTER_SIZE of them are seeds and the rest are reserve nodes
    available to AddNode operations."""

    # every id a node has been assigned (a node recycled through wipes takes
    # a new id each time). Lets us stop / drop / decommission a node by its
    # ducktape handle even when the id was auto-assigned and the node's own
    # admin is no longer reachable.
    node_id_history: dict[ClusterNode, list[int]] = {}

    def _start_seed_cluster(self) -> list[ClusterNode]:
        seed_nodes = self.redpanda.nodes[: self.INITIAL_CLUSTER_SIZE]
        self.logger.info(
            f"[raft0-ops] starting {len(seed_nodes)}-node seed cluster "
            f"(seeds: {[n.name for n in seed_nodes]})"
        )
        self.redpanda.set_seed_servers(seed_nodes)
        self.redpanda.add_extra_rp_conf(
            {
                "internal_topic_replication_factor": self.INITIAL_CLUSTER_SIZE,
                "controller_log_learner_recovery_rate_enabled": True,
            }
        )
        # start unthrottled; the operation list drives throttling.
        self.redpanda.start(
            nodes=seed_nodes, omit_seeds_on_idx_one=False, auto_assign_node_id=True
        )
        # give the controller log some non-bootstrap state to ship.
        self.client().create_topic(TopicSpec(replication_factor=3, partition_count=10))
        wait_until(
            lambda: self._controller_state() == GroupConfigurationState.SIMPLE,
            timeout_sec=SHORT_TIMEOUT.timeout_s,
            backoff_sec=SHORT_TIMEOUT.backoff_s,
            err_msg="raft0 did not start in simple state",
        )
        return seed_nodes

    def _set_recovery_rate(self, rate: int) -> None:
        """patch raft_learner_recovery_rate on the controller without waiting
        for every (possibly stuck) node to ack the new config."""
        controller = self.redpanda.controller()
        assert controller is not None, "no controller leader to set config on"
        self.redpanda._admin.patch_cluster_config(
            upsert={"raft_learner_recovery_rate": rate}, node=controller
        )

    def _assert_learner_expectation(
        self, node_id: int, expect_learner: bool, idx: int
    ) -> None:
        """Assert whether ``node_id`` becomes a raft0 learner."""
        if expect_learner:
            wait_until(
                lambda: (
                    self._controller_state() == GroupConfigurationState.TRANSITIONAL
                    and node_id in self._raft0_learner_ids()
                ),
                timeout_sec=MEDIUM_TIMEOUT.timeout_s,
                backoff_sec=MEDIUM_TIMEOUT.backoff_s,
                err_msg=f"op {idx}: node {node_id} never became a raft0 learner",
            )
            return
        # expect_learner is False: it must NOT become a learner. Confirm by
        # waiting for it to appear as a learner and requiring that to time out.
        try:
            wait_until(
                lambda: node_id in self._raft0_learner_ids(),
                timeout_sec=SHORT_TIMEOUT.timeout_s,
                backoff_sec=SHORT_TIMEOUT.backoff_s,
            )
        except TimeoutError:
            return
        raise AssertionError(
            f"op {idx}: node {node_id} unexpectedly became a raft0 learner"
        )

    def _wait_until_voter(self, node_id: int, idx: int) -> None:
        """Wait until ``node_id`` is a raft0 voter with the configuration back
        to `simple`, i.e. its join fully completed."""

        def _is_voter() -> bool:
            config = self._raft0_configuration()
            return (
                config is not None
                and config.state == GroupConfigurationState.SIMPLE
                and node_id in config.current.voters
            )

        wait_until(
            _is_voter,
            timeout_sec=MEDIUM_TIMEOUT.timeout_s,
            backoff_sec=MEDIUM_TIMEOUT.backoff_s,
            err_msg=f"op {idx}: node {node_id} never became a raft0 voter",
        )

    def _run_operations(self, operations: list[Operation]) -> None:
        """Execute a list of operations against an already-started cluster
        (call ``_start_seed_cluster`` first)."""
        seed_nodes = self.redpanda.nodes[: self.INITIAL_CLUSTER_SIZE]
        # reserve ducktape nodes used to satisfy AddNode ops with no explicit node
        reserve = list(self.redpanda.nodes[self.INITIAL_CLUSTER_SIZE :])
        added_nodes: list[ClusterNode] = []
        decommissioned_ids: set[int] = set()

        # reverse index id -> node, for ops that target a node by id.
        id_to_node: dict[int, ClusterNode] = {}

        def resolve_node(
            op: StopNode | DropNodeData, idx: int, what: str
        ) -> ClusterNode:
            if op.node is not None:
                return op.node
            if op.node_id is not None:
                node = id_to_node.get(op.node_id) or self.redpanda.get_node_by_id(
                    op.node_id
                )
                assert node is not None, (
                    f"op {idx}: {what} could not resolve node for node_id={op.node_id}"
                )
                return node
            raise AssertionError(f"op {idx}: {what} requires node_id or node")

        def decommission_id(node_id: int, idx: int) -> None:
            if node_id not in self._broker_ids():
                self.logger.info(
                    f"[raft0-ops] op {idx}: node_id={node_id} is not a member, "
                    f"skipping decommission"
                )
                return
            controller = self.redpanda.controller()
            assert controller is not None, (
                "no controller leader to send decommission to"
            )
            self.logger.info(f"[raft0-ops] op {idx}: decommissioning node_id={node_id}")
            self.redpanda._admin.decommission_broker(node_id, node=controller)
            decommissioned_ids.add(node_id)

        for i, op in enumerate(operations):
            if isinstance(op, ThrottleRaft0):
                self.logger.info(
                    f"[raft0-ops] op {i}: throttling raft0 recovery "
                    f"(rate={_THROTTLED_RATE})"
                )
                self._set_recovery_rate(_THROTTLED_RATE)
            elif isinstance(op, UnthrottleRaft0):
                self.logger.info(
                    f"[raft0-ops] op {i}: unthrottling raft0 recovery "
                    f"(rate={_UNTHROTTLED_RATE})"
                )
                self._set_recovery_rate(_UNTHROTTLED_RATE)
            elif isinstance(op, AddNode):
                node = op.node
                if node is None:
                    assert reserve, f"op {i}: ran out of reserve nodes for AddNode"
                    node = reserve.pop(0)
                self.logger.info(
                    f"[raft0-ops] op {i}: adding node {node.name} "
                    f"({'node_id=' + str(op.node_id) if op.node_id else 'auto-assigned id'})"
                )
                broker_ids_before = self._broker_ids()
                self.redpanda.clean_node(node, preserve_current_install=True)
                self.redpanda.start_node(
                    node,
                    auto_assign_node_id=op.node_id is None,
                    node_id_override=op.node_id,
                    omit_seeds_on_idx_one=False,
                    skip_readiness_check=True,
                )

                def _storage_is_present(node: ClusterNode) -> bool:
                    storage = self.redpanda.storage(nodes=[node], sizes=True)
                    return (
                        len(storage.nodes) > 0
                        and len(storage.nodes[0].partitions("redpanda", "controller"))
                        > 0
                    )

                # Wait for the node to start and register its pid so we can stop it later.
                wait_until(
                    lambda n=node: self.redpanda.redpanda_pid(n) is not None
                    and _storage_is_present(n),
                    timeout_sec=SHORT_TIMEOUT.timeout_s,
                )
                added_nodes.append(node)
                # Wait for the broker to register in the cluster member list and
                # determine its (possibly auto-assigned) id. Registration is a
                # controller-log append, so it completes even while raft0 learner
                # recovery is throttled (the node just won't become a learner).
                if op.node_id is not None:
                    expected_id = op.node_id
                    wait_until(
                        lambda eid=expected_id: eid in self._broker_ids(),
                        timeout_sec=LONG_TIMEOUT.timeout_s,
                        backoff_sec=LONG_TIMEOUT.backoff_s,
                        err_msg=(
                            f"op {i}: added broker node_id={expected_id} never "
                            f"appeared in the broker list"
                        ),
                    )
                    assigned_id = op.node_id
                else:
                    wait_until(
                        lambda before=broker_ids_before: bool(
                            self._broker_ids() - before
                        ),
                        timeout_sec=LONG_TIMEOUT.timeout_s,
                        backoff_sec=LONG_TIMEOUT.backoff_s,
                        err_msg=(
                            f"op {i}: added node {node.name} never appeared in "
                            f"the broker list"
                        ),
                    )
                    new_ids = self._broker_ids() - broker_ids_before
                    assert len(new_ids) == 1, (
                        f"op {i}: expected exactly one new broker, "
                        f"saw {sorted(new_ids)}"
                    )
                    assigned_id = next(iter(new_ids))
                self.node_id_history.setdefault(node, []).append(assigned_id)
                id_to_node[assigned_id] = node
                decommissioned_ids.discard(assigned_id)
                self.logger.info(
                    f"[raft0-ops] op {i}: node {node.name} registered as "
                    f"node_id={assigned_id}"
                )
                if op.expect_learner is not None:
                    self._assert_learner_expectation(assigned_id, op.expect_learner, i)
                if op.expect_voter:
                    self._wait_until_voter(assigned_id, i)
            elif isinstance(op, StopNode):
                node = resolve_node(op, i, "StopNode")
                self.logger.info(f"[raft0-ops] op {i}: stopping node {node.name}")
                self.redpanda.stop_node(node)
            elif isinstance(op, DropNodeData):
                node = resolve_node(op, i, "DropNodeData")
                self.logger.info(
                    f"[raft0-ops] op {i}: dropping data on node {node.name}"
                )
                self.redpanda.remove_local_data(node)
            else:
                # DecommissionNode. select_id (computed from live state) wins;
                # otherwise by id, or by node -> every id that node has been
                # assigned (a recycled node leaves a dead broker per
                # incarnation; decommission them all).
                if op.select_id is not None:
                    target_ids = [op.select_id()]
                elif op.node_id is not None:
                    target_ids = [op.node_id]
                elif op.node is not None:
                    target_ids = list(self.node_id_history.get(op.node, []))
                    if not target_ids:
                        target_ids = [
                            self.redpanda.node_id(op.node, force_refresh=True)
                        ]
                else:
                    raise AssertionError(
                        f"op {i}: DecommissionNode requires node_id, node, or select_id"
                    )
                for node_id in target_ids:
                    decommission_id(node_id, i)

        self._converge_after_operations(seed_nodes, added_nodes, decommissioned_ids)

    def _converge_after_operations(
        self,
        seed_nodes: list[ClusterNode],
        added_nodes: list[ClusterNode],
        decommissioned_ids: set[int],
    ) -> None:
        # ensure recovery is not throttled so any pending learner adds can
        # complete (or have been cancelled by decommission).
        self._set_recovery_rate(_UNTHROTTLED_RATE)

        # raft0 must settle back to `simple`: nothing left stuck in a
        # reconfiguration.
        wait_until(
            lambda: self._controller_state() == GroupConfigurationState.SIMPLE,
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg="raft0 did not converge to `simple` after running operations",
        )

        # decommissioned brokers must be fully removed from membership.
        for node_id in decommissioned_ids:
            NodeDecommissionWaiter(
                self.redpanda,
                node_id,
                self.logger,
                progress_timeout=MEDIUM_TIMEOUT.timeout_s,
            ).wait_for_removal()

        # net membership = seeds + added-still-alive - decommissioned.
        expected_member_ids = {
            self.redpanda.node_id(n, force_refresh=True) for n in seed_nodes
        }
        for node in added_nodes:
            try:
                node_id = self.redpanda.node_id(node, force_refresh=True)
            except Exception:
                # a decommissioned node may no longer answer; it is excluded
                # below regardless.
                continue
            expected_member_ids.add(node_id)
        expected_member_ids -= decommissioned_ids

        wait_until(
            lambda: self._broker_ids() == expected_member_ids,
            timeout_sec=LONG_TIMEOUT.timeout_s,
            backoff_sec=LONG_TIMEOUT.backoff_s,
            err_msg=(
                f"membership did not converge to {sorted(expected_member_ids)}; "
                f"current: {sorted(self._broker_ids())}"
            ),
        )
        self.logger.info("[raft0-ops] cluster converged; membership as expected")

    @cluster(num_nodes=5)
    def test_interleaved_operations(self):
        NEW_JOINER = 9
        STUCK_JOINER = 10

        self._start_seed_cluster()
        self._run_operations(
            [
                AddNode(node_id=NEW_JOINER, expect_voter=True),
                ThrottleRaft0(),
                AddNode(node_id=STUCK_JOINER, expect_learner=True),
                StopNode(node_id=STUCK_JOINER),
                DecommissionNode(node_id=NEW_JOINER),
                DecommissionNode(node_id=STUCK_JOINER),
                UnthrottleRaft0(),
            ]
        )

    @cluster(num_nodes=4)
    def test_join_decommission_join(self):
        self._start_seed_cluster()
        new_joiner = self.redpanda.nodes[self.INITIAL_CLUSTER_SIZE]
        new_joiner_id = 10
        self._run_operations(
            [
                ThrottleRaft0(),
                AddNode(node=new_joiner, node_id=new_joiner_id),
                StopNode(node_id=new_joiner_id),
                DropNodeData(node_id=new_joiner_id),
                DecommissionNode(node_id=new_joiner_id),
                AddNode(node=new_joiner, node_id=new_joiner_id),
                DecommissionNode(node_id=new_joiner_id),
                StopNode(node_id=new_joiner_id),
                DropNodeData(node_id=new_joiner_id),
                AddNode(node=new_joiner, node_id=new_joiner_id),
                UnthrottleRaft0(),
            ]
        )

    @cluster(num_nodes=4)
    def test_readding_the_node_with_the_same_id(self):
        self._start_seed_cluster()
        new_joiner = self.redpanda.nodes[self.INITIAL_CLUSTER_SIZE]
        new_joiner_id = 10
        self._run_operations(
            [
                ThrottleRaft0(),
                AddNode(node=new_joiner, node_id=new_joiner_id, expect_learner=True),
                StopNode(node_id=new_joiner_id),
                DropNodeData(node_id=new_joiner_id),
                AddNode(node=new_joiner, node_id=new_joiner_id, expect_learner=True),
                StopNode(node_id=new_joiner_id),
                DropNodeData(node_id=new_joiner_id),
                AddNode(node=new_joiner, node_id=new_joiner_id),
                UnthrottleRaft0(),
            ]
        )

    @cluster(num_nodes=4)
    def test_ghost_broker(self):
        self._start_seed_cluster()
        new_joiner = self.redpanda.nodes[self.INITIAL_CLUSTER_SIZE]

        self._run_operations(
            [
                ThrottleRaft0(),
                AddNode(node=new_joiner, expect_learner=True),
                StopNode(node=new_joiner),
                DropNodeData(node=new_joiner),
                UnthrottleRaft0(),
                AddNode(node=new_joiner),
                DecommissionNode(
                    select_id=lambda: self.node_id_history.get(new_joiner, [])[0]
                ),
            ]
        )

    @cluster(num_nodes=4)
    def test_repeated_new_id_learner_adds_via_ops(self):
        """Operations-framework equivalent of test_repeated_new_id_learner_adds.

        Recycle a single node twice (a distinct pinned id each time) while raft0
        recovery is throttled. The first incarnation must become a stuck raft0
        learner; the second, blocked behind the first, must only register as a
        broker and never become a learner. Then decommission both dead ids and
        let the cluster converge back to `simple`."""
        self._start_seed_cluster()
        joiner = self.redpanda.nodes[self.INITIAL_CLUSTER_SIZE]
        FIRST_ID = 10
        SECOND_ID = 11
        self._run_operations(
            [
                ThrottleRaft0(),
                AddNode(node=joiner, node_id=FIRST_ID, expect_learner=True),
                StopNode(node_id=FIRST_ID),
                DropNodeData(node_id=FIRST_ID),
                AddNode(node=joiner, node_id=SECOND_ID, expect_learner=False),
                StopNode(node_id=SECOND_ID),
                DecommissionNode(node_id=FIRST_ID),
                DecommissionNode(node_id=SECOND_ID),
                UnthrottleRaft0(),
            ]
        )
