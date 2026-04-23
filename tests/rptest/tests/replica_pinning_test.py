# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
from collections import defaultdict

from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result

REPLICAS_PREFERENCE_KEY = "redpanda.replicas.preference"

# Allowed deviation from expected replica counts in statistical tests.
# Internal topic replicas cause small shifts in the distribution.
JITTER_PERCENT = 0.05


class ReplicaPinningTest(RedpandaTest):
    """
    Integration tests for the per-topic replicas_preference property that
    influences replica placement using a priority-ordered list of racks.

    All tests allocate 8 nodes (the max needed across all tests) and start
    only the subset required via _start_cluster(rack_layout).
    """

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context=test_context,
            num_brokers=8,
            skip_if_no_redpanda_log=True,
        )

    def setUp(self):
        # Defer start to each test method via _start_cluster.
        pass

    def _start_cluster(
        self,
        rack_layout: list[str | None],
        rack_awareness: bool = True,
    ):
        """
        Configure and start a cluster with the given rack layout.

        rack_layout is a list of rack names, one per broker. Use None for
        a rackless node. The list length determines how many of the
        pre-allocated nodes to start.
        """
        nodes_to_start = self.redpanda.nodes[: len(rack_layout)]

        self.redpanda.add_extra_rp_conf(
            {
                "enable_rack_awareness": rack_awareness,
                "partition_autobalancing_mode": "continuous",
                "partition_autobalancing_node_availability_timeout_sec": 10,
                "partition_autobalancing_tick_interval_ms": 5000,
                "raft_learner_recovery_rate": 100_000_000,
            }
        )

        for ix, node in enumerate(nodes_to_start):
            if rack_layout[ix] is not None:
                self.redpanda.set_extra_node_conf(node, {"rack": rack_layout[ix]})

        self.redpanda.start(nodes=nodes_to_start)
        self._rack_layout = rack_layout

    def _node_id_to_rack(self) -> dict[int, str | None]:
        """Return a map of node_id -> rack from the admin API."""
        admin = Admin(self.redpanda)
        brokers = admin.get_brokers()
        result: dict[int, str | None] = {}
        for b in brokers:
            result[b["node_id"]] = b.get("rack")
        return result

    def _get_partition_rack_counts(
        self, topic: str, n2r: dict[int, str | None]
    ) -> dict[str | None, int]:
        """
        For a single-partition topic, return {rack: replica_count}.
        """
        rpk = RpkTool(self.redpanda)
        partitions = list(rpk.describe_topic(topic))
        assert len(partitions) == 1, f"Expected 1 partition, got {len(partitions)}"
        counts: dict[str | None, int] = defaultdict(int)
        for replica_id in partitions[0].replicas:
            counts[n2r.get(replica_id)] += 1
        return dict(counts)

    def _get_multi_partition_rack_counts(
        self,
        topic: str,
        n2r: dict[int, str | None],
        expected_partitions: int,
    ) -> dict[str | None, int]:
        """
        For a multi-partition topic, return total replica count per rack
        across all partitions. Waits until all partitions are ready.
        """
        rpk = RpkTool(self.redpanda)

        def ready():
            try:
                parts = list(rpk.describe_topic(topic))
            except Exception:
                return False
            if len(parts) != expected_partitions:
                return False
            for p in parts:
                if len(p.replicas) == 0:
                    return False
            return (True, parts)

        parts = wait_until_result(
            ready, timeout_sec=60, backoff_sec=1, err_msg="partitions not ready"
        )

        counts: dict[str | None, int] = defaultdict(int)
        for p in parts:
            for replica_id in p.replicas:
                counts[n2r.get(replica_id)] += 1
        return dict(counts)

    def _get_multi_partition_node_counts(
        self,
        topic: str,
        expected_partitions: int,
    ) -> dict[int, int]:
        """
        For a multi-partition topic, return total replica count per node.
        Waits until all partitions are ready.
        """
        rpk = RpkTool(self.redpanda)

        def ready():
            try:
                parts = list(rpk.describe_topic(topic))
            except Exception:
                return False
            if len(parts) != expected_partitions:
                return False
            for p in parts:
                if len(p.replicas) == 0:
                    return False
            return (True, parts)

        parts = wait_until_result(
            ready, timeout_sec=60, backoff_sec=1, err_msg="partitions not ready"
        )

        counts: dict[int, int] = defaultdict(int)
        for p in parts:
            for replica_id in p.replicas:
                counts[replica_id] += 1
        return dict(counts)

    def _wait_no_reconfigurations(self, timeout_sec: int = 120):
        """Wait until there are no pending partition reconfigurations."""
        admin = Admin(self.redpanda)

        def no_reconfigs():
            try:
                return len(admin.list_reconfigurations()) == 0
            except Exception:
                return False

        wait_until(
            no_reconfigs,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg="Reconfigurations did not complete in time",
        )

    # ------------------------------------------------------------------
    # Test 1: Pinning + rack awareness ON
    # ------------------------------------------------------------------
    @cluster(num_nodes=8)
    def test_pinning_with_rack_awareness(self):
        """
        Rack awareness (L0) spreads across racks, pinning (L1) selects WHICH
        racks. Unpinned rack D is skipped.

        5 nodes, 4 racks. rack_awareness=ON. RF=3, preference="racks: A, B, C"
        Nodes: 1,2 in rack A; 3 in rack B; 4 in rack C; 5 in rack D

        Expected: 1 replica in each of racks A, B, C. 0 in rack D.
        """
        self._start_cluster(["A", "A", "B", "C", "D"])

        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            "pinning-rack-aware",
            partitions=1,
            replicas=3,
            config={REPLICAS_PREFERENCE_KEY: "racks: A, B, C"},
        )

        n2r = self._node_id_to_rack()
        rc = self._get_partition_rack_counts("pinning-rack-aware", n2r)

        self.logger.info(f"Rack counts: {rc}")
        assert rc.get("A", 0) == 1, (
            f"Expected 1 replica in rack A, got {rc.get('A', 0)}"
        )
        assert rc.get("B", 0) == 1, (
            f"Expected 1 replica in rack B, got {rc.get('B', 0)}"
        )
        assert rc.get("C", 0) == 1, (
            f"Expected 1 replica in rack C, got {rc.get('C', 0)}"
        )
        assert rc.get("D", 0) == 0, (
            f"Expected 0 replicas in rack D, got {rc.get('D', 0)}"
        )

    # ------------------------------------------------------------------
    # Test 2: Pinning without rack awareness
    # ------------------------------------------------------------------
    @cluster(num_nodes=8)
    def test_pinning_without_rack_awareness(self):
        """
        With rack awareness OFF, pinning concentrates replicas in the
        highest-priority rack, then overflows to the next.

        5 nodes, 3 racks. rack_awareness=OFF. RF=3, preference="racks: A, B"
        Nodes: 1,2,3 in rack A; 4 in rack B; 5 in rack C
        Rack A has capacity 3 >= RF, so all 3 replicas should land on A.

        Expected: 3 replicas in rack A, 0 in B, 0 in C.
        """
        self._start_cluster(["A", "A", "A", "B", "C"], rack_awareness=False)

        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            "pinning-no-rack",
            partitions=1,
            replicas=3,
            config={REPLICAS_PREFERENCE_KEY: "racks: A, B"},
        )

        n2r = self._node_id_to_rack()
        rc = self._get_partition_rack_counts("pinning-no-rack", n2r)

        self.logger.info(f"Rack counts: {rc}")
        assert rc.get("A", 0) == 3, (
            f"Expected 3 replicas in rack A, got {rc.get('A', 0)}"
        )
        assert rc.get("B", 0) == 0, (
            f"Expected 0 replicas in rack B, got {rc.get('B', 0)}"
        )
        assert rc.get("C", 0) == 0, (
            f"Expected 0 replicas in rack C, got {rc.get('C', 0)}"
        )

    # ------------------------------------------------------------------
    # Test 3: Priority ordering
    # ------------------------------------------------------------------
    @cluster(num_nodes=8)
    def test_pinning_priority_ordering(self):
        """
        The allocator respects priority ordering: A > B > C > D. With RF=3,
        the top-3 priority racks are chosen.

        4 nodes, 4 racks. rack_awareness=ON. RF=3,
        preference="racks: A, B, C, D"
        """
        self._start_cluster(["A", "B", "C", "D"])

        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            "pinning-priority",
            partitions=1,
            replicas=3,
            config={REPLICAS_PREFERENCE_KEY: "racks: A, B, C, D"},
        )

        n2r = self._node_id_to_rack()
        rc = self._get_partition_rack_counts("pinning-priority", n2r)

        self.logger.info(f"Rack counts: {rc}")
        assert rc.get("A", 0) == 1, (
            f"Expected 1 replica in rack A, got {rc.get('A', 0)}"
        )
        assert rc.get("B", 0) == 1, (
            f"Expected 1 replica in rack B, got {rc.get('B', 0)}"
        )
        assert rc.get("C", 0) == 1, (
            f"Expected 1 replica in rack C, got {rc.get('C', 0)}"
        )
        assert rc.get("D", 0) == 0, (
            f"Expected 0 replicas in rack D, got {rc.get('D', 0)}"
        )

    # ------------------------------------------------------------------
    # Test 4: Group notation -- statistical
    # ------------------------------------------------------------------
    @cluster(num_nodes=8)
    def test_pinning_with_group_notation(self):
        """
        Group notation {B, C, D} treats racks within braces as equal
        priority. Over many partitions, B, C, D should get roughly equal
        replica counts.

        8 nodes, 4 racks. rack_awareness=ON. 60 partitions, RF=3,
        preference="racks: A, {B, C, D}"
        Nodes: 2 in A; 2 in B; 2 in C; 2 in D
        """
        self._start_cluster(["A", "A", "B", "B", "C", "C", "D", "D"])

        num_partitions = 60
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            "pinning-groups",
            partitions=num_partitions,
            replicas=3,
            config={REPLICAS_PREFERENCE_KEY: "racks: A, {B, C, D}"},
        )

        self._wait_no_reconfigurations()

        n2r = self._node_id_to_rack()
        rc = self._get_multi_partition_rack_counts(
            "pinning-groups", n2r, num_partitions
        )

        self.logger.info(f"Rack counts: {rc}")

        count_a = rc.get("A", 0)
        count_b = rc.get("B", 0)
        count_c = rc.get("C", 0)
        count_d = rc.get("D", 0)

        # Rack A is highest priority; with rack awareness, each partition
        # gets one replica per rack, so A should get ~1 per partition.
        assert count_a >= num_partitions * 0.95, (
            f"Rack A should have >= {num_partitions * 0.95} replicas, got {count_a}"
        )

        # B, C, D are in the same group -- roughly equal share of non-A.
        non_a = count_b + count_c + count_d
        expected_per_rack = non_a / 3.0
        tolerance = 0.4  # 40% tolerance for statistical variation

        for rack, count in [("B", count_b), ("C", count_c), ("D", count_d)]:
            low = expected_per_rack * (1 - tolerance)
            high = expected_per_rack * (1 + tolerance)
            assert low <= count <= high, (
                f"Rack {rack} count {count} outside expected range "
                f"[{low:.0f}, {high:.0f}] (expected ~{expected_per_rack:.0f})"
            )

    # ------------------------------------------------------------------
    # Test 5: Fallback to unpreferred racks
    # ------------------------------------------------------------------
    @cluster(num_nodes=8)
    def test_pinning_fallback_to_unpreferred(self):
        """
        When the preference list has fewer racks than RF, the allocator
        falls back to unpreferred racks while still respecting rack awareness.

        4 nodes, 4 racks. rack_awareness=ON. RF=3, preference="racks: A"
        """
        self._start_cluster(["A", "B", "C", "D"])

        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            "pinning-fallback",
            partitions=1,
            replicas=3,
            config={REPLICAS_PREFERENCE_KEY: "racks: A"},
        )

        n2r = self._node_id_to_rack()
        rc = self._get_partition_rack_counts("pinning-fallback", n2r)

        self.logger.info(f"Rack counts: {rc}")
        assert rc.get("A", 0) == 1, (
            f"Expected 1 replica in rack A, got {rc.get('A', 0)}"
        )
        fallback_count = sum(rc.get(r, 0) for r in ["B", "C", "D"])
        assert fallback_count == 2, (
            f"Expected 2 replicas in fallback racks, got {fallback_count}"
        )
        for r in ["B", "C", "D"]:
            assert rc.get(r, 0) <= 1, (
                f"Rack {r} has {rc.get(r, 0)} replicas, expected at most 1"
            )

    # ------------------------------------------------------------------
    # Test 6: Balancer repairs pinning violation
    # ------------------------------------------------------------------
    @cluster(num_nodes=8)
    def test_balancer_repairs_pinning_violation(self):
        """
        After all nodes in the highest-priority rack go down and come back,
        the balancer moves replicas back to the preferred rack.

        5 nodes, 4 racks. rack_awareness=ON. RF=3, preference="racks: A, B, C"
        Nodes: 1,2 in rack A; 3 in rack B; 4 in rack C; 5 in rack D
        """
        self._start_cluster(["A", "A", "B", "C", "D"])

        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            "pinning-repair",
            partitions=1,
            replicas=3,
            config={REPLICAS_PREFERENCE_KEY: "racks: A, B, C"},
        )

        n2r = self._node_id_to_rack()
        rc = self._get_partition_rack_counts("pinning-repair", n2r)
        self.logger.info(f"Initial rack counts: {rc}")

        assert rc.get("A", 0) == 1
        assert rc.get("B", 0) == 1
        assert rc.get("C", 0) == 1
        assert rc.get("D", 0) == 0

        # Kill BOTH nodes in rack A
        node_a0 = self.redpanda.nodes[0]
        node_a1 = self.redpanda.nodes[1]
        self.redpanda.stop_node(node_a0)
        self.redpanda.stop_node(node_a1)

        # Wait for balancer to move A's replica to D
        def replicas_moved_to_d():
            try:
                rc = self._get_partition_rack_counts("pinning-repair", n2r)
                self.logger.info(f"After kill rack counts: {rc}")
                return rc.get("A", 0) == 0 and rc.get("D", 0) == 1
            except Exception:
                return False

        wait_until(
            replicas_moved_to_d,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="Balancer did not move replica from A to D",
        )

        # Restore both A nodes
        self.redpanda.start_node(node_a0)
        self.redpanda.start_node(node_a1)

        # Wait for balancer to repair: move replica from D back to A
        def replicas_back_in_abc():
            try:
                rc = self._get_partition_rack_counts("pinning-repair", n2r)
                self.logger.info(f"After restore rack counts: {rc}")
                return (
                    rc.get("A", 0) == 1
                    and rc.get("B", 0) == 1
                    and rc.get("C", 0) == 1
                    and rc.get("D", 0) == 0
                )
            except Exception:
                return False

        wait_until(
            replicas_back_in_abc,
            timeout_sec=180,
            backoff_sec=5,
            err_msg="Balancer did not repair pinning violation (D->A)",
        )

    # ------------------------------------------------------------------
    # Test 7: Alter replicas_preference
    # ------------------------------------------------------------------
    @cluster(num_nodes=8)
    def test_alter_replicas_preference(self):
        """
        Altering a topic's replicas_preference triggers the balancer to
        rebalance existing replicas. Removing preference stops enforcement.

        5 nodes, 4 racks. rack_awareness=ON. RF=3
        Nodes: 1 in A; 2 in B; 3 in C; 4 in D; 5 in D
        """
        self._start_cluster(["A", "B", "C", "D", "D"])

        rpk = RpkTool(self.redpanda)
        rpk.create_topic("pinning-alter", partitions=1, replicas=3)

        n2r = self._node_id_to_rack()
        rc = self._get_partition_rack_counts("pinning-alter", n2r)
        self.logger.info(f"Initial rack counts (no pref): {rc}")

        # Set replicas_preference to A, B, C
        rpk.alter_topic_config(
            "pinning-alter", REPLICAS_PREFERENCE_KEY, "racks: A, B, C"
        )

        # Wait for balancer to move replicas to A, B, C (and away from D)
        def replicas_in_abc():
            try:
                rc = self._get_partition_rack_counts("pinning-alter", n2r)
                self.logger.info(f"After alter rack counts: {rc}")
                return (
                    rc.get("A", 0) == 1
                    and rc.get("B", 0) == 1
                    and rc.get("C", 0) == 1
                    and rc.get("D", 0) == 0
                )
            except Exception:
                return False

        wait_until(
            replicas_in_abc,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="Balancer did not move replicas to A,B,C after alter",
        )

        rc_before = self._get_partition_rack_counts("pinning-alter", n2r)

        # Remove preference
        rpk.delete_topic_config("pinning-alter", REPLICAS_PREFERENCE_KEY)

        # Wait for several balancer tick intervals (5s each) to allow any
        # rebalancing to trigger if it were going to.
        time.sleep(15)

        # Verify no reconfigurations were started
        admin = Admin(self.redpanda)
        reconfigs = admin.list_reconfigurations()
        assert len(reconfigs) == 0, (
            f"Expected no reconfigurations after removing preference, "
            f"got {len(reconfigs)}"
        )

        rc_after = self._get_partition_rack_counts("pinning-alter", n2r)
        self.logger.info(f"After remove pref: before={rc_before}, after={rc_after}")
        assert rc_before == rc_after, (
            "Removing replicas_preference should not trigger rebalancing"
        )

    # ------------------------------------------------------------------
    # Test 8: Pinning with rackless nodes
    # ------------------------------------------------------------------
    @cluster(num_nodes=8)
    def test_pinning_with_rackless_nodes(self):
        """
        Nodes without a rack assignment are deprioritized (score 0).
        When preferred racks have capacity, rackless nodes get no replicas.

        4 nodes, 2 racks + 1 rackless. rack_awareness=ON. RF=3,
        preference="racks: A, B"
        Nodes: 1 in rack A; 2 in rack B; 3 in rack B; 4 has NO rack
        """
        self._start_cluster(["A", "B", "B", None])

        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            "pinning-rackless",
            partitions=1,
            replicas=3,
            config={REPLICAS_PREFERENCE_KEY: "racks: A, B"},
        )

        n2r = self._node_id_to_rack()
        rc = self._get_partition_rack_counts("pinning-rackless", n2r)

        self.logger.info(f"Rack counts: {rc}")
        assert rc.get("A", 0) == 1, (
            f"Expected 1 replica in rack A, got {rc.get('A', 0)}"
        )
        assert rc.get("B", 0) == 2, (
            f"Expected 2 replicas in rack B, got {rc.get('B', 0)}"
        )
        assert rc.get(None, 0) == 0, (
            f"Expected 0 replicas on rackless node, got {rc.get(None, 0)}"
        )

    # ------------------------------------------------------------------
    # Test 9a: Spread within rack -- rack awareness ON
    # ------------------------------------------------------------------
    @cluster(num_nodes=8)
    def test_spread_within_rack_awareness_on(self):
        """
        With rack awareness ON, each partition's RF=3 replicas land in
        3 distinct racks. Racks A and B are preferred (group 0), so they
        should each receive ~60 replicas. Rack C is overflow (group 1)
        and gets at most the remainder. Within A and B, replicas spread
        across the 3 nodes per rack (no node > 2x average).

        8 nodes: 3xA, 3xB, 2xC.  60 partitions, RF=3, pref="racks: A, B"
        Total replicas = 180.

        With rack awareness ON, each partition gets one replica per rack.
        Racks A and B are group 0 (preferred), rack C is group 1.
        With 3 racks available and RF=3, ideal per partition = {A, B, C}.
        So racks A and B each get ~60 replicas (one per partition), and
        rack C gets ~60 as overflow.

        Due to internal topic partitions, actual counts may deviate slightly.
        We allow JITTER_PERCENT deviation from the expected counts.

        Expected: A ~60, B ~60, C ~60 (bounded by jitter).
        """
        self._start_cluster(
            ["A", "A", "A", "B", "B", "B", "C", "C"], rack_awareness=True
        )

        num_partitions = 60
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            "spread-rack-on",
            partitions=num_partitions,
            replicas=3,
            config={REPLICAS_PREFERENCE_KEY: "racks: A, B"},
        )

        self._wait_no_reconfigurations()

        n2r = self._node_id_to_rack()
        rack_counts = self._get_multi_partition_rack_counts(
            "spread-rack-on", n2r, num_partitions
        )
        node_counts = self._get_multi_partition_node_counts(
            "spread-rack-on", num_partitions
        )

        self.logger.info(f"Rack counts: {rack_counts}")
        self.logger.info(f"Node counts: {node_counts}")

        count_a = rack_counts.get("A", 0)
        count_b = rack_counts.get("B", 0)
        count_c = rack_counts.get("C", 0)

        # Ideal: each partition gets one replica per rack -> 60 per rack.
        expected_per_rack = num_partitions  # 60
        jitter = int(expected_per_rack * JITTER_PERCENT)

        assert count_a >= expected_per_rack - jitter, (
            f"Expected rack A >= {expected_per_rack - jitter} replicas, got {count_a}"
        )
        assert count_b >= expected_per_rack - jitter, (
            f"Expected rack B >= {expected_per_rack - jitter} replicas, got {count_b}"
        )
        assert count_c <= expected_per_rack + jitter, (
            f"Expected rack C <= {expected_per_rack + jitter} replicas, got {count_c}"
        )

        # Within A and B, no node has > 2x the average
        for rack in ["A", "B"]:
            rack_node_ids = [nid for nid, r in n2r.items() if r == rack]
            if not rack_node_ids:
                continue
            counts = [node_counts.get(nid, 0) for nid in rack_node_ids]
            avg = sum(counts) / len(counts)
            if avg == 0:
                continue
            for nid, count in zip(rack_node_ids, counts):
                assert count <= avg * 2.0, (
                    f"Node {nid} in rack {rack} has {count} replicas, "
                    f"more than 2.0x the average ({avg:.1f})"
                )

    # ------------------------------------------------------------------
    # Test 9b: Spread within rack -- rack awareness OFF
    # ------------------------------------------------------------------
    @cluster(num_nodes=8)
    def test_spread_within_rack_awareness_off(self):
        """
        With rack awareness OFF, the allocator ignores rack diversity.
        Rack A has 4 nodes (capacity 4 in group 0), so nearly all 180
        replicas should land in rack A. Rack B (group 1) and C (group 2)
        are overflow only.

        7 nodes: 4xA, 2xB, 1xC.  60 partitions, RF=3, pref="racks: A, B"
        Total replicas = 180.

        With rack awareness OFF, capacity of group 0 (rack A) = 4 nodes.
        RF=3 fits entirely in group 0, so ideal = {A, A, A}.
        Nearly all 180 replicas should land in rack A. B and C get only
        residual replicas from internal topics.

        Expected: A ~180, B ~0, C ~0 (with jitter from internal topics).
        """
        self._start_cluster(["A", "A", "A", "A", "B", "B", "C"], rack_awareness=False)

        num_partitions = 60
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            "spread-rack-off",
            partitions=num_partitions,
            replicas=3,
            config={REPLICAS_PREFERENCE_KEY: "racks: A, B"},
        )

        self._wait_no_reconfigurations()

        n2r = self._node_id_to_rack()
        rack_counts = self._get_multi_partition_rack_counts(
            "spread-rack-off", n2r, num_partitions
        )
        node_counts = self._get_multi_partition_node_counts(
            "spread-rack-off", num_partitions
        )

        self.logger.info(f"Rack counts: {rack_counts}")
        self.logger.info(f"Node counts: {node_counts}")

        total_replicas = num_partitions * 3  # 180
        count_a = rack_counts.get("A", 0)
        count_b = rack_counts.get("B", 0)
        count_c = rack_counts.get("C", 0)

        # Ideal: all 180 replicas in A. Internal topics add jitter.
        jitter = int(total_replicas * JITTER_PERCENT)

        assert count_a >= total_replicas - jitter, (
            f"Expected rack A >= {total_replicas - jitter} of "
            f"{total_replicas} replicas, got {count_a}"
        )
        assert count_b <= jitter, f"Expected rack B <= {jitter} replicas, got {count_b}"
        assert count_c <= jitter, f"Expected rack C <= {jitter} replicas, got {count_c}"

        # Within rack A, no node has > 2x the average
        rack_a_node_ids = [nid for nid, r in n2r.items() if r == "A"]
        counts_a = [node_counts.get(nid, 0) for nid in rack_a_node_ids]
        avg_a = sum(counts_a) / len(counts_a) if counts_a else 0
        if avg_a > 0:
            for nid, count in zip(rack_a_node_ids, counts_a):
                assert count <= avg_a * 2.0, (
                    f"Node {nid} in rack A has {count} replicas, "
                    f"more than 2.0x the average ({avg_a:.1f})"
                )

        # Every rack-A node should have meaningful load.
        avg_per_node = total_replicas / len(rack_a_node_ids)
        min_per_node = int(avg_per_node * (1.0 - JITTER_PERCENT))
        for nid, count in zip(rack_a_node_ids, counts_a):
            assert count >= min_per_node, (
                f"Node {nid} in rack A has only {count} replicas, "
                f"expected >= {min_per_node}"
            )
