# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import collections
from dataclasses import dataclass
from enum import Enum
import math
import random
import time
from typing import Any, TypedDict, cast

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, ResourceSettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result


class ReplicaMetadata(TypedDict):
    id: int


class BrokerMetadata(TypedDict):
    id: int


class PartitionMetadata(TypedDict):
    partition: int
    leader: int
    replicas: list[ReplicaMetadata]


class TopicMetadata(TypedDict):
    topic: str
    partitions: list[PartitionMetadata]


class KafkaMetadata(TypedDict):
    brokers: list[BrokerMetadata]
    topics: list[TopicMetadata]


class LeadershipTransferTest(RedpandaTest):
    """
    Transfer leadership from one node to another.
    """

    topics = (TopicSpec(partition_count=3, replication_factor=3),)

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(LeadershipTransferTest, self).__init__(
            *args,
            extra_rp_conf={
                # Disable leader balancer, as this test is doing its own
                # partition movement and the balancer would interfere
                "enable_leader_balancer": False
            },
            **kwargs,
        )

    @cluster(num_nodes=3)
    def test_controller_recovery(self) -> None:
        kc = KafkaCat(self.redpanda)

        # choose a partition and a target node
        partition = self._get_partition(kc)
        target_node_id = next(
            replica["id"]
            for replica in partition["replicas"]
            if replica["id"] != partition["leader"]
        )
        self.logger.debug(
            f"Transfering leader from {partition['leader']} to {target_node_id}"
        )

        # build the transfer url
        meta = cast(KafkaMetadata, kc.metadata())
        brokers = meta["brokers"]
        source_broker = next(
            broker for broker in brokers if broker["id"] == partition["leader"]
        )
        target_broker = next(
            broker for broker in brokers if broker["id"] == target_node_id
        )
        self.logger.debug(f"Source broker {source_broker}")
        self.logger.debug(f"Target broker {target_broker}")

        # Send the request to any host, they should redirect to
        # the leader of the partition.
        partition_id = partition["partition"]

        admin = Admin(self.redpanda)
        admin.partition_transfer_leadership(
            "kafka", self.topic, partition_id, target_node_id
        )

        def transfer_complete() -> bool:
            for _ in range(3):  # just give it a moment
                time.sleep(1)
                meta = cast(KafkaMetadata, kc.metadata())
                partition = next(
                    topic_partition
                    for topic_partition in meta["topics"][0]["partitions"]
                    if topic_partition["partition"] == partition_id
                )
                if partition["leader"] == target_node_id:
                    return True
            return False

        wait_until(
            lambda: transfer_complete(),
            timeout_sec=30,
            backoff_sec=5,
            err_msg="Transfer did not complete",
        )

    def _get_partition(self, kc: KafkaCat) -> PartitionMetadata:
        def get_partition() -> tuple[bool, PartitionMetadata]:
            meta = cast(KafkaMetadata, kc.metadata())
            topics = meta["topics"]
            assert len(topics) == 1
            assert topics[0]["topic"] == self.topic
            partition = random.choice(topics[0]["partitions"])
            return partition["leader"] > 0, partition

        return cast(
            PartitionMetadata,
            wait_until_result(
                get_partition,
                timeout_sec=30,
                backoff_sec=2,
                err_msg="No partition with leader available",
            ),
        )

    @cluster(num_nodes=3)
    def test_self_transfer(self) -> None:
        admin = Admin(self.redpanda)
        for topic in self.topics:
            for partition in range(topic.partition_count):
                leader = admin.get_partitions(topic.name, partition)["leader_id"]
                admin.partition_transfer_leadership(
                    "kafka", topic.name, partition, leader
                )


class TopicAwareRebalanceTestBase(RedpandaTest):
    def __init__(
        self,
        test_context: TestContext,
        mode: str,
        topic_specs: list[TopicSpec],
        leaders_per_node_ratio: float = 0.8,
        improvement_deadline: int = 70,
        require_even_distribution: bool = False,
        **kwargs: Any,
    ):
        extra_rp_conf: dict[str, Any] = dict(
            leader_balancer_idle_timeout=20000,
            leader_balancer_mode=mode,
        )

        super().__init__(
            test_context=test_context, extra_rp_conf=extra_rp_conf, **kwargs
        )
        self.topics = topic_specs
        self._leaders_per_node_ratio = leaders_per_node_ratio
        self._improvement_deadline = improvement_deadline
        self._require_even_distribution = require_even_distribution

    def _do_test_topic_aware_rebalance(self, num_nodes: int = 3):
        def all_partitions_present(nodes: int):
            for t in self.topics:
                tps = self.redpanda.partitions(t.name)
                total_leaders = sum(1 if t.leader else 0 for t in tps)
                total_nodes = set(t.leader for t in tps if t.leader)

                self.logger.debug(
                    f"Topic: {t.name}, total_leaders: {total_leaders}, total_nodes: {len(total_nodes)}"
                )

                if len(total_nodes) < nodes:
                    self.logger.debug(
                        f"Fewer nodes: {len(total_nodes)} than expected: {nodes}"
                    )
                    return False

                if total_leaders != t.partition_count:
                    self.logger.debug(
                        f"Fewer leaders: {total_leaders} than expected: {t.partition_count}"
                    )
                    missing_leaders = [
                        f"{t.topic}/{t.index}" for t in tps if not t.leader
                    ]
                    self.logger.debug(f"partitions without leaders: {missing_leaders}")
                    return False

            return True

        def count_leaders_per_node(topic_name: str) -> dict[int, int]:
            leaders_per_node: dict[int, int] = collections.defaultdict(int)
            tps = self.redpanda.partitions(topic_name)
            for p in tps:
                if p.leader:
                    leaders_per_node[self.redpanda.node_id(p.leader)] += 1

            return leaders_per_node

        def distribution_error() -> float:
            nodes = [self.redpanda.node_id(n) for n in self.redpanda.started_nodes()]
            error = 0.0
            for t in self.topics:
                leaders_per_node = count_leaders_per_node(topic_name=t.name)
                opt_leaders = t.partition_count / len(nodes)

                for n in nodes:
                    if n in leaders_per_node:
                        error += (opt_leaders - leaders_per_node[n]) ** 2
                    else:
                        error += opt_leaders**2

            return error

        def has_leader_count(topic_name: str, min_per_node: int, nodes: int) -> bool:
            leaders_per_node = count_leaders_per_node(topic_name)

            if len(set(leaders_per_node)) < nodes:
                return False

            self.logger.info(f"{topic_name} has dist {leaders_per_node.values()}")
            return all(
                leader_cnt >= min_per_node for leader_cnt in leaders_per_node.values()
            )

        def topic_leadership_evenly_distributed() -> bool:
            for t in self.topics:
                expected_leaders_per_node = int(
                    self._leaders_per_node_ratio * (t.partition_count / num_nodes)
                )
                self.logger.info(
                    f"for topic {t} expecting {expected_leaders_per_node} leaders"
                )

                if not has_leader_count(t.name, expected_leaders_per_node, num_nodes):
                    return False

            return True

        self.logger.info("initial stabilization")
        wait_until(
            lambda: all_partitions_present(num_nodes),
            timeout_sec=120,
            backoff_sec=2,
            err_msg="Leadership did not stablize",
        )

        node = self.redpanda.nodes[0]
        self.redpanda.stop_node(node)
        self.logger.info("stabilization post stop")
        wait_until(
            lambda: all_partitions_present(num_nodes - 1),
            timeout_sec=120,
            backoff_sec=2,
            err_msg="Leadership did not stablize",
        )

        # sleep for a bit to avoid triggering any of the sticky leaderhsip
        # optimizations
        time.sleep(60)

        start_timeout = None
        if self.debug_mode:
            # Due to the high partition count in this test Redpanda
            # can take longer than the default 20s to start on a debug
            # release.
            start_timeout = 60
        self.redpanda.start_node(node, timeout=start_timeout)

        admin = Admin(self.redpanda)

        def cluster_healthy() -> bool:
            try:
                overview = admin.get_cluster_health_overview()
                return overview.get("is_healthy", False)
            except Exception:
                return False

        self.logger.info("waiting for healthy cluster post start")
        wait_until(
            cluster_healthy,
            timeout_sec=120,
            backoff_sec=2,
            err_msg="Cluster did not become healthy after restart",
        )

        self.logger.info("waiting for leadership to stabilize post start")
        wait_until(
            lambda: all_partitions_present(num_nodes),
            timeout_sec=120,
            backoff_sec=2,
            err_msg="Leadership did not stablize after restart",
        )

        def wait_for_topics_evenly_distributed(improvement_deadline: int) -> bool:
            last_update = time.time()
            last_error = distribution_error()
            while time.time() - last_update < improvement_deadline:
                if topic_leadership_evenly_distributed():
                    return True
                current_error = distribution_error()
                self.logger.debug(
                    f"current distribution error: {current_error}, previous error: {last_error}, last improvement update: {last_update}"
                )
                if current_error < last_error:
                    last_update = time.time()
                    last_error = current_error

                time.sleep(5)

            return False

        self.logger.info("stabilization post start")
        balanced = wait_for_topics_evenly_distributed(self._improvement_deadline)
        if self._require_even_distribution:
            assert balanced


class MultiTopicAutomaticLeadershipBalancingTest(TopicAwareRebalanceTestBase):
    def __init__(self, test_context: TestContext):
        topics = [
            TopicSpec(partition_count=61, replication_factor=3),
            TopicSpec(partition_count=151, replication_factor=3),
        ]
        super().__init__(
            test_context,
            mode="random_hill_climbing",
            topic_specs=topics,
        )
        # Avoid the largest topic in debug builds where it is slow.
        if not self.debug_mode:
            assert isinstance(self.topics, list)
            self.topics.append(TopicSpec(partition_count=263, replication_factor=3))

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_topic_aware_rebalance(self):
        self._do_test_topic_aware_rebalance()


class GreedyLeaderBalancingTest(TopicAwareRebalanceTestBase):
    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context,
            mode="greedy",
            topic_specs=[
                TopicSpec(partition_count=9, replication_factor=3),
                TopicSpec(partition_count=9, replication_factor=3),
            ],
            leaders_per_node_ratio=1.0,
            improvement_deadline=120,
            require_even_distribution=True,
        )

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_greedy_rebalance(self):
        self._do_test_topic_aware_rebalance()


class GreedyLeaderBalancingFourShardsTest(TopicAwareRebalanceTestBase):
    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context,
            mode="greedy",
            topic_specs=[],
            leaders_per_node_ratio=1.0,
            improvement_deadline=120,
            require_even_distribution=True,
            resource_settings=ResourceSettings(num_cpus=4),
        )

    def _run_scenario(self, specs: list[TopicSpec]):
        self.topics = specs
        for s in specs:
            self.client().create_topic(s)
        self._do_test_topic_aware_rebalance()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_one_topic_18p(self):
        self._run_scenario([TopicSpec(partition_count=18, replication_factor=3)])

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_one_topic_12p(self):
        self._run_scenario([TopicSpec(partition_count=12, replication_factor=3)])

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_two_topics_18p(self):
        self._run_scenario(
            [
                TopicSpec(partition_count=18, replication_factor=3),
                TopicSpec(partition_count=18, replication_factor=3),
            ]
        )


class AutomaticLeadershipBalancingTest(RedpandaTest):
    # number cores = 3 (default)
    # number nodes = 3 (default)
    # parts per core = 7
    topics = (TopicSpec(partition_count=63, replication_factor=3),)

    def __init__(self, test_context: Any) -> None:
        extra_rp_conf = dict(
            leader_balancer_idle_timeout=20000,
        )

        super(AutomaticLeadershipBalancingTest, self).__init__(
            test_context=test_context, extra_rp_conf=extra_rp_conf
        )

    def _get_leaders_by_node(self) -> collections.Counter[int]:
        kc = KafkaCat(self.redpanda)
        md = cast(KafkaMetadata, kc.metadata())
        topic = next(
            metadata_topic
            for metadata_topic in md["topics"]
            if metadata_topic["topic"] == self.topic
        )
        leaders = (p["leader"] for p in topic["partitions"])
        return collections.Counter(leaders)

    def _get_leaders_by_shard(self) -> dict[tuple[int, int], int]:
        admin = Admin(self.redpanda)
        shard2count: dict[tuple[int, int], int] = {}
        for n in self.redpanda.started_nodes():
            node_id = self.redpanda.node_id(n)
            partitions = admin.get_partitions(node=n)
            for p in partitions:
                if p.get("leader") == node_id:
                    shard = (node_id, cast(int, p["core"]))
                    shard2count[shard] = shard2count.setdefault(shard, 0) + 1
        return shard2count

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_automatic_rebalance(self) -> None:
        def all_partitions_present(num_nodes: int, per_node: int | None = None) -> bool:
            leaders = self._get_leaders_by_node()
            for l in leaders:
                self.redpanda.logger.debug(f"Leaders on {l}: {leaders[l]}")
            count = sum(leaders[l] for l in leaders)
            total = len(leaders) == num_nodes and count == 63
            if per_node is not None:
                per_node_sat = all((leaders[l] > per_node for l in leaders))
                return total and per_node_sat
            return total

        # wait until all the partition leaders are elected on all three nodes
        wait_until(
            lambda: all_partitions_present(3),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Leadership did not stablize",
        )

        # stop node and wait for all leaders to transfer
        # to another node
        node = self.redpanda.nodes[0]
        self.redpanda.stop_node(node)
        wait_until(
            lambda: all_partitions_present(2),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Leadership did not move to running nodes",
        )

        # sleep for a bit to avoid triggering any of the sticky leaderhsip
        # optimizations
        time.sleep(60)

        # sanity check -- the node we stopped shouldn't be a leader for any
        # partition after the sleep above as releection should have taken place
        leaders = self._get_leaders_by_node()
        assert self.redpanda.node_id(node) not in leaders

        # restart the stopped node and wait for 15 (out of 21) leaders to be
        # rebalanced on to the node. the error minimization done in the leader
        # balancer is a little fuzzy so it problematic to assert an exact target
        # number that should return
        self.redpanda.start_node(node)
        wait_until(
            lambda: all_partitions_present(3, 15),
            timeout_sec=300,
            backoff_sec=10,
            err_msg="Leadership did not stablize",
        )

        shard2leaders = self._get_leaders_by_shard()
        self.redpanda.logger.debug(f"Leaders by shard: {shard2leaders}")
        expected_on_shard = sum(shard2leaders.values()) / len(shard2leaders)
        for s, count in shard2leaders.items():
            expected_min = math.floor(expected_on_shard * 0.8)
            assert count >= expected_min, (
                f"leader count on shard {s} ({count}) is < {expected_min}"
            )


class Ordering(str, Enum):
    ordered = "ordered"
    unordered = "unordered"


@dataclass
class LeaderPinningConfig:
    ordering: Ordering

    @property
    def is_ordered(self) -> bool:
        return self.ordering == Ordering.ordered

    @property
    def preference_str(self) -> str:
        if self.is_ordered:
            return "ordered_racks"
        return "racks"


@dataclass
class RackPreference:
    """Encodes expected rack placement for a topic's leaders."""

    racks: list[str]
    is_ordered: bool


class LeadershipPinningTest(RedpandaTest):
    def __init__(self, test_context: Any) -> None:
        super(LeadershipPinningTest, self).__init__(
            test_context=test_context,
            num_brokers=6,
            extra_rp_conf={
                "enable_rack_awareness": True,
                # In this test we kill nodes. Leader balancer might try
                # transferring leaders to those nodes before it realizes
                # that they are offline. To avoid muting corresponding groups
                # for long (which might prevent reaching balancing goals),
                # lower the mute timeout.
                "leader_balancer_mute_timeout": 20000,
            },
        )

    def setUp(self) -> None:
        pass

    rack_layout: list[str] = ["A", "A", "B", "B", "C", "C"]

    def _get_topic2node2leaders(self) -> dict[str, dict[int, int]]:
        kc = KafkaCat(self.redpanda)
        md = cast(KafkaMetadata, kc.metadata())
        ret: dict[str, dict[int, int]] = {}
        for topic in md["topics"]:
            name = topic["topic"]
            node2leaders = dict(
                collections.Counter(p["leader"] for p in topic["partitions"])
            )
            self.logger.debug(f"topic {name} leaders: {sorted(node2leaders.items())}")

            ret[name] = node2leaders
        return ret

    def _rack_counts(self, node_counts: dict[int, int]) -> dict[str, int]:
        rack2count: dict[str, int] = {}
        for ix, node in enumerate(self.redpanda.nodes):
            node_id = self.redpanda.node_id(node)
            leaders = node_counts.get(node_id, 0)
            if leaders > 0:
                rack = self.rack_layout[ix]
                rack2count[rack] = rack2count.setdefault(rack, 0) + leaders
        return rack2count

    def _get_topic2racks(self) -> dict[str, set[str]]:
        t2n2l = self._get_topic2node2leaders()
        return {
            topic: set(self._rack_counts(node2leaders).keys())
            for topic, node2leaders in t2n2l.items()
        }

    def wait_for_racks(
        self,
        partition_counts: dict[str, int],
        topic2expected_racks: dict[str, RackPreference],
        dead_racks: list[str] | None = None,
        check_balance: bool = True,
        timeout_sec: int = 60,
    ) -> None:
        """
        wait for leadership balance with rack placement.
        balance is:
        1. LEADER COUNT: all partitions in all topics have leaders

        2. RACK PLACEMENT: leadership is on a favored rack

        3. LOAD BALANCE (optional): checked when check_balance is true. once ntp leaders are on their preferred rack, check that it balances the number of leaders per node within the rack

        Args:
            partition_counts: Dict mapping topic names to expected partition counts
                             e.g., {"foo": 60, "bar": 20}
            topic2expected_racks: Dict mapping topic names to RackPreference instances
                                 encoding expected racks and whether the preference is ordered
            dead_racks: racks that are currently down, filtered from each
                        topic's expected racks before validation
            check_balance: If True, also verify even distribution within racks
            timeout_sec: Maximum time to wait for conditions to be met (default: 60)

        Raises:
            TimeoutError: If the conditions are not met within timeout_sec
        """
        excluded = set(dead_racks) if dead_racks else set[str]()

        def predicate() -> bool:
            t2n2l = self._get_topic2node2leaders()

            def check_leader_placement(
                is_ordered: bool, rack_preference: list[str], found_racks: set[str]
            ) -> bool:
                if not is_ordered:
                    # all found racks should be within the rack preference
                    return found_racks == set(rack_preference)
                else:
                    # otherwise, all leaders should accumulate on the first rack
                    highest_priority = rack_preference[0]
                    return len(found_racks) == 1 and highest_priority in found_racks

            for topic, expected_count in partition_counts.items():
                node2leaders = t2n2l.get(topic, dict())

                count = sum(node2leaders.values())
                if count != expected_count:
                    self.logger.debug(
                        f"not all leaders for topic {topic} present, "
                        f"expected {expected_count}, got {count}"
                    )
                    return False

                pref = topic2expected_racks.get(
                    topic, RackPreference(racks=[], is_ordered=False)
                )
                effective_racks = [r for r in pref.racks if r not in excluded]
                rack2leaders = self._rack_counts(node2leaders)

                if not check_leader_placement(
                    pref.is_ordered, effective_racks, set(rack2leaders.keys())
                ):
                    self.logger.debug(
                        f"leader rack expectations failed for topic {topic}, is_ordered: {pref.is_ordered} "
                        f"expected: {effective_racks}, actual: {list(rack2leaders.keys())}"
                    )
                    return False

                if check_balance:
                    nonzero_counts = [l for l in node2leaders.values() if l > 0]
                    if min(nonzero_counts) + 2 < max(nonzero_counts):
                        self.logger.debug(
                            f"leader counts unbalanced for topic {topic}: "
                            f"{sorted(node2leaders.items())}"
                        )
                        return False

            return True

        wait_until(predicate, timeout_sec=timeout_sec, backoff_sec=5)

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_ordered_leader_pinning_failover(self):
        """
        check that ordered pinning will failover to the next highest priority
        and return to the highest priority if and when it again becomes available
        """
        rack_layout = ["A", "A", "B", "C", "D", "E"]
        self.rack_layout = rack_layout

        for ix, node in enumerate(self.redpanda.nodes):
            self.redpanda.set_extra_node_conf(node, {"rack": rack_layout[ix]})
        self.redpanda.start()

        # Decrease idle timeout to not wait too long after nodes are killed
        self.redpanda.set_cluster_config({"enable_leader_balancer": False})
        self.redpanda.set_cluster_config({"leader_balancer_idle_timeout": 20000})
        self.redpanda.set_cluster_config({"enable_leader_balancer": True})

        rpk = RpkTool(self.redpanda)
        topic = "test-topic"
        partitions = 60
        partition_counts = {topic: partitions}
        preference = ["E", "D", "C", "B", "A"]

        rpk.create_topic(
            topic,
            partitions=partitions,
            replicas=5,
            config={
                "redpanda.leaders.preference": "ordered_racks: " + ", ".join(preference)
            },
        )

        topic_pref = {topic: RackPreference(racks=preference, is_ordered=True)}

        self.wait_for_racks(
            partition_counts,
            topic_pref,
            timeout_sec=90,
        )

        node_e = self.redpanda.nodes[rack_layout.index("E")]
        self.redpanda.stop_node(node_e)
        self.wait_for_racks(
            partition_counts,
            topic_pref,
            dead_racks=["E"],
            timeout_sec=60,
        )

        node_d = self.redpanda.nodes[rack_layout.index("D")]
        self.redpanda.stop_node(node_d)
        self.wait_for_racks(
            partition_counts,
            topic_pref,
            dead_racks=["E", "D"],
            timeout_sec=60,
        )

        self.redpanda.start_node(node_e)
        self.wait_for_racks(
            partition_counts,
            topic_pref,
            dead_racks=["D"],
            timeout_sec=90,
        )

        node_b = self.redpanda.nodes[rack_layout.index("B")]
        self.redpanda.stop_node(node_b)
        self.wait_for_racks(
            partition_counts,
            topic_pref,
            dead_racks=["D", "B"],
            timeout_sec=60,
        )

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_ordered_unordered_swap(self):
        """
        1. ordered, check ordered leadership
        2. swap to unordered, check unordered leadership
        3. swap to ordered, check ordered leadership
        """
        for ix, node in enumerate(self.redpanda.nodes):
            self.redpanda.set_extra_node_conf(node, {"rack": self.rack_layout[ix]})
        self.redpanda.start()

        self.redpanda.set_cluster_config({"enable_leader_balancer": False})
        self.redpanda.set_cluster_config({"leader_balancer_idle_timeout": 20000})
        self.redpanda.set_cluster_config({"enable_leader_balancer": True})

        rpk = RpkTool(self.redpanda)
        topic = "test-topic"
        partitions = 60
        partition_counts = {topic: partitions}

        rpk.create_topic(
            topic,
            partitions=partitions,
            replicas=3,
            config={"redpanda.leaders.preference": "ordered_racks: A, B"},
        )

        self.wait_for_racks(
            partition_counts,
            {topic: RackPreference(racks=["A", "B"], is_ordered=True)},
            timeout_sec=90,
        )

        rpk.alter_topic_config(topic, "redpanda.leaders.preference", "racks: A, B")

        self.wait_for_racks(
            partition_counts,
            {topic: RackPreference(racks=["A", "B"], is_ordered=False)},
            timeout_sec=60,
        )

        rpk.alter_topic_config(
            topic, "redpanda.leaders.preference", "ordered_racks: A, B"
        )

        self.wait_for_racks(
            partition_counts,
            {topic: RackPreference(racks=["A", "B"], is_ordered=True)},
            timeout_sec=60,
        )

        rpk.alter_topic_config(topic, "redpanda.leaders.preference", "racks: A, B")

        self.wait_for_racks(
            partition_counts,
            {topic: RackPreference(racks=["A", "B"], is_ordered=False)},
            timeout_sec=60,
        )

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(ordering=[Ordering.ordered, Ordering.unordered])
    def test_leadership_pinning(self, ordering: Ordering):
        """
        test overview:
        1. set a default of A
        2.
            foo: default, bar: C
            check foo on A bar on C
        3.
            foo: default, bar: B -> C
            check foo on A, bar matches ordered or unordered behavior
        4.
            kill all nodes in rack B
            check foo on A, bar on C
        5.
            foo: default, bar: default
            check foo and bar on A
        6. restart B
        7.
            default is now none
            check foo and bar spread across A, B, C
        """
        config = LeaderPinningConfig(ordering)

        for ix, node in enumerate(self.redpanda.nodes):
            self.redpanda.set_extra_node_conf(
                node,
                {
                    "rack": self.rack_layout[ix],
                },
            )
        self.redpanda.add_extra_rp_conf(
            {"default_leaders_preference": f"{config.preference_str}: A"}
        )
        self.redpanda.start()

        rpk = RpkTool(self.redpanda)

        partition_counts = {"foo": 60, "bar": 20}

        self.logger.info("creating topics")

        # foo receives default preference which is A
        rpk.create_topic("foo", partitions=60, replicas=3)

        # bar receives a preference of C
        rpk.create_topic(
            "bar",
            partitions=20,
            replicas=3,
            config={"redpanda.leaders.preference": f"{config.preference_str}: C"},
        )

        ordered = config.is_ordered
        all_racks = sorted(set(self.rack_layout))

        # bigger timeout to allow balancer to activate, health reports to propagate, etc.
        self.wait_for_racks(
            partition_counts,
            {
                "foo": RackPreference(racks=["A"], is_ordered=ordered),
                "bar": RackPreference(racks=["C"], is_ordered=ordered),
            },
            timeout_sec=90,
        )

        self.logger.info("altering topic preference")

        rpk.alter_topic_config(
            "bar", "redpanda.leaders.preference", f"{config.preference_str}: B, C"
        )

        self.wait_for_racks(
            partition_counts,
            {
                "foo": RackPreference(racks=["A"], is_ordered=ordered),
                "bar": RackPreference(racks=["B", "C"], is_ordered=ordered),
            },
            timeout_sec=60,
        )

        # Decrease idle timeout to not wait too long after nodes are killed
        self.redpanda.set_cluster_config({"enable_leader_balancer": False})
        self.redpanda.set_cluster_config({"leader_balancer_idle_timeout": 20000})
        self.redpanda.set_cluster_config({"enable_leader_balancer": True})

        self.logger.info("killing rack B")

        for ix, node in enumerate(self.redpanda.nodes):
            if self.rack_layout[ix] == "B":
                self.redpanda.stop_node(node)

        self.wait_for_racks(
            partition_counts,
            {
                "foo": RackPreference(racks=["A"], is_ordered=ordered),
                "bar": RackPreference(racks=["B", "C"], is_ordered=ordered),
            },
            dead_racks=["B"],
            timeout_sec=60,
        )

        self.logger.info("explicitly disabling for topic")
        rpk.alter_topic_config("foo", "redpanda.leaders.preference", "none")

        # foo's preference is now "none" so it is unordered regardless of test
        # config. There is cross-talk between partition counts of foo and bar,
        # so we don't require balanced counts.
        self.wait_for_racks(
            partition_counts,
            {
                "foo": RackPreference(racks=all_racks, is_ordered=False),
                "bar": RackPreference(racks=["B", "C"], is_ordered=ordered),
            },
            dead_racks=["B"],
            check_balance=False,
            timeout_sec=60,
        )

        self.logger.info("unset topic configs")

        # default preference is governing which is still A
        rpk.delete_topic_config("foo", "redpanda.leaders.preference")
        rpk.delete_topic_config("bar", "redpanda.leaders.preference")

        self.wait_for_racks(
            partition_counts,
            {
                "foo": RackPreference(racks=["A"], is_ordered=ordered),
                "bar": RackPreference(racks=["A"], is_ordered=ordered),
            },
            dead_racks=["B"],
            timeout_sec=60,
        )

        self.logger.info("unset default preference")

        for ix, node in enumerate(self.redpanda.nodes):
            if self.rack_layout[ix] == "B":
                self.redpanda.start_node(node)

        # regardless of test config, no preference is semantically equivalent to unordered with all racks defined, check as such
        self.redpanda.set_cluster_config({"default_leaders_preference": "none"})
        self.wait_for_racks(
            partition_counts,
            {
                "foo": RackPreference(racks=all_racks, is_ordered=False),
                "bar": RackPreference(racks=all_racks, is_ordered=False),
            },
            check_balance=False,
            timeout_sec=90,
        )

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(ordering=[Ordering.ordered, Ordering.unordered])
    def test_leadership_pinning_reporting(self, ordering: Ordering):
        config = LeaderPinningConfig(ordering)
        for ix, node in enumerate(self.redpanda.nodes):
            self.redpanda.set_extra_node_conf(
                node,
                {
                    "rack": self.rack_layout[ix],
                },
            )

        rpk = RpkTool(self.redpanda)
        admin = Admin(self.redpanda)

        def get_leadership_pinning_status():
            features = admin.get_enterprise_features().json()["features"]
            for f in features:
                if f["name"] == "leadership_pinning":
                    return f["enabled"]

        # no default leaders preference and no topic leaders preference
        self.redpanda.start()
        rpk.create_topic("foo", partitions=60, replicas=3)

        assert not get_leadership_pinning_status(), (
            "Leadership pinning reported enabled while is it disabled"
        )

        # no default leaders preference and no topic leaders preference
        # but topic leaders preference is defined
        self.redpanda.start()
        rpk.create_topic(
            "foo",
            partitions=60,
            replicas=3,
            config={"redpanda.leaders.preference": "none"},
        )

        assert not get_leadership_pinning_status(), (
            "Leadership pinning reported enabled while is it disabled"
        )

        # topic leaders preference defined
        self.redpanda.start()
        rpk.create_topic(
            "foo",
            partitions=60,
            replicas=3,
            config={"redpanda.leaders.preference": f"{config.preference_str}: C"},
        )

        assert get_leadership_pinning_status(), (
            "Leadership pinning reported disabled while is it enabled"
        )

        # default leaders preference defined as A
        self.redpanda.start()
        self.redpanda.set_cluster_config(
            {"default_leaders_preference": f"{config.preference_str}: A"}
        )
        rpk.create_topic("foo", partitions=60, replicas=3)

        assert get_leadership_pinning_status(), (
            "Leadership pinning reported disabled while is it enabled"
        )

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @matrix(ordering=[Ordering.ordered, Ordering.unordered])
    def test_leadership_pinning_sanctions(self, ordering: Ordering):
        config = LeaderPinningConfig(ordering)
        for ix, node in enumerate(self.redpanda.nodes):
            self.redpanda.set_extra_node_conf(
                node,
                {
                    "rack": self.rack_layout[ix],
                },
            )
        self.redpanda.add_extra_rp_conf(
            {"default_leaders_preference": f"{config.preference_str}: A"}
        )
        self.redpanda.start()

        rpk = RpkTool(self.redpanda)

        def get_leaders_preference(topic: str) -> str:
            config = cast(dict[str, tuple[str, Any]], rpk.describe_topic_configs(topic))
            return config["redpanda.leaders.preference"][0]

        partition_counts = {"foo": 60, "bar": 20}

        self.logger.info("creating topics")

        rpk.create_topic("foo", partitions=60, replicas=3)
        rpk.create_topic(
            "bar",
            partitions=20,
            replicas=3,
            config={"redpanda.leaders.preference": f"{config.preference_str}: C"},
        )

        # bigger timeout to allow balancer to activate, health reports to propagate, etc.
        self.wait_for_racks(
            partition_counts,
            {
                "foo": RackPreference(racks=["A"], is_ordered=config.is_ordered),
                "bar": RackPreference(racks=["C"], is_ordered=config.is_ordered),
            },
            timeout_sec=90,
        )

        # restart redpanda without an active license
        environment = dict(__REDPANDA_DISABLE_BUILTIN_TRIAL_LICENSE="1")
        self.redpanda.set_environment(environment)
        self.redpanda.restart_nodes(self.redpanda.nodes)

        # validate cluster and topic state
        cluster_config_get = cast(Any, rpk).cluster_config_get
        cluster_config = cluster_config_get("default_leaders_preference")
        assert cluster_config == f"{config.preference_str}:A", (
            f"Failed to properly load cluster's config on restart. Got '{cluster_config}')."
        )
        topic_config = get_leaders_preference("bar")
        assert topic_config == f"{config.preference_str}:C", (
            f"Failed to load topic's preferences on restart. Got '{topic_config}'."
        )

        # existing leadership preferences should be ignored as there is no active license
        time.sleep(90)
        t2r = self._get_topic2racks()
        expected = {"foo": {"A", "B", "C"}, "bar": {"A", "B", "C"}}
        assert t2r == expected, (
            f"Expected topic-to-rack leaders {expected}. Got {t2r} instead"
        )
