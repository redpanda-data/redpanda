# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import collections
import random
import time
import math

from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk import RpkTool
from rptest.util import wait_until_result
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest


class LeadershipTransferTest(RedpandaTest):
    """
    Transfer leadership from one node to another.
    """
    topics = (TopicSpec(partition_count=3, replication_factor=3), )

    def __init__(self, *args, **kwargs):
        super(LeadershipTransferTest, self).__init__(
            *args,
            extra_rp_conf={
                # Disable leader balancer, as this test is doing its own
                # partition movement and the balancer would interfere
                'enable_leader_balancer': False
            },
            **kwargs)

    @cluster(num_nodes=3)
    def test_controller_recovery(self):
        kc = KafkaCat(self.redpanda)

        # choose a partition and a target node
        partition = self._get_partition(kc)
        target_node_id = next(
            filter(lambda r: r["id"] != partition["leader"],
                   partition["replicas"]))["id"]
        self.logger.debug(
            f"Transfering leader from {partition['leader']} to {target_node_id}"
        )

        # build the transfer url
        meta = kc.metadata()
        brokers = meta["brokers"]
        source_broker = next(
            filter(lambda b: b["id"] == partition["leader"], brokers))
        target_broker = next(
            filter(lambda b: b["id"] == target_node_id, brokers))
        self.logger.debug(f"Source broker {source_broker}")
        self.logger.debug(f"Target broker {target_broker}")

        # Send the request to any host, they should redirect to
        # the leader of the partition.
        partition_id = partition['partition']

        admin = Admin(self.redpanda)
        admin.partition_transfer_leadership("kafka", self.topic, partition_id,
                                            target_node_id)

        def transfer_complete():
            for _ in range(3):  # just give it a moment
                time.sleep(1)
                meta = kc.metadata()
                partition = next(
                    filter(lambda p: p["partition"] == partition_id,
                           meta["topics"][0]["partitions"]))
                if partition["leader"] == target_node_id:
                    return True
            return False

        wait_until(lambda: transfer_complete(),
                   timeout_sec=30,
                   backoff_sec=5,
                   err_msg="Transfer did not complete")

    def _get_partition(self, kc):
        def get_partition():
            meta = kc.metadata()
            topics = meta["topics"]
            assert len(topics) == 1
            assert topics[0]["topic"] == self.topic
            partition = random.choice(topics[0]["partitions"])
            return partition["leader"] > 0, partition

        return wait_until_result(get_partition,
                                 timeout_sec=30,
                                 backoff_sec=2,
                                 err_msg="No partition with leader available")

    @cluster(num_nodes=3)
    def test_self_transfer(self):
        admin = Admin(self.redpanda)
        for topic in self.topics:
            for partition in range(topic.partition_count):
                leader = admin.get_partitions(topic, partition)['leader_id']
                admin.partition_transfer_leadership("kafka", topic, partition,
                                                    leader)


class MultiTopicAutomaticLeadershipBalancingTest(RedpandaTest):
    def __init__(self, test_context):
        extra_rp_conf = dict(leader_balancer_idle_timeout=20000,
                             leader_balancer_mode="random_hill_climbing")

        super(MultiTopicAutomaticLeadershipBalancingTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)
        self.topics = [
            TopicSpec(partition_count=61, replication_factor=3),
            TopicSpec(partition_count=151, replication_factor=3),
        ]
        if not self.debug_mode:
            self.topics.append(
                TopicSpec(partition_count=263, replication_factor=3))

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_topic_aware_rebalance(self):
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
                    self.logger.debug(
                        f"partitions without leaders: {missing_leaders}")
                    return False

            return True

        def count_leaders_per_node(topic_name: str):
            leaders_per_node = collections.defaultdict(int)
            tps = self.redpanda.partitions(topic_name)
            for p in tps:
                if p.leader:
                    leaders_per_node[p.leader] += 1

            return leaders_per_node

        def distribution_error():
            nodes = [
                self.redpanda.node_id(n)
                for n in self.redpanda.started_nodes()
            ]
            error = 0.0
            for t in self.topics:
                leaders_per_node = count_leaders_per_node(topic_name=t.name)
                opt_leaders = t.partition_count / len(nodes)

                for n in nodes:
                    if n in leaders_per_node:
                        error += (opt_leaders - leaders_per_node[n])**2
                    else:
                        error += opt_leaders**2

            return error

        def has_leader_count(topic_name: str, min_per_node: int,
                             nodes: int) -> bool:
            leaders_per_node = count_leaders_per_node(topic_name)

            if len(set(leaders_per_node)) < nodes:
                return False

            self.logger.info(
                f"{topic_name} has dist {leaders_per_node.values()}")
            return all(leader_cnt >= min_per_node
                       for leader_cnt in leaders_per_node.values())

        def topic_leadership_evenly_distributed():
            for t in self.topics:
                expected_leaders_per_node = int(0.8 * (t.partition_count / 3))
                self.logger.info(
                    f"for topic {t} expecting {expected_leaders_per_node} leaders"
                )

                if not has_leader_count(t.name, expected_leaders_per_node, 3):
                    return False

            return True

        self.logger.info("initial stabilization")
        wait_until(lambda: all_partitions_present(3),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Leadership did not stablize")

        node = self.redpanda.nodes[0]
        self.redpanda.stop_node(node)
        self.logger.info("stabilization post stop")
        wait_until(lambda: all_partitions_present(2),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Leadership did not stablize")

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

        def wait_for_topics_evenly_distributed(improvement_deadline):
            last_update = time.time()
            last_error = distribution_error()
            while (time.time() - last_update < improvement_deadline):
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

        self.logger.info("stabilization post start")
        wait_for_topics_evenly_distributed(30)


class AutomaticLeadershipBalancingTest(RedpandaTest):
    # number cores = 3 (default)
    # number nodes = 3 (default)
    # parts per core = 7
    topics = (TopicSpec(partition_count=63, replication_factor=3), )

    def __init__(self, test_context):
        extra_rp_conf = dict(leader_balancer_idle_timeout=20000, )

        super(AutomaticLeadershipBalancingTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)

    def _get_leaders_by_node(self):
        kc = KafkaCat(self.redpanda)
        md = kc.metadata()
        topic = next(filter(lambda t: t["topic"] == self.topic, md["topics"]))
        leaders = (p["leader"] for p in topic["partitions"])
        return collections.Counter(leaders)

    def _get_leaders_by_shard(self):
        admin = Admin(self.redpanda)
        shard2count = dict()
        for n in self.redpanda.started_nodes():
            node_id = self.redpanda.node_id(n)
            partitions = admin.get_partitions(node=n)
            for p in partitions:
                if p.get('leader') == node_id:
                    shard = (node_id, p['core'])
                    shard2count[shard] = shard2count.setdefault(shard, 0) + 1
        return shard2count

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_automatic_rebalance(self):
        def all_partitions_present(num_nodes, per_node=None):
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
        wait_until(lambda: all_partitions_present(3),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Leadership did not stablize")

        # stop node and wait for all leaders to transfer
        # to another node
        node = self.redpanda.nodes[0]
        self.redpanda.stop_node(node)
        wait_until(lambda: all_partitions_present(2),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Leadership did not move to running nodes")

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
        wait_until(lambda: all_partitions_present(3, 15),
                   timeout_sec=300,
                   backoff_sec=10,
                   err_msg="Leadership did not stablize")

        shard2leaders = self._get_leaders_by_shard()
        self.redpanda.logger.debug(f"Leaders by shard: {shard2leaders}")
        expected_on_shard = sum(shard2leaders.values()) / len(shard2leaders)
        for s, count in shard2leaders.items():
            expected_min = math.floor(expected_on_shard * 0.8)
            assert count >= expected_min, \
                f"leader count on shard {s} ({count}) is < {expected_min}"


class LeadershipPinningTest(RedpandaTest):
    def __init__(self, test_context):
        super(LeadershipPinningTest, self).__init__(
            test_context=test_context,
            num_brokers=6,
            extra_rp_conf={
                'enable_rack_awareness': True,
                # In this test we kill nodes. Leader balancer might try
                # transferring leaders to those nodes before it realizes
                # that they are offline. To avoid muting corresponding groups
                # for long (which might prevent reaching balancing goals),
                # lower the mute timeout.
                'leader_balancer_mute_timeout': 20000,
            },
        )

    def setUp(self):
        pass

    RACK_LAYOUT = ['A', 'A', 'B', 'B', 'C', 'C']

    def _get_topic2node2leaders(self):
        kc = KafkaCat(self.redpanda)
        md = kc.metadata()
        ret = dict()
        for topic in md["topics"]:
            name = topic["topic"]
            node2leaders = dict(
                collections.Counter(p["leader"] for p in topic["partitions"]))
            self.logger.debug(
                f"topic {name} leaders: {sorted(node2leaders.items())}")

            ret[name] = node2leaders
        return ret

    def _rack_counts(self, node_counts):
        rack2count = dict()
        for ix, node in enumerate(self.redpanda.nodes):
            node_id = self.redpanda.node_id(node)
            leaders = node_counts.get(node_id, 0)
            if leaders > 0:
                rack = self.RACK_LAYOUT[ix]
                rack2count[rack] = rack2count.setdefault(rack, 0) + leaders
        return rack2count

    def wait_for_racks(self,
                       partition_counts,
                       topic2expected_racks,
                       check_balance=True,
                       timeout_sec=60):
        def predicate():
            t2n2l = self._get_topic2node2leaders()

            for topic, expected_count in partition_counts.items():
                node2leaders = t2n2l.get(topic, dict())

                count = sum(node2leaders.values())
                if count != expected_count:
                    self.logger.debug(
                        f"not all leaders for topic {topic} present, "
                        f"expected {expected_count}, got {count}")
                    return False

                expected_racks = topic2expected_racks.get(topic, {})
                rack2leaders = self._rack_counts(node2leaders)

                if expected_racks != rack2leaders.keys():
                    self.logger.debug(
                        f"leader rack sets for topic {topic} differ, "
                        f"expected: {expected_racks}, actual counts: {rack2leaders}"
                    )
                    return False

                if check_balance:
                    nonzero_counts = [
                        l for l in node2leaders.values() if l > 0
                    ]
                    if min(nonzero_counts) + 2 < max(nonzero_counts):
                        self.logger.debug(
                            f"leader counts unbalanced for topic {topic}: "
                            f"{sorted(node2leaders.items())}")
                        return False

            return True

        wait_until(predicate, timeout_sec=timeout_sec, backoff_sec=5)

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_leadership_pinning(self):
        for ix, node in enumerate(self.redpanda.nodes):
            self.redpanda.set_extra_node_conf(node, {
                'rack': self.RACK_LAYOUT[ix],
            })
        self.redpanda.add_extra_rp_conf(
            {'default_leaders_preference': "racks: A"})
        self.redpanda.start()

        rpk = RpkTool(self.redpanda)

        partition_counts = {"foo": 60, "bar": 20}

        self.logger.info("creating topics")

        rpk.create_topic("foo", partitions=60, replicas=3)
        rpk.create_topic("bar",
                         partitions=20,
                         replicas=3,
                         config={"redpanda.leaders.preference": "racks: C"})

        # bigger timeout to allow balancer to activate, health reports to propagate, etc.
        self.wait_for_racks(partition_counts, {
            "foo": {"A"},
            "bar": {"C"}
        },
                            timeout_sec=90)

        self.logger.info("altering topic preference")

        rpk.alter_topic_config("bar", "redpanda.leaders.preference",
                               "racks: B, C")

        self.wait_for_racks(partition_counts, {
            "foo": {"A"},
            "bar": {"B", "C"}
        },
                            timeout_sec=30)

        # Decrease idle timeout to not wait too long after nodes are killed
        self.redpanda.set_cluster_config({"enable_leader_balancer": False})
        self.redpanda.set_cluster_config(
            {"leader_balancer_idle_timeout": 20000})
        self.redpanda.set_cluster_config({"enable_leader_balancer": True})

        self.logger.info("killing rack B")

        for ix, node in enumerate(self.redpanda.nodes):
            if self.RACK_LAYOUT[ix] == "B":
                self.redpanda.stop_node(node)

        self.wait_for_racks(partition_counts, {
            "foo": {"A"},
            "bar": {"C"}
        },
                            timeout_sec=60)

        self.logger.info("explicitly disabling for topic")
        rpk.alter_topic_config("foo", "redpanda.leaders.preference", "none")

        # There is cross-talk between partition counts of foo and bar, so we don't
        # require balanced counts.
        self.wait_for_racks(partition_counts, {
            "foo": {"A", "C"},
            "bar": {"C"}
        },
                            check_balance=False,
                            timeout_sec=30)

        self.logger.info("unset topic configs")

        rpk.delete_topic_config("foo", "redpanda.leaders.preference")
        rpk.delete_topic_config("bar", "redpanda.leaders.preference")

        self.wait_for_racks(partition_counts, {
            "foo": {"A"},
            "bar": {"A"}
        },
                            timeout_sec=30)

        self.logger.info("unset default preference")

        for ix, node in enumerate(self.redpanda.nodes):
            if self.RACK_LAYOUT[ix] == "B":
                self.redpanda.start_node(node)

        self.redpanda.set_cluster_config(
            {"default_leaders_preference": "none"})
        self.wait_for_racks(partition_counts, {
            "foo": {"A", "B", "C"},
            "bar": {"A", "B", "C"}
        },
                            check_balance=False,
                            timeout_sec=90)
