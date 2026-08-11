# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import abc
import random
import re
from contextlib import contextmanager
from typing import Dict, Iterable, Sequence, Tuple

from ducktape.cluster.cluster import ClusterNode
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.tests.redpanda_test import RedpandaTest

from rptest.services.tc_netem import NetemDelay, tc_netem_add, tc_netem_delete
from rptest.utils.mode_checks import skip_debug_mode


class Risks(dict):
    """a Dict[str, frozenset[str]] that makes sure keys are exactly these:"""

    KEYS = frozenset(
        (
            "rf1_offline",
            "full_acks_produce_unavailable",
            "unavailable",
            "acks1_data_loss",
        )
    )
    VALUE_RE = "^kafka/"  # ignore system topics

    @classmethod
    def build_value(cls, input: Iterable[str]):
        return frozenset(v for v in input if re.match(cls.VALUE_RE, v))

    def __init__(self, **kvargs):
        keys = kvargs.keys()
        assert keys == self.KEYS, f"{keys=}, {self.KEYS=}"
        dict.__init__(self, **{k: self.build_value(v) for k, v in kvargs.items()})


NO_RISKS = Risks(**{typ: set() for typ in Risks.KEYS})


class NodeRestartProbeTestBase(RedpandaTest):
    MSG_SIZE = 1024

    def __init__(self, *args, **kwargs):
        super(NodeRestartProbeTestBase, self).__init__(*args, **kwargs)
        self.admin = Admin(self.redpanda)
        self.kafka_tools = KafkaCliTools(self.redpanda)

    @contextmanager
    def with_append_entries_error_injection(
        self: RedpandaTest, node: ClusterNode, partitions: Sequence[Tuple[str, int]]
    ):
        node_id = self.redpanda.node_id(node)

        def toggle(inject: bool):
            for topic, partition in partitions:
                self.redpanda.logger.info(
                    "toggle append_entries failure injection "
                    f"{topic=} {partition=} {node_id=} {inject=}"
                )
                self.admin.toggle_failure_injection(
                    topic, partition, "append_entries", inject=inject, node=node
                )

        toggle(True)
        try:
            yield
        finally:
            toggle(False)

    def produce_to_all_partitions(self, acks):
        """
        produce data unevenly, so that partitions of different topics catch up
        at different times
        """
        for topic_no, topic in enumerate(self.topics):
            self.redpanda.logger.debug(f"producing to {topic.name}")
            # produce into topics with shorter data first to increase
            # discrepancy between partition catch-up times
            produce_bytes = self.PRODUCE_BYTES + self.PRODUCE_BYTES_JITTER * (
                topic_no * 3 / len(self.topics) - 1
            )
            produce_messages = max(produce_bytes / self.MSG_SIZE, 1)
            num_messages = int(topic.partition_count * produce_messages)
            self.kafka_tools.produce(topic.name, num_messages, self.MSG_SIZE, acks)
            self.redpanda.logger.debug(f"produced to {topic.name}")

    @abc.abstractmethod
    def create_topics(self):
        """should populate self.topics too"""


class NodePreRestartProbeTest(NodeRestartProbeTestBase):
    PRODUCE_BYTES = 300 * 1024 * 1024
    PRODUCE_BYTES_JITTER = 0

    def __init__(self, test_context):
        super(NodePreRestartProbeTest, self).__init__(
            test_context=test_context,
            num_brokers=5,
            extra_rp_conf={
                "health_monitor_max_metadata_age": 100,  # ms
                "enable_leader_balancer": False,
            },
        )

    def create_topics(self):
        self.topics = [
            TopicSpec(name="t1", partition_count=1, replication_factor=1),
            TopicSpec(name="t3", partition_count=2, replication_factor=3),
            TopicSpec(name="t5", partition_count=1, replication_factor=5),
        ]
        self.client().create_topic_with_assignment(self.topics[0].name, [[1]])
        self.client().create_topic_with_assignment(
            self.topics[1].name, [[1, 2, 3], [3, 4, 5]]
        )
        self.client().create_topic(self.topics[2])

    def get_node_risks(self, node, limit=None) -> Risks:
        reply = self.admin.get_broker_pre_restart_probe(node=node, limit=limit)
        self.redpanda.logger.debug(f"get_risks returned: {reply}")
        return Risks(**reply["risks"])

    def get_risks(self) -> Dict[int, Risks]:
        return {
            self.redpanda.node_id(node): self.get_node_risks(node)
            for node in self.redpanda.started_nodes()
        }

    def wait_pre_restart_probes(self, expected_risks: Dict[int, Risks], timeout_sec=30):
        """wait until it returns expected result, make sure it
        does not return anything milder in the meanwhile"""

        def risks_are_as_expected():
            actual_risks = self.get_risks()
            self.redpanda.logger.debug(
                f"actual_risks={sorted(actual_risks.items())}, "
                f"expected_risks={sorted(expected_risks.items())}"
            )
            return actual_risks == expected_risks

        wait_until(
            risks_are_as_expected,
            timeout_sec=timeout_sec,
            backoff_sec=0.1,
            err_msg="Waiting for reported risks to match expected",
        )

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def pre_restart_probe_test(self):
        nodes = {self.redpanda.node_id(node): node for node in self.redpanda.nodes}

        self.create_topics()
        t1 = self.topics[0].name
        t3 = self.topics[1].name
        t5 = self.topics[2].name
        t1p = f"kafka/{t1}/0"
        t3p0 = f"kafka/{t3}/0"
        t3p1 = f"kafka/{t3}/1"
        t5p = f"kafka/{t5}/0"

        # all nodes up
        inevitable_risks = {
            1: Risks(
                rf1_offline=[t1p],
                full_acks_produce_unavailable=[],
                unavailable=[],
                acks1_data_loss=[],
            ),
            2: NO_RISKS,
            3: NO_RISKS,
            4: NO_RISKS,
            5: NO_RISKS,
        }
        self.wait_pre_restart_probes(inevitable_risks)
        # limit 0 cuts off
        assert self.get_node_risks(nodes[1], limit=0) == NO_RISKS

        self.redpanda.stop_node(nodes[3])

        # node 3 down
        self.wait_pre_restart_probes(
            {
                1: Risks(
                    rf1_offline=[t1p],
                    full_acks_produce_unavailable=[],
                    unavailable=[t3p0],
                    acks1_data_loss=[],
                ),
                2: Risks(
                    rf1_offline=[],
                    full_acks_produce_unavailable=[],
                    unavailable=[t3p0],
                    acks1_data_loss=[],
                ),
                4: Risks(
                    rf1_offline=[],
                    full_acks_produce_unavailable=[],
                    unavailable=[t3p1],
                    acks1_data_loss=[],
                ),
                5: Risks(
                    rf1_offline=[],
                    full_acks_produce_unavailable=[],
                    unavailable=[t3p1],
                    acks1_data_loss=[],
                ),
            }
        )

        self.redpanda.start_node(nodes[3])
        # move t3_0, t3_1 and t5 leaders off node 3 which we will make lagged
        assert self.admin.transfer_leadership_to(
            namespace="kafka", topic=t3, partition=0, target_id=2
        )
        assert self.admin.transfer_leadership_to(
            namespace="kafka", topic=t3, partition=1, target_id=4
        )
        assert self.admin.transfer_leadership_to(
            namespace="kafka", topic=t5, partition=0, target_id=2
        )
        self.redpanda.stop_node(nodes[5])

        # lag node 3
        with self.with_append_entries_error_injection(
            nodes[3], [(t3, 0), (t3, 1), (t5, 0)]
        ):
            self.produce_to_all_partitions(acks=1)

        # node 3 lags, node 5 down
        self.wait_pre_restart_probes(
            {
                1: Risks(
                    rf1_offline=[t1p],
                    full_acks_produce_unavailable=[t3p0, t5p],
                    unavailable=[],
                    acks1_data_loss=[],
                ),
                2: Risks(
                    rf1_offline=[],
                    full_acks_produce_unavailable=[t3p0, t5p],
                    unavailable=[],
                    acks1_data_loss=[],
                ),
                3: Risks(
                    rf1_offline=[],
                    full_acks_produce_unavailable=[],
                    unavailable=[t3p1],
                    acks1_data_loss=[],
                ),
                4: Risks(
                    rf1_offline=[],
                    full_acks_produce_unavailable=[t5p],
                    unavailable=[t3p1],
                    acks1_data_loss=[t3p1],
                ),
            }
        )
        # good time to see how limits work
        assert (
            len(self.get_node_risks(nodes[1], limit=0)["full_acks_produce_unavailable"])
            == 0
        )
        assert (
            len(self.get_node_risks(nodes[1], limit=1)["full_acks_produce_unavailable"])
            == 1
        )
        assert (
            len(self.get_node_risks(nodes[1], limit=2)["full_acks_produce_unavailable"])
            == 2
        )
        assert (
            len(self.get_node_risks(nodes[1], limit=3)["full_acks_produce_unavailable"])
            == 2
        )

        # move t3_1 and t5 leaders off nodes 3 and 5 which we will make lagged
        assert self.admin.transfer_leadership_to(
            namespace="kafka", topic=t3, partition=1, target_id=4
        )
        assert self.admin.transfer_leadership_to(
            namespace="kafka", topic=t5, partition=0, target_id=2
        )
        # lag nodes 3 and 5
        self.redpanda.start_node(nodes[5])
        with (
            self.with_append_entries_error_injection(
                nodes[3], [(t3, 0), (t3, 1), (t5, 0)]
            ),
            self.with_append_entries_error_injection(nodes[5], [(t3, 1), (t5, 0)]),
        ):
            self.produce_to_all_partitions(acks=1)

        # all nodes up, but 3 and 5 lag
        self.wait_pre_restart_probes(
            {
                1: Risks(
                    rf1_offline=[t1p],
                    full_acks_produce_unavailable=[t3p0, t5p],
                    unavailable=[],
                    acks1_data_loss=[],
                ),
                2: Risks(
                    rf1_offline=[],
                    full_acks_produce_unavailable=[t3p0, t5p],
                    unavailable=[],
                    acks1_data_loss=[],
                ),
                3: Risks(
                    rf1_offline=[],
                    full_acks_produce_unavailable=[t3p1],
                    unavailable=[],
                    acks1_data_loss=[],
                ),
                4: Risks(
                    rf1_offline=[],
                    full_acks_produce_unavailable=[t5p],
                    unavailable=[],
                    acks1_data_loss=[t3p1],
                ),
                5: Risks(
                    rf1_offline=[],
                    full_acks_produce_unavailable=[t3p1],
                    unavailable=[],
                    acks1_data_loss=[],
                ),
            }
        )

        # when lag clears
        self.wait_pre_restart_probes(inevitable_risks, timeout_sec=240)


# Waits for value_fn() to eventually settle into a monotonically increasing
# sequence from `from_at_most` (or lower) up to `to`, with at least
# `min_values` distinct values in that final run.  Earlier dips are tolerated:
# any decrease resets the tracked run.
def wait_eventually_gradually_increases(
    value_fn, from_at_most, to, min_values, **kwargs
):
    last_dip = None
    prev = None
    distinct_values_since_last_dip = 0

    def completed():
        nonlocal last_dip, prev, distinct_values_since_last_dip
        cur_value = value_fn()
        if prev is None or cur_value < prev:
            last_dip = cur_value
            distinct_values_since_last_dip = 1
        elif cur_value > prev:
            distinct_values_since_last_dip += 1
        prev = cur_value
        return (
            cur_value == to
            and last_dip <= from_at_most
            and distinct_values_since_last_dip >= min_values
        )

    wait_until(completed, **kwargs)


def unittest_wait_eventually_gradually_increases():
    def make_val_fn(*values):
        it = iter(values)
        return lambda: next(it)

    default_params = dict(
        from_at_most=50, to=100, min_values=5, timeout_sec=1, backoff_sec=0
    )

    def call(*values):
        wait_eventually_gradually_increases(make_val_fn(*values), **default_params)

    def expect_pass(*values):
        call(*values)

    def expect_fail(*values):
        try:
            call(*values)
        except (StopIteration, AssertionError):
            pass
        else:
            assert False, "should have failed"

    # dip to 40 (<= 50), then monotonic rise with 5 distinct values
    expect_pass(80, 70, 40, 55, 70, 85, 100)
    # dip to 30, rise, another dip to 45, then final monotonic rise
    expect_pass(60, 30, 50, 45, 55, 70, 85, 95, 100)
    # dip to 50 (== from_at_most), valid
    expect_pass(80, 50, 60, 70, 80, 100)
    # never dips to from_at_most
    expect_fail(60, 55, 70, 85, 95, 100)
    # dips to from_at_most but the last dip is above from_at_most
    expect_fail(60, 45, 70, 55, 95, 100)
    # too few distinct values in final run
    expect_fail(80, 40, 90, 100)
    # does not reach `to`
    expect_fail(80, 40, 55, 70, 85, 99)


class NodePostRestartProbeTest(NodeRestartProbeTestBase):
    PRODUCE_BYTES = 5 * 1024 * 1024
    PRODUCE_BYTES_JITTER = 3 * 1024 * 1024

    def __init__(self, test_context):
        super(NodePostRestartProbeTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf={
                "health_monitor_max_metadata_age": 500,  # ms
                # Leader balancer transfers can cause partitions to be
                # momentarily "unclaimed" in the health walk (node stepped
                # down but new leader not yet reported), making
                # load_reclaimed_pc drop non-monotonically.
                "enable_leader_balancer": False,
                # High concurrency with the minimum memory budget makes
                # each recovery_stm read tiny chunks
                # (32 MiB / 16384 ≈ 2 KiB per round), slowing individual
                # recovery throughput while keeping all lagging partitions
                # in is_recovering state simultaneously.
                "raft_recovery_concurrency_per_shard": 16384,
                "raft_max_recovery_memory": 33554432,  # 32 MiB (minimum)
            },
        )

    @contextmanager
    def with_netem_delay(self):
        """Add 10ms delay on all nodes (bidirectional) to slow down
        recovery round-trips. With ~2 KiB per round and ~1000-4000
        rounds per partition, 10ms × rounds ≈ 20-80s of recovery."""
        delay = NetemDelay(delay_us=10000, jitter_us=4000)
        try:
            for node in self.redpanda.nodes:
                tc_netem_add(node, delay)
            yield
        finally:
            for node in self.redpanda.nodes:
                tc_netem_delete(node)

    def create_topics(self):
        self.topics = [
            TopicSpec(partition_count=10, replication_factor=3) for _ in range(10)
        ]
        self.client().create_topic(self.topics)

    def get_load_reclaimed_pc(self, node):
        load_reclaimed_pc = self.admin.get_broker_post_restart_probe(node)[
            "load_reclaimed_pc"
        ]
        assert 0 <= load_reclaimed_pc <= 100
        self.redpanda.logger.info(f"{load_reclaimed_pc=}")
        return load_reclaimed_pc

    @skip_debug_mode
    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def post_restart_probe_test(self):
        unittest_wait_eventually_gradually_increases()

        self.create_topics()

        lagging_node = random.choice(self.redpanda.nodes)

        self.produce_to_all_partitions(acks=1)

        wait_until(
            lambda: self.get_load_reclaimed_pc(lagging_node) == 100,
            timeout_sec=10,
            backoff_sec=1,
            err_msg="non-lagged replica load_reclaimed_pc won't reach 100%",
        )

        all_partitions = [
            (t.name, pid) for t in self.topics for pid in range(t.partition_count)
        ]

        # Netem wraps the entire recovery phase: it must be active
        # before error injection is lifted so recovery RPCs are slow
        # from the very first round.
        with self.with_netem_delay():
            with self.with_append_entries_error_injection(lagging_node, all_partitions):
                self.produce_to_all_partitions(acks=1)

            # After error injection is lifted, recovery_stm starts a continuous read loop for every
            # lagging partition. With a tiny per-recovery read budget (≈2 KiB) and and netem adding
            # 10 ms delay per direction on all nodes, each partition takes seconds to recover, so
            # the metric drops deep and then climbs gradually as partitions finish one by one.
            wait_until(
                lambda: self.get_load_reclaimed_pc(lagging_node) <= 60,
                timeout_sec=10,
                backoff_sec=0.1,
                err_msg="lagged replica load_reclaimed_pc won't go down",
            )

            # Score may fluctuate as it takes some time to gather up-to-date health info from all nodes
            wait_eventually_gradually_increases(
                lambda: self.get_load_reclaimed_pc(lagging_node),
                from_at_most=60,
                to=100,
                min_values=4,
                timeout_sec=60,
                backoff_sec=0.1,
                err_msg="lagged replica load_reclaimed_pc won't reach 100% gradually",
            )

        for n in self.redpanda.nodes:
            wait_until(
                lambda: self.get_load_reclaimed_pc(n) == 100,
                timeout_sec=10,
                backoff_sec=2,
                err_msg="non-lagged replica load_reclaimed_pc won't reach 100%",
            )
