# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import json
from collections import defaultdict

from rptest.util import wait_until
from rptest.utils.mode_checks import skip_debug_mode
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.cluster.cluster import ClusterNode
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.kgo_repeater_service import repeater_traffic, KgoRepeaterService


class RaftRecoveryTest(RedpandaTest):

    topics = [TopicSpec(
        partition_count=128,
        replication_factor=3,
    )]

    msg_size = 4096

    def setUp(self):
        pass

    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)

    @skip_debug_mode
    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_recovery_concurrency_limit(self):
        """
        Verify that `raft_concurrent_recoveries` is respected when
        the number of partitions to recover exceeds it.
        """

        # Artificially low limits to slow down recovery enough that we
        # can watch the partitions trickle through
        shard_concurrency = 4
        self.redpanda.set_extra_rp_conf({
            'raft_recovery_concurrent_per_shard':
            shard_concurrency,
            'raft_max_recovery_memory':
            32 * 1024 * 1024,
        })

        cpu_count = self.redpanda.get_node_cpu_count()
        expect_concurrency = cpu_count * shard_concurrency

        self.redpanda.start()

        topic = "recoverytest"
        partition_count = 128 * cpu_count
        self.client().create_topic(
            TopicSpec(name=topic,
                      partition_count=partition_count,
                      retention_bytes=16 * 1024 * 1024,
                      segment_bytes=1024 * 1024))

        # Arbitrary choice of node to be restarted
        victim_node = self.redpanda.nodes[1]

        with repeater_traffic(context=self.test_context,
                              redpanda=self.redpanda,
                              topic=topic,
                              msg_size=self.msg_size,
                              workers=1,
                              max_buffered_records=1024) as repeater:
            self._restart_and_verify(victim_node=victim_node,
                                     repeater=repeater,
                                     node_concurrency=expect_concurrency)

        self._validate_recovery_log(victim_node, expect_concurrency)

    def _validate_recovery_log(self, victim_node: ClusterNode,
                               node_concurrency: int):
        # The admin API does not give you a partition-by-partition readout
        # because it would be prohibitively long.  We scrape the log to
        # validate the order in which recoveries happened.
        # Format from recovery_coordinator:
        #  "Starting recovery of partition {} ({}) from {} to {}"

        # Map of shard to list of lists, where each list is the recovery
        # start events within a lifetime of redpanda (i.e. we split when
        # we see redpanda restart)
        partition_recovery_order = defaultdict(list)

        for line in self.redpanda.filter_log(
                patterns=[
                    # Recovery start event
                    "Starting recovery of partition",
                    # Redpanda startup event
                    "Welcome to the Redpanda community"
                ],
                node=victim_node):
            if line.startswith("Welcome"):
                cpu_count = self.redpanda.get_node_cpu_count()
                for i in range(0, cpu_count):
                    partition_recovery_order[i].append([])
                continue

            try:
                shard, n, t, p = re.search(
                    "\[shard (\d+)\].*Starting recovery of partition {(.+)/(.+)/(\d+)}",
                    line).groups()
            except AttributeError:
                # If you get here, we probably changed logging and need to update
                # the test
                raise RuntimeError(f"Can't parse line '{line}'")

            partition_recovery_order[int(shard)][-1].append((n, t, p))

        for shard, orders in partition_recovery_order.items():
            # Only care about last redpanda lifetime (not the recovery events
            # that happened in its first startup when partitions were getting
            # created)
            order = orders[-1]

            expect_order = sorted(
                order,
                key=lambda ntp: 0
                if ntp[1] in ("__consumer_offsets", "id_allocator") else 1)
            self.logger.info(
                f"Recovery order[{shard}]: {json.dumps(order, indent=2)}")
            self.logger.info(
                f"Expect recovery order[{shard}]: {json.dumps(expect_order, indent=2)}"
            )
            assert order == expect_order

    def _restart_and_verify(self, victim_node: ClusterNode,
                            repeater: KgoRepeaterService,
                            node_concurrency: int):

        # Check traffic is flowing before stopping a node
        repeater.await_group_ready()
        repeater.await_progress(10, 30)

        self.redpanda.stop_node(victim_node)

        # FIXME: measure or flex with environment.
        expect_bandwidth = 10E6
        outage_time = 20
        recovery_data_target = outage_time * expect_bandwidth

        recovery_msgs_target = recovery_data_target // self.msg_size
        expect_progress_time = outage_time * 2

        # Wait for enough traffic to play that recovery will take long enough for us to
        # observe its progress.
        repeater.await_group_ready()

        # FIXME  - this is timing out sometimes.  Is it because we're falsely
        # seeing the group ready?
        repeater.await_progress(recovery_msgs_target, expect_progress_time)

        self.redpanda.start_node(victim_node)

        admin = Admin(self.redpanda)

        # TODO: enable leader balancer with a high enough frequency that it will
        # try to run during recovery, and validate that it does not move leaderships
        # to the recovering node until recovery is done.

        # We will wait for various conditions, but continously want to validate
        # general invariants such as that the concurrent recovery count
        # is not violated.
        def wait_with_invariants(check_fn, *, timeout_sec, backoff_sec):
            def wrapped():
                state = admin.get_recovery_status(node=victim_node)

                assert state['partitions_active'] <= node_concurrency
                assert state['offsets_hwm'] >= state['offsets_pending']
                assert state['offsets_pending'] >= 0

                return check_fn(state)

            return wait_until(wrapped,
                              timeout_sec=timeout_sec,
                              backoff_sec=backoff_sec)

        # The recovering node's offset highwatermark should rise to the amount
        # of data it has to recover
        wait_with_invariants(lambda s: s['offsets_hwm'] > 0,
                             timeout_sec=15,
                             backoff_sec=1)
        wait_with_invariants(lambda s: s['partitions_pending'] > 0,
                             timeout_sec=15,
                             backoff_sec=1)
        wait_with_invariants(lambda s: s['partitions_active'] > 0,
                             timeout_sec=15,
                             backoff_sec=1)

        wait_with_invariants(lambda s: s['partitions_pending'] == 0,
                             timeout_sec=120,
                             backoff_sec=1)
        wait_with_invariants(lambda s: s['partitions_active'] == 0,
                             timeout_sec=120,
                             backoff_sec=1)
