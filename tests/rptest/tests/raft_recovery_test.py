# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from rptest.util import wait_until, wait_until_result
from rptest.utils.mode_checks import skip_debug_mode
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda_installer import RedpandaInstaller


def _high_watermarks(rpk: RpkTool, topic: str):
    def func():
        hwms = list(p.high_watermark
                    for p in rpk.describe_topic(topic, tolerant=True))
        if any(o is None for o in hwms):
            return False

        rpk._redpanda.logger.debug(f"topic '{topic}' high watermarks: {hwms}")
        return True, hwms

    return wait_until_result(func,
                             backoff_sec=1,
                             timeout_sec=60,
                             err_msg="failed to wait for valid hwm offsets")


def _await_progress_in_all_partitions(rpk: RpkTool, topic: str):
    start = _high_watermarks(rpk, topic)

    def condition():
        current = _high_watermarks(rpk, topic)
        assert len(current) == len(start)
        return all(c > s for c, s in zip(current, start))

    wait_until(condition,
               backoff_sec=1,
               timeout_sec=60,
               err_msg="failed to wait for progress in all partitions")


class RaftRecoveryUpgradeTest(RedpandaTest):
    def setUp(self):
        pass

    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)

    @skip_debug_mode
    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_upgrade(self):
        """
        Verify that recovery works both ways during upgrade
        """

        installer = self.redpanda._installer
        prev_version = installer.highest_from_prior_feature_version(
            RedpandaInstaller.HEAD)
        self.logger.info(f"will test against prev version {prev_version}")

        installer.install(self.redpanda.nodes, prev_version)
        self.redpanda.start()

        topic = "mytopic"
        partition_count = 32
        self.client().create_topic(
            TopicSpec(name=topic,
                      partition_count=partition_count,
                      retention_bytes=8 * 1024 * 1024,
                      segment_bytes=1024 * 1024))

        rpk = RpkTool(self.redpanda)

        def wait_for_recovery_finished():
            _await_progress_in_all_partitions(rpk, topic)
            self._wait_for_underreplicated(self.redpanda.nodes, 0)
            self.logger.info(f"recovery finished")

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=1024,
            # some large number to get produce load till the end of test
            msg_count=2**30,
            rate_limit_bps=4 * 2**20)
        producer.start()

        _await_progress_in_all_partitions(rpk, topic)

        # upgrade node 3 but don't start it yet, wait for replication lag to accumulate
        node3 = self.redpanda.nodes[2]
        self.redpanda.stop_node(node3)
        installer.install([node3], RedpandaInstaller.HEAD)
        self.logger.info(f"stopped node {node3.name}")
        self._wait_for_underreplicated(self.redpanda.nodes[0:2],
                                       partition_count)

        # start node3 and check that it can recover from old nodes
        self.redpanda.start_node(node3)
        self.logger.info(f"started node {node3.name}")
        wait_for_recovery_finished()

        # upgrade and restart node 2 and wait for the cluster to stabilize
        node2 = self.redpanda.nodes[1]
        self.redpanda.stop_node(node2)
        installer.install([node2], RedpandaInstaller.HEAD)
        self.redpanda.start_node(node2)
        self.logger.info(f"upgraded node {node2.name}")
        wait_for_recovery_finished()

        # stop node 1 and wait for the replication lag to accumulate
        node1 = self.redpanda.nodes[0]
        self.redpanda.stop_node(node1)
        self.logger.info(f"stopped node {node1.name}")
        _await_progress_in_all_partitions(rpk, topic)
        self._wait_for_underreplicated(self.redpanda.nodes[1:3],
                                       partition_count)

        # start node 1 and check that it can recover from upgraded nodes
        self.redpanda.start_node(node1)
        self.logger.info(f"started node {node1.name}")
        wait_for_recovery_finished()

    def _wait_for_underreplicated(self, nodes: [ClusterNode], target: int):
        def ready():
            count = self.redpanda.metric_sum(
                'vectorized_cluster_partition_under_replicated_replicas',
                nodes=nodes)
            self.logger.info(f"under-replicated partitions count: {count}")
            if target == 0:
                return count == 0
            else:
                return count >= target

        wait_until(
            ready,
            timeout_sec=60,
            backoff_sec=2,
            err_msg=f"couldn't reach under-replicated count target: {target}")


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
    @matrix(stop_producer=[False, True])
    def test_recovery_concurrency_limit(self, stop_producer):
        """
        Verify that `raft_concurrent_recoveries` is respected when
        the number of partitions to recover exceeds it.
        """

        # Artificially low limits to slow down recovery enough that we
        # can watch the partitions trickle through
        shard_concurrency = 4
        self.redpanda.set_extra_rp_conf({
            'raft_recovery_concurrency_per_shard':
            shard_concurrency,
            'raft_max_recovery_memory':
            32 * 1024 * 1024,
            # FIXME: leadership transfers force recoveries that can then
            # violate the concurrency limit
            'enable_leader_balancer':
            False,
        })

        cpu_count = self.redpanda.get_node_cpu_count()
        node_concurrency = cpu_count * shard_concurrency

        self.redpanda.start()

        topic = "mytopic"
        partition_count = 64 * cpu_count

        # TODO: different values for CDT
        segment_bytes = 2**20
        produce_bandwidth = 5 * 2**20
        recovery_data_target = 60 * produce_bandwidth

        self.client().create_topic(
            TopicSpec(name=topic,
                      partition_count=partition_count,
                      segment_bytes=segment_bytes,
                      retention_bytes=16 * segment_bytes))

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=self.msg_size,
            # some large number to get produce load till the end of test
            msg_count=2**30,
            rate_limit_bps=produce_bandwidth)
        producer.start()

        # Arbitrary choice of node to be restarted
        victim_node = self.redpanda.nodes[1]

        rpk = RpkTool(self.redpanda)

        _await_progress_in_all_partitions(rpk, topic)
        self.redpanda.stop_node(victim_node)

        # Wait for recovery lag to accumulate.
        recovery_msgs_target = recovery_data_target // self.msg_size
        expect_progress_time = recovery_data_target / produce_bandwidth

        start_count = producer.produce_status.acked
        target_count = start_count + recovery_msgs_target

        self.logger.info(
            f"wait for producer to reach {target_count} acked msgs in {expect_progress_time} s."
        )

        time.sleep(expect_progress_time)
        # give producer 2x expected time to produce the required amount of data
        wait_until(lambda: producer.produce_status.acked >= target_count,
                   backoff_sec=1,
                   timeout_sec=expect_progress_time)

        if stop_producer:
            # Stop producer before starting the victim node so that
            # its raft instances will have to rely on heartbeats to
            # detect when to exit recovery.
            producer.stop_node(producer.nodes[0])

        self.redpanda.start_node(victim_node)

        admin = Admin(self.redpanda)

        # We will wait for various conditions, but continously want to validate
        # general invariants such as that the concurrent recovery count
        # is not violated.
        def wait_with_invariants(check_fn, *, timeout_sec, backoff_sec):
            def wrapped():
                state = admin.get_raft_recovery_status(node=victim_node)
                assert state['partitions_active'] <= node_concurrency
                return check_fn(state)

            return wait_until(wrapped,
                              timeout_sec=timeout_sec,
                              backoff_sec=backoff_sec)

        # wait for recovery to start
        wait_with_invariants(lambda s: s['partitions_active'] > 0,
                             timeout_sec=15,
                             backoff_sec=1)

        def recovery_finished(state):
            if state['partitions_to_recover'] > 0 or state[
                    'offsets_pending'] > 0:
                return False

            # if recovery status says that we are done, additionally check metrics

            count = self.redpanda.metric_sum(
                'vectorized_cluster_partition_under_replicated_replicas')
            self.logger.info(f"under-replicated partitions count: {count}")

            return count == 0

        wait_with_invariants(recovery_finished, timeout_sec=120, backoff_sec=1)
