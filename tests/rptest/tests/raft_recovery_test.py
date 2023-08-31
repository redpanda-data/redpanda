# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.util import wait_until, wait_until_result
from rptest.utils.mode_checks import skip_debug_mode
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from ducktape.cluster.cluster import ClusterNode
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
