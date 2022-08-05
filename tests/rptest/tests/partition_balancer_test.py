# Copyright 2022 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import os
import random
import time

from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.util import wait_until, wait_until_result
from rptest.clients.default import DefaultClient
from rptest.services.redpanda import RedpandaService, CHAOS_LOG_ALLOW_LIST
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer
from rptest.services.franz_go_verifiable_services import (
    FranzGoVerifiableProducer,
    await_minimum_produced_records,
)
from rptest.tests.end_to_end import EndToEndTest
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool, RpkException
from ducktape.cluster.cluster_spec import ClusterSpec

# We inject failures which might cause consumer groups
# to re-negotiate, so it is necessary to have a longer
# consumer timeout than the default 30 seconds
CONSUMER_TIMEOUT = 90


class PartitionBalancerTest(EndToEndTest):
    NODE_AVAILABILITY_TIMEOUT = 10

    def __init__(self, ctx, *args, **kwargs):
        super(PartitionBalancerTest, self).__init__(
            ctx,
            *args,
            extra_rp_conf={
                "partition_autobalancing_mode": "continuous",
                "partition_autobalancing_node_availability_timeout_sec":
                self.NODE_AVAILABILITY_TIMEOUT,
                "partition_autobalancing_tick_interval_ms": 5000,
                "raft_learner_recovery_rate": 1_000_000,
            },
            **kwargs,
        )

    def topic_partitions_replicas(self, topic):

        rpk = RpkTool(self.redpanda)
        num_partitions = topic.partition_count

        def all_partitions_ready():
            try:
                partitions = list(rpk.describe_topic(topic.name))
            except RpkException:
                return False
            return (len(partitions) == num_partitions, partitions)

        return wait_until_result(
            all_partitions_ready,
            timeout_sec=120,
            backoff_sec=1,
            err_msg="failed to wait until all partitions have leaders",
        )

    def node2partition_count(self):
        topics = [self.topic]
        ret = {}
        for topic in topics:
            partitions = self.topic_partitions_replicas(topic)
            for p in partitions:
                for r in p.replicas:
                    ret[r] = ret.setdefault(r, 0) + 1

        return ret

    def wait_until_status(self, predicate, timeout_sec=120):
        admin = Admin(self.redpanda)
        start = time.time()

        def check():
            req_start = time.time()

            status = admin.get_partition_balancer_status(timeout=1)
            self.logger.info(f"partition balancer status: {status}")

            if "seconds_since_last_tick" not in status:
                return False
            return (
                req_start - status["seconds_since_last_tick"] - 1 > start
                and predicate(status),
                status,
            )

        return wait_until_result(
            check,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg="failed to wait until status condition",
        )

    def wait_until_ready(self, timeout_sec=120):
        return self.wait_until_status(
            lambda status: status["status"] == "ready",
            timeout_sec=timeout_sec)

    def check_no_replicas_on_node(self, node):
        node2pc = self.node2partition_count()
        self.logger.info(f"partition counts: {node2pc}")
        total_replicas = self.topic.partition_count * self.topic.replication_factor
        assert sum(node2pc.values()) == total_replicas
        assert self.redpanda.idx(node) not in node2pc

    class NodeStopper:
        """Stop/kill/freeze one node at a time and wait for partition balancer."""
        def __init__(self, test):
            self.test = test
            self.f_injector = FailureInjector(test.redpanda)
            self.cur_failure = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.make_available()

        def wait_for_node_to_be_ready(self, node_id):
            admin = Admin(self.test.redpanda)
            brokers = admin.get_brokers()

            for b in brokers:
                if b['node_id'] == node_id and b['is_alive'] == False:
                    return False

            return True

        def make_available(self, wait_for_node=False):
            if self.cur_failure:
                # heal the previous failure
                self.f_injector._stop_func(self.cur_failure.type)(
                    self.cur_failure.node)
                if wait_for_node:
                    wait_until(
                        lambda: self.wait_for_node_to_be_ready(
                            self.test.redpanda.idx(self.cur_failure.node)), 30,
                        1)
                self.cur_failure = None

        def make_unavailable(self,
                             node,
                             failure_types=None,
                             wait_for_previous_node=False):
            self.make_available(wait_for_previous_node)

            if failure_types == None:
                failure_types = [
                    FailureSpec.FAILURE_KILL,
                    FailureSpec.FAILURE_TERMINATE,
                    FailureSpec.FAILURE_SUSPEND,
                ]

            # Only track one failure at a time
            assert self.cur_failure is None

            self.cur_failure = FailureSpec(random.choice(failure_types), node)
            self.f_injector._start_func(self.cur_failure.type)(
                self.cur_failure.node)
            time.sleep(self.test.NODE_AVAILABILITY_TIMEOUT)

        def step_and_wait_for_in_progress(self, node):
            self.make_unavailable(node)
            self.test.logger.info(f"waiting for partition balancer to kick in")
            self.test.wait_until_status(lambda s: s["status"] == "in_progress"
                                        or s["status"] == "ready")

        def step_and_wait_for_ready(self, node, wait_for_node_status=False):
            self.make_unavailable(node,
                                  wait_for_previous_node=wait_for_node_status)
            self.test.logger.info(f"waiting for quiescent state")
            self.test.wait_until_ready()

    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_unavailable_nodes(self):
        self.start_redpanda(num_nodes=5)

        self.topic = TopicSpec(partition_count=random.randint(20, 30))
        self.client().create_topic(self.topic)

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        with self.NodeStopper(self) as ns:
            for n in range(7):
                node = self.redpanda.nodes[n % len(self.redpanda.nodes)]
                if n in {1, 3, 4}:
                    ns.step_and_wait_for_in_progress(node)
                else:
                    ns.step_and_wait_for_ready(node)
                    self.check_no_replicas_on_node(node)

            # Restore the system to a fully healthy state before validation:
            # not strictly necessary but simplifies debugging.
            ns.make_available()
            self.run_validation(consumer_timeout_sec=CONSUMER_TIMEOUT)

    def _throttle_recovery(self, new_value):
        self.redpanda.set_cluster_config(
            {"raft_learner_recovery_rate": str(new_value)})

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_movement_cancellations(self):
        self.start_redpanda(num_nodes=4)

        self.topic = TopicSpec(partition_count=random.randint(20, 30))

        self.client().create_topic(self.topic)

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        empty_node = self.redpanda.nodes[-1]

        with self.NodeStopper(self) as ns:
            # clean node
            ns.make_unavailable(empty_node,
                                failure_types=[FailureSpec.FAILURE_KILL])
            time.sleep(self.NODE_AVAILABILITY_TIMEOUT)
            self.wait_until_ready()
            ns.make_available()
            self.check_no_replicas_on_node(empty_node)

            # throttle recovery to prevent partition move from finishing
            self._throttle_recovery(10)

            for n in range(3):
                node = self.redpanda.nodes[n % 3]
                ns.make_unavailable(node,
                                    failure_types=[FailureSpec.FAILURE_KILL])
                time.sleep(self.NODE_AVAILABILITY_TIMEOUT)

                # wait until movement start
                self.wait_until_status(lambda s: s["status"] == "in_progress")
                ns.make_available()

                with self.NodeStopper(self) as ns2:
                    # stop empty node
                    ns.make_unavailable(
                        empty_node, failure_types=[FailureSpec.FAILURE_KILL])
                    time.sleep(self.NODE_AVAILABILITY_TIMEOUT)

                    # wait until movements are cancelled
                    self.wait_until_ready()

                    self.check_no_replicas_on_node(empty_node)

            self.run_validation(consumer_timeout_sec=CONSUMER_TIMEOUT)

    @cluster(num_nodes=8, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_rack_awareness(self):
        extra_rp_conf = self._extra_rp_conf | {"enable_rack_awareness": True}
        self.redpanda = RedpandaService(self.test_context,
                                        num_brokers=6,
                                        extra_rp_conf=extra_rp_conf)

        rack_layout = "AABBCC"
        for ix, node in enumerate(self.redpanda.nodes):
            self.redpanda.set_extra_node_conf(node, {"rack": rack_layout[ix]})

        self.redpanda.start()
        self._client = DefaultClient(self.redpanda)

        self.topic = TopicSpec(partition_count=random.randint(20, 30))
        self.client().create_topic(self.topic)

        def check_rack_placement():
            for p in self.topic_partitions_replicas(self.topic):
                racks = {(r - 1) // 2 for r in p.replicas}
                assert (
                    len(racks) == 3
                ), f"bad rack placement {racks} for partition id: {p.id} (replicas: {p.replicas})"

        check_rack_placement()

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        with self.NodeStopper(self) as ns:
            for n in range(7):
                node = self.redpanda.nodes[n % len(self.redpanda.nodes)]
                ns.step_and_wait_for_ready(node, wait_for_node_status=True)
                self.check_no_replicas_on_node(node)
                check_rack_placement()

            ns.make_available()
            self.run_validation(consumer_timeout_sec=CONSUMER_TIMEOUT)

    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_fuzz_admin_ops(self):
        self.start_redpanda(num_nodes=5)

        self.topic = TopicSpec(partition_count=random.randint(20, 30))
        self.client().create_topic(self.topic)

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        admin_fuzz = AdminOperationsFuzzer(self.redpanda, min_replication=3)
        admin_fuzz.start()

        def describe_topics(retries=5, retries_interval=5):
            if retries == 0:
                return self.client().describe_topics()
            error = None
            for retry in range(retries):
                try:
                    return self.client().describe_topics()
                except Exception as e:
                    error = e
                    self.logger.info(
                        f"describe_topics error: {error}, "
                        f"retries left: {retries-retry}/{retries}")
                    time.sleep(retries_interval)
            raise error

        def get_node2partition_count():
            node2partition_count = {}
            for t in describe_topics():
                for p in t.partitions:
                    for r in p.replicas:
                        node2partition_count[
                            r] = node2partition_count.setdefault(r, 0) + 1
            self.logger.info(f"partition counts: {node2partition_count}")
            return node2partition_count

        with self.NodeStopper(self) as ns:
            for n in range(7):
                node = self.redpanda.nodes[n % len(self.redpanda.nodes)]
                ns.make_unavailable(node)
                try:
                    admin_fuzz.pause()
                    self.wait_until_ready()
                    node2partition_count = get_node2partition_count()
                    assert self.redpanda.idx(node) not in node2partition_count
                finally:
                    admin_fuzz.unpause()

            admin_fuzz.wait(count=10, timeout=240)
            admin_fuzz.stop()

            ns.make_available()
            self.run_validation(consumer_timeout_sec=CONSUMER_TIMEOUT)

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_full_nodes(self):
        """
        Test partition balancer full disk node handling with the following scenario:
        * produce data and fill the cluster up to ~75%
        * kill a node.
        * partition balancer will move partitions to remaining nodes,
          causing remaining nodes to go over 80%
        * start the killed node, check that partition balancer will balance
          partitions away from full nodes.
        """

        skip_reason = None
        if not self.test_context.globals.get('use_xfs_partitions', False):
            skip_reason = "looks like we are not using separate partitions for each node"
        elif os.environ.get('BUILD_TYPE', None) == 'debug':
            skip_reason = "debug builds are too slow"

        if skip_reason:
            self.logger.warn("skipping test: " + skip_reason)
            # avoid the "Test requested 6 nodes, used only 0" error
            self.redpanda = RedpandaService(self.test_context, 0)
            self.test_context.cluster.alloc(ClusterSpec.simple_linux(6))
            return

        disk_size = 2 * 1024 * 1024 * 1024
        self.start_redpanda(
            num_nodes=5,
            extra_rp_conf={
                "storage_min_free_bytes": 10 * 1024 * 1024,
                "raft_learner_recovery_rate": 100_000_000,
                "health_monitor_tick_interval": 3000
            },
            environment={"__REDPANDA_TEST_DISK_SIZE": disk_size})

        def get_disk_usage(node):
            for line in node.account.ssh_capture(
                    "df --block-size 1 /var/lib/redpanda"):
                self.logger.debug(line.strip())
                if '/var/lib/redpanda' in line:
                    return int(line.split()[2])
            assert False, "couldn't parse df output"

        def get_total_disk_usage():
            return sum(get_disk_usage(n) for n in self.redpanda.nodes)

        self.topic = TopicSpec(partition_count=32)
        self.client().create_topic(self.topic)

        # produce around 2GB of data, this should be enough to fill node disks
        # to a bit more than 70% usage on average
        producer = FranzGoVerifiableProducer(self.test_context,
                                             self.redpanda,
                                             self.topic,
                                             msg_size=102_400,
                                             msg_count=19_000)
        producer.start(clean=False)

        wait_until(lambda: get_total_disk_usage() / 5 / disk_size > 0.7,
                   timeout_sec=120,
                   backoff_sec=5)

        # a waiter that prints current node disk usage while it waits.
        def create_waiter(target_status):
            def func(s):
                for n in self.redpanda.nodes:
                    disk_usage = get_disk_usage(n)
                    self.logger.info(
                        f"node {self.redpanda.idx(n)}: "
                        f"disk used percentage: {int(100.0 * disk_usage/disk_size)}"
                    )
                return s["status"] == target_status

            return func

        with self.NodeStopper(self) as ns:
            ns.make_unavailable(random.choice(self.redpanda.nodes),
                                failure_types=[FailureSpec.FAILURE_KILL])

            time.sleep(self.NODE_AVAILABILITY_TIMEOUT)

            # Wait until the balancer manages to move partitions from the killed node.
            # This will make other 4 nodes go over the disk usage limit and the balancer
            # will stall because there won't be any other node to move partitions to.
            self.wait_until_status(create_waiter("stalled"))
            self.check_no_replicas_on_node(ns.cur_failure.node)

            # bring failed node up
            ns.make_available()

        self.wait_until_status(create_waiter("ready"))
        for n in self.redpanda.nodes:
            disk_usage = get_disk_usage(n)
            used_ratio = disk_usage / disk_size
            self.logger.info(
                f"node {self.redpanda.idx(n)}: "
                f"disk used percentage: {int(100.0 * used_ratio)}")
            assert used_ratio < 0.8
