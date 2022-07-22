# Copyright 2022 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import random
import time

from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.util import wait_until_result
from rptest.clients.default import DefaultClient
from rptest.services.redpanda import RedpandaService, CHAOS_LOG_ALLOW_LIST
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer
from rptest.tests.end_to_end import EndToEndTest
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool, RpkException


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

        def make_unavailable(self, node):
            if self.cur_failure:
                # heal the previous failure
                self.f_injector._stop_func(self.cur_failure.type)(
                    self.cur_failure.node)

            failure_types = [
                FailureSpec.FAILURE_KILL,
                FailureSpec.FAILURE_TERMINATE,
                FailureSpec.FAILURE_SUSPEND,
            ]
            self.cur_failure = FailureSpec(random.choice(failure_types), node)
            self.f_injector._start_func(self.cur_failure.type)(
                self.cur_failure.node)
            time.sleep(self.test.NODE_AVAILABILITY_TIMEOUT)

        def step_and_wait_for_in_progress(self, node):
            self.make_unavailable(node)
            self.test.logger.info(f"waiting for partition balancer to kick in")
            self.test.wait_until_status(lambda s: s["status"] == "in_progress"
                                        or s["status"] == "ready")

        def step_and_wait_for_ready(self, node):
            self.make_unavailable(node)
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

        ns = self.NodeStopper(self)
        for n in range(7):
            node = self.redpanda.nodes[n % len(self.redpanda.nodes)]
            if n in {1, 3, 4}:
                ns.step_and_wait_for_in_progress(node)
            else:
                ns.step_and_wait_for_ready(node)
                self.check_no_replicas_on_node(node)

        self.run_validation()

    def _throttle_recovery(self, new_value):
        self.redpanda.set_cluster_config(
            {"raft_learner_recovery_rate": str(new_value)})

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_movement_cancellations(self):
        self.start_redpanda(num_nodes=4)

        self.topic = TopicSpec(partition_count=random.randint(20, 30))

        f_injector = FailureInjector(self.redpanda)

        self.client().create_topic(self.topic)

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        empty_node = self.redpanda.nodes[-1]

        # clean node
        initial_failure = FailureSpec(FailureSpec.FAILURE_KILL, empty_node)
        f_injector._start_func(initial_failure.type)(initial_failure.node)
        time.sleep(self.NODE_AVAILABILITY_TIMEOUT)
        self.wait_until_ready()
        f_injector._stop_func(initial_failure.type)(initial_failure.node)
        self.check_no_replicas_on_node(empty_node)

        # throttle recovery to prevent partition move from finishing
        self._throttle_recovery(10)

        for n in range(3):
            node = self.redpanda.nodes[n % 3]
            failure = FailureSpec(FailureSpec.FAILURE_KILL, node)
            f_injector._start_func(failure.type)(failure.node)

            time.sleep(self.NODE_AVAILABILITY_TIMEOUT)

            # wait until movement start
            self.wait_until_status(lambda s: s["status"] == "in_progress")
            f_injector._stop_func(failure.type)(failure.node)

            # stop empty node
            f_injector._start_func(initial_failure.type)(initial_failure.node)
            time.sleep(self.NODE_AVAILABILITY_TIMEOUT)

            # wait until movements are cancelled
            self.wait_until_ready()

            self.check_no_replicas_on_node(empty_node)

            f_injector._stop_func(initial_failure.type)(initial_failure.node)

        self.run_validation()

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

        ns = self.NodeStopper(self)
        for n in range(7):
            node = self.redpanda.nodes[n % len(self.redpanda.nodes)]
            ns.step_and_wait_for_ready(node)
            self.check_no_replicas_on_node(node)
            check_rack_placement()

        self.run_validation()

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

        ns = self.NodeStopper(self)
        for n in range(7):
            node = self.redpanda.nodes[n % len(self.redpanda.nodes)]
            ns.make_unavailable(node)
            try:
                admin_fuzz.pause()
                self.wait_until_ready()
                node2partition_count = {}
                for t in describe_topics():
                    for p in t.partitions:
                        for r in p.replicas:
                            node2partition_count[
                                r] = node2partition_count.setdefault(r, 0) + 1
                self.logger.info(f"partition counts: {node2partition_count}")
                assert self.redpanda.idx(node) not in node2partition_count

            finally:
                admin_fuzz.unpause()

        admin_fuzz.wait(count=10, timeout=240)
        admin_fuzz.stop()

        self.run_validation()
