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

    def run_node_stop_start_chain(self, steps, additional_check=None):

        total_replicas = sum(self.node2partition_count().values())

        f_injector = FailureInjector(self.redpanda)
        prev_failure = None

        for n in range(steps):
            node = self.redpanda.nodes[n % len(self.redpanda.nodes)]
            failure_types = [
                FailureSpec.FAILURE_KILL,
                FailureSpec.FAILURE_TERMINATE,
                FailureSpec.FAILURE_SUSPEND,
            ]
            failure = FailureSpec(random.choice(failure_types), node)
            f_injector._start_func(failure.type)(failure.node)

            if prev_failure:
                # heal the previous failure
                f_injector._stop_func(prev_failure.type)(prev_failure.node)

            time.sleep(self.NODE_AVAILABILITY_TIMEOUT)

            self.wait_until_ready()

            node2pc = self.node2partition_count()
            self.logger.info(f"partition counts after: {node2pc}")

            assert sum(node2pc.values()) == total_replicas
            assert self.redpanda.idx(node) not in node2pc

            if additional_check:
                additional_check()

            prev_failure = failure

    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_unavailable_nodes(self):
        self.start_redpanda(num_nodes=5)

        self.topic = TopicSpec(partition_count=random.randint(20, 30))
        self.client().create_topic(self.topic)

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        self.run_node_stop_start_chain(steps=7)

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
        node2pc = self.node2partition_count()
        self.logger.info(f"partition counts after: {node2pc}")
        assert self.redpanda.idx(empty_node) not in node2pc

        # throttle recovery to prevent partition move from finishing
        self._throttle_recovery(10)

        total_replicas = sum(self.node2partition_count().values())

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

            node2pc = self.node2partition_count()
            self.logger.info(f"partition counts after: {node2pc}")

            assert sum(node2pc.values()) == total_replicas
            assert self.redpanda.idx(empty_node) not in node2pc

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

        self.run_node_stop_start_chain(steps=3,
                                       additional_check=check_rack_placement)

        self.run_validation()
