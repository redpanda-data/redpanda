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
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST
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
                'partition_autobalancing_mode': 'continuous',
                'partition_autobalancing_node_availability_timeout_sec':
                self.NODE_AVAILABILITY_TIMEOUT,
                'partition_autobalancing_tick_interval_ms': 5000,
                'raft_learner_recovery_rate': 1_000_000,
            },
            **kwargs)

    def node2partition_count(self):
        topics = [self.topic]
        rpk = RpkTool(self.redpanda)
        ret = {}
        for topic in topics:
            num_partitions = topic.partition_count

            def all_partitions_ready():
                try:
                    partitions = list(rpk.describe_topic(topic.name))
                except RpkException:
                    return False
                return (len(partitions) == num_partitions, partitions)

            partitions = wait_until_result(
                all_partitions_ready,
                timeout_sec=120,
                backoff_sec=1,
                err_msg="failed to wait until all partitions have leaders")

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
            self.logger.info(f'partition balancer status: {status}')

            if 'seconds_since_last_tick' not in status:
                return False
            return (req_start - status['seconds_since_last_tick'] - 1 > start
                    and predicate(status), status)

        return wait_until_result(
            check,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg="failed to wait until status condition")

    def wait_until_ready(self, timeout_sec=120):
        return self.wait_until_status(
            lambda status: status['status'] == 'ready',
            timeout_sec=timeout_sec)

    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_unavailable_nodes(self):
        self.start_redpanda(num_nodes=5)

        self.topic = TopicSpec(partition_count=random.randint(20, 30))
        self.client().create_topic(self.topic)

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        total_replicas = sum(self.node2partition_count().values())

        f_injector = FailureInjector(self.redpanda)
        prev_failure = None

        for n in range(10):
            node = self.redpanda.nodes[n % 5]
            failure_types = [
                FailureSpec.FAILURE_KILL, FailureSpec.FAILURE_TERMINATE,
                FailureSpec.FAILURE_SUSPEND
            ]
            failure = FailureSpec(random.choice(failure_types), node)
            f_injector._start_func(failure.type)(failure.node)

            if prev_failure:
                # heal the previous failure
                f_injector._stop_func(prev_failure.type)(prev_failure.node)

            time.sleep(self.NODE_AVAILABILITY_TIMEOUT)

            # TODO: enable when cancellation gets implemented
            # wait_for_quiescent_state = random.random() < 0.5
            wait_for_quiescent_state = True

            if wait_for_quiescent_state:
                self.wait_until_ready()

                node2pc = self.node2partition_count()
                self.logger.info(f'partition counts after: {node2pc}')

                assert sum(node2pc.values()) == total_replicas
                assert self.redpanda.idx(node) not in node2pc
            else:
                self.wait_until_status(lambda s: s['status'] == 'in_progress'
                                       or s['status'] == 'ready')

            prev_failure = failure

        self.run_validation()
