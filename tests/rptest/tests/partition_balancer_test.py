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
from math import ceil

from rptest.services.cluster import cluster
from rptest.services.admin import Admin
from rptest.util import wait_until_result
from rptest.utils.mode_checks import skip_debug_mode
from rptest.clients.default import DefaultClient
from rptest.services.redpanda import RedpandaService, CHAOS_LOG_ALLOW_LIST, MetricsEndpoint
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer
from rptest.services.kgo_verifier_services import KgoVerifierProducer

from rptest.tests.end_to_end import EndToEndTest
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool, RpkException
from ducktape.cluster.cluster_spec import ClusterSpec
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

# We inject failures which might cause consumer groups
# to re-negotiate, so it is necessary to have a longer
# consumer timeout than the default 30 seconds
CONSUMER_TIMEOUT = 90

# We can ignore race between deletion and adding different partitions, because rp retries adding after this error
RACE_BETWEEN_DELETION_AND_ADDING_PARTITION = [
    # e.g. cluster - controller_backend.cc:717 - [{kafka/fuzzy-operator-9578-devcbk/1}] exception while executing partition operation: {type: addition, ntp: {kafka/fuzzy-operator-9578-devcbk/1}, offset: 451, new_assignment: { id: 1, group_id: 57, replicas: {{node_id: 5, shard: 1}, {node_id: 4, shard: 0}, {node_id: 3, shard: 1}} }, previous_replica_set: {nullopt}} - std::__1::__fs::filesystem::filesystem_error (error system:2, filesystem error: mkdir failed: No such file or directory ["/var/lib/redpanda/data/kafka/fuzzy-operator-9578-devcbk/1_451"])"
    "cluster - .*std::__1::__fs::filesystem::filesystem_error \(error system:2, filesystem error: mkdir failed: No such file or directory"
]

STARTUP_SEQUENCE_ABORTED = [
    # Example: main - application.cc:335 - Failure during startup: seastar::abort_requested_exception (abort requested)
    "Failure during startup: seastar::abort_requested_exception \(abort requested\)"
]


class PartitionBalancerService(EndToEndTest):
    def __init__(self, ctx, *args, **kwargs):
        self.admin_fuzz = None
        super(PartitionBalancerService, self).__init__(
            ctx,
            *args,
            extra_rp_conf={
                "partition_autobalancing_mode": "continuous",
                "partition_autobalancing_node_availability_timeout_sec": 10,
                "partition_autobalancing_tick_interval_ms": 5000,
                "raft_learner_recovery_rate": 100_000_000,
                # set disk timeout to value greater than max suspend time
                # not to emit spurious errors
                "raft_io_timeout_ms": 30000,
            },
            **kwargs,
        )

    def tearDown(self):
        if self.admin_fuzz is not None:
            self.admin_fuzz.stop()

        return super().tearDown()

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

    def get_partition_amount_on_node(self, node):
        admin = Admin(self.redpanda)
        return len(
            list(
                filter(lambda x: x["ns"] == "kafka",
                       admin.get_partitions(node=node))))

    def validate_metrics_node_down(self, disabled_node,
                                   expected_moving_partitions_amount):
        self.logger.debug(
            f"Checking metrics for movement with disabled node: {self.redpanda.idx(disabled_node)}"
        )
        availabale_nodes = list(
            filter(lambda x: x != disabled_node, self.redpanda.nodes))
        metrics = self.redpanda.metrics_sample(
            "moving_to_node",
            availabale_nodes,
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        moving_to_sum = int(sum([metric.value for metric in metrics.samples]))

        self.logger.debug(f"\
            moving_to_sum: {moving_to_sum}, \
            expected_moving_partitions_amount: {expected_moving_partitions_amount}"
                          )

        if moving_to_sum != expected_moving_partitions_amount:
            return False

        metrics = self.redpanda.metrics_sample(
            "moving_from_node",
            availabale_nodes,
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        moving_from_sum = int(sum([metric.value
                                   for metric in metrics.samples]))
        self.logger.debug(f"moving_from_sum: {moving_from_sum}")
        if moving_from_sum != 0:
            return False

        metrics = self.redpanda.metrics_sample(
            "node_cancelling_movements",
            availabale_nodes,
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        cancelling_movements = int(
            sum([metric.value for metric in metrics.samples]))
        self.logger.debug(f"cancelling_movements: {cancelling_movements}")
        if cancelling_movements != 0:
            return False

        return True

    def check_metrics(self, disabled_node, expected_moving_partitions_amount):
        return wait_until(lambda: self.validate_metrics_node_down(
            disabled_node, expected_moving_partitions_amount),
                          timeout_sec=10)

    def wait_until_status(self, predicate, timeout_sec=120):
        # We may get a 504 if we proxy a status request to a suspended node.
        # It is okay to retry (the controller leader will get re-elected in the meantime).
        admin = Admin(self.redpanda, retry_codes=[503, 504], retries_amount=10)
        start = time.time()

        def check():
            req_start = time.time()

            status = admin.get_partition_balancer_status(timeout=10)
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

    def wait_until_ready(self,
                         timeout_sec=120,
                         expected_unavailable_node=None):
        if expected_unavailable_node:
            node_id = self.redpanda.idx(expected_unavailable_node)
            self.logger.info(f"waiting for quiescent state, "
                             f"expected unavailable node: {node_id}")
        else:
            node_id = None
            self.logger.info(f"waiting for quiescent state")

        def predicate(status):
            return (status["status"] == "ready" and
                    ((node_id is None) or (node_id in status["violations"].get(
                        "unavailable_nodes", []))))

        return self.wait_until_status(predicate, timeout_sec=timeout_sec)

    def check_no_replicas_on_node(self, node):
        node2pc = self.node2partition_count()
        self.logger.info(f"partition counts: {node2pc}")
        total_replicas = self.topic.partition_count * self.topic.replication_factor
        assert sum(node2pc.values()) == total_replicas
        assert self.redpanda.idx(node) not in node2pc

    class NodeStopper:
        """Stop/kill/freeze one node at a time and wait for partition balancer."""
        def __init__(self, test, exclude_unavailable_nodes=False):
            self.test = test
            self.f_injector = FailureInjector(test.redpanda)
            self.cur_failure = None
            self.logger = test.logger
            self.exclude_unavailable_nodes = exclude_unavailable_nodes

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
                    self.logger.info(
                        f"made {self.cur_failure.node.account.hostname} available"
                    )
                if self.exclude_unavailable_nodes:
                    self.test.redpanda.add_to_started_nodes(
                        self.cur_failure.node)
                self.cur_failure = None

        def make_unavailable(self,
                             node,
                             failure_types=None,
                             wait_for_previous_node=False):
            self.make_available(wait_for_previous_node)
            self.logger.info(f"making {node.account.hostname} unavailable")

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
            if self.exclude_unavailable_nodes:
                self.test.redpanda.remove_from_started_nodes(node)

            self.logger.info(f"made {node.account.hostname} unavailable")


class PartitionBalancerTest(PartitionBalancerService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @skip_debug_mode
    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    @matrix(decom_before_add=[True, False])
    def test_restack_nodes(self, decom_before_add):
        extra_rp_conf = {"raft_learner_recovery_rate": 100 * 1024 * 1024}
        self.start_redpanda(num_nodes=5,
                            extra_rp_conf=extra_rp_conf,
                            new_bootstrap=True,
                            max_num_seeds=5)
        self.topic = TopicSpec(partition_count=1)
        self.client().create_topic(self.topic)

        self.start_producer(1, throughput=10)
        self.start_consumer(1)
        self.await_startup()

        redpanda = self.redpanda
        used_node_ids = [self.redpanda.node_id(n) for n in self.redpanda.nodes]

        # Wipe nodes in random order to exercise more than just decommissioning
        # the first node first.
        rand_order = list(range(1, len(self.redpanda.nodes) + 1))
        random.shuffle(rand_order)
        for i in rand_order:
            node = self.redpanda.get_node(i)
            # Wipe the node such that its next restart will assign it a new
            # node ID.
            old_node_id = redpanda.node_id(node)

            def decommission_node():
                self.redpanda._admin.decommission_broker(id=old_node_id)

                def node_removed():
                    try:
                        brokers = self.redpanda._admin.get_brokers()
                    except:
                        return False
                    return not any(b['node_id'] == old_node_id
                                   for b in brokers)

                wait_until(node_removed, timeout_sec=120, backoff_sec=2)

            if decom_before_add:
                decommission_node()

            redpanda.stop_node(node)
            redpanda.clean_node(node,
                                preserve_logs=True,
                                preserve_current_install=True)
            redpanda.start_node(node,
                                auto_assign_node_id=True,
                                omit_seeds_on_idx_one=False)

            if not decom_before_add:
                decommission_node()

            # The revived node should have a new node ID.
            new_node_id = self.redpanda.node_id(node, force_refresh=True)
            assert new_node_id not in used_node_ids, \
                f"Expected new node ID for {old_node_id}, got {new_node_id}"
            used_node_ids.append(new_node_id)

            self.logger.debug(
                f"Wiped and restarted node ID {old_node_id} at {node.account.hostname}"
            )
            wait_until(self.redpanda.healthy, timeout_sec=120, backoff_sec=1)
        self.run_validation(min_records=100,
                            consumer_timeout_sec=CONSUMER_TIMEOUT)

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
                ns.make_unavailable(node)

                if n in {1, 3, 4}:
                    # For these nodes we don't wait for the quiescent state,
                    # but just for the partition movement to start and then
                    # move to the next node.

                    self.logger.info(
                        f"waiting for partition balancer to kick in")

                    node_id = self.redpanda.idx(node)

                    def predicate(s):
                        if s["status"] == "in_progress":
                            return True
                        elif s["status"] == "ready":
                            return node_id in s["violations"].get(
                                "unavailable_nodes", [])
                        else:
                            return False

                    self.wait_until_status(predicate)
                else:
                    self.wait_until_ready(expected_unavailable_node=node)
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
            self.wait_until_ready(expected_unavailable_node=empty_node)
            ns.make_available()
            self.check_no_replicas_on_node(empty_node)

            # throttle recovery to prevent partition move from finishing
            self._throttle_recovery(10)

            for n in range(3):
                node = self.redpanda.nodes[n % 3]
                partitions_amount_to_move = self.get_partition_amount_on_node(
                    node)
                ns.make_unavailable(node,
                                    failure_types=[FailureSpec.FAILURE_KILL])

                # wait until movement start
                self.wait_until_status(lambda s: s["status"] == "in_progress")
                self.check_metrics(node, partitions_amount_to_move)
                ns.make_available()

                # stop empty node
                ns.make_unavailable(empty_node,
                                    failure_types=[FailureSpec.FAILURE_KILL])

                # wait until movements are cancelling
                self.wait_until_ready(expected_unavailable_node=empty_node)

                self.check_no_replicas_on_node(empty_node)

            self.run_validation(consumer_timeout_sec=CONSUMER_TIMEOUT)

    def check_rack_placement(self, topic, rack_layout):
        for p in self.topic_partitions_replicas(topic):
            racks = {rack_layout[r - 1] for r in p.replicas}
            assert (
                len(racks) == 3
            ), f"bad rack placement {racks} for partition id: {p.id} (replicas: {p.replicas})"

    @skip_debug_mode
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

        self.check_rack_placement(self.topic, rack_layout)

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        with self.NodeStopper(self) as ns:
            for n in range(7):
                node = self.redpanda.nodes[n % len(self.redpanda.nodes)]
                ns.make_unavailable(node, wait_for_previous_node=True)
                self.wait_until_ready(expected_unavailable_node=node)
                self.check_no_replicas_on_node(node)
                self.check_rack_placement(self.topic, rack_layout)

            ns.make_available()
            self.run_validation(consumer_timeout_sec=CONSUMER_TIMEOUT)

    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_rack_constraint_repair(self):
        """
        Test partition balancer rack constraint repair with the following scenario:
        * kill a node, making the whole AZ unavailable.
        * partition balancer will move partitions to remaining nodes,
          causing rack awareness constraint to be violated
        * start another node, making the number of available AZs enough to satisfy
          rack constraint
        * check that the balancer repaired the constraint
        """

        extra_rp_conf = self._extra_rp_conf | {"enable_rack_awareness": True}
        self.redpanda = RedpandaService(self.test_context,
                                        num_brokers=5,
                                        extra_rp_conf=extra_rp_conf)

        rack_layout = "ABBCC"
        for ix, node in enumerate(self.redpanda.nodes):
            self.redpanda.set_extra_node_conf(node, {"rack": rack_layout[ix]})

        self.redpanda.start(nodes=self.redpanda.nodes[0:4])
        self._client = DefaultClient(self.redpanda)

        self.topic = TopicSpec(partition_count=random.randint(20, 30))
        self.client().create_topic(self.topic)

        self.check_rack_placement(self.topic, rack_layout)

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        def num_with_broken_rack_constraint() -> int:
            metrics = self.redpanda.metrics_sample(
                "num_with_broken_rack_constraint",
                nodes=self.redpanda.nodes[0:3])
            self.logger.debug(f"samples: {metrics.samples}")

            val = int(max(s.value for s in metrics.samples))
            self.logger.debug(
                f"number of partitions with broken rack constraint: {val}")
            return val

        with self.NodeStopper(self) as ns:
            assert num_with_broken_rack_constraint() == 0

            node = self.redpanda.nodes[3]
            num_partitions_on_killed_node = len(
                Admin(self.redpanda).get_partitions(node=node))
            self.logger.info(
                f"number of partitions on killed node: {num_partitions_on_killed_node}"
            )

            ns.make_unavailable(node)
            self.wait_until_ready(expected_unavailable_node=node)

            # - 1 comes from the controller partition
            assert num_with_broken_rack_constraint(
            ) == num_partitions_on_killed_node - 1

            self.redpanda.start_node(self.redpanda.nodes[4])

            self.wait_until_ready(expected_unavailable_node=node)
            self.check_rack_placement(self.topic, rack_layout)

        self.run_validation(consumer_timeout_sec=CONSUMER_TIMEOUT)
        assert num_with_broken_rack_constraint() == 0

    @cluster(num_nodes=7,
             log_allow_list=CHAOS_LOG_ALLOW_LIST +
             RACE_BETWEEN_DELETION_AND_ADDING_PARTITION)
    def test_fuzz_admin_ops(self):
        self.start_redpanda(num_nodes=5)

        self.topic = TopicSpec(partition_count=random.randint(20, 30))
        self.client().create_topic(self.topic)

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        self.admin_fuzz = AdminOperationsFuzzer(self.redpanda,
                                                min_replication=3,
                                                retries=0)
        self.admin_fuzz.start()

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

        with self.NodeStopper(self, exclude_unavailable_nodes=True) as ns:
            for n in range(7):
                node = self.redpanda.nodes[n % len(self.redpanda.nodes)]
                try:
                    self.admin_fuzz.pause()
                    ns.make_unavailable(node, wait_for_previous_node=True)
                    self.wait_until_ready(expected_unavailable_node=node)
                    node2partition_count = get_node2partition_count()
                    assert self.redpanda.idx(node) not in node2partition_count
                finally:
                    self.admin_fuzz.unpause()
                self.admin_fuzz.ensure_progress()

            self.admin_fuzz.stop()

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

        disk_size = 10 * 1024 * 1024 * 1024
        self.start_redpanda(
            num_nodes=5,
            extra_rp_conf={
                "storage_min_free_bytes": 10 * 1024 * 1024,
                "raft_learner_recovery_rate": 100_000_000,
                "health_monitor_max_metadata_age": 3000,
                "log_segment_size": 104857600,  # 100 MiB
            },
            environment={"__REDPANDA_TEST_DISK_SIZE": disk_size})

        self.topic = TopicSpec(partition_count=30)
        self.client().create_topic(self.topic)

        def get_avg_disk_usage():
            return sum([
                self.redpanda.get_node_disk_usage(n)
                for n in self.redpanda.nodes
            ]) / len(self.redpanda.nodes) / disk_size

        msg_size = 102_400
        produce_batch_size = ceil(disk_size / msg_size / 30)
        while get_avg_disk_usage() < 0.7:
            producer = KgoVerifierProducer(self.test_context,
                                           self.redpanda,
                                           self.topic,
                                           msg_size=msg_size,
                                           msg_count=produce_batch_size)
            producer.start(clean=False)
            producer.wait_for_acks(produce_batch_size,
                                   timeout_sec=120,
                                   backoff_sec=5)
            producer.stop()
            producer.free()

        def print_disk_usage_per_node():
            for n in self.redpanda.nodes:
                disk_usage = self.redpanda.get_node_disk_usage(n)
                self.logger.info(
                    f"node {self.redpanda.idx(n)}: "
                    f"disk used percentage: {int(100.0 * disk_usage/disk_size)}"
                )

        with self.NodeStopper(self) as ns:
            ns.make_unavailable(random.choice(self.redpanda.nodes),
                                failure_types=[FailureSpec.FAILURE_KILL])

            # Wait until the balancer manages to move partitions from the killed node.
            # This will make other 4 nodes go over the disk usage limit and the balancer
            # will stall because there won't be any other node to move partitions to.

            # waiter that prints current node disk usage while it waits.
            def is_stalled(s):
                print_disk_usage_per_node()
                return s["status"] == "stalled"

            self.wait_until_status(is_stalled)
            self.check_no_replicas_on_node(ns.cur_failure.node)

            # bring failed node up
            ns.make_available()

        # Wait until the balancer is done and stable for a few ticks in a row
        ready_appeared_at = None

        def is_ready_and_stable(s):
            print_disk_usage_per_node()
            nonlocal ready_appeared_at
            if s["status"] == "ready":
                if ready_appeared_at is None:
                    ready_appeared_at = time.time()
                else:
                    # ready status is stable for 11 seconds, should be enough for 3 ticks to pass
                    return time.time() - ready_appeared_at > 11.0
            else:
                ready_appeared_at = None

        self.wait_until_status(is_ready_and_stable)

        for n in self.redpanda.nodes:
            disk_usage = self.redpanda.get_node_disk_usage(n)
            used_ratio = disk_usage / disk_size
            self.logger.info(
                f"node {self.redpanda.idx(n)}: "
                f"disk used percentage: {int(100.0 * used_ratio)}")
            # Checking that used_ratio is less than 81 instead of 80
            # Because when we check, disk can become overfilled one more time
            # and partition balancing is not invoked yet
            assert used_ratio < 0.81

    @cluster(num_nodes=7, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    @matrix(kill_same_node=[True, False])
    def test_maintenance_mode(self, kill_same_node):
        """
        Test interaction with maintenance mode: the balancer should not schedule
        any movements as long as there is a node in maintenance mode.
        Test scenario is as follows:
        * enable maintenance mode on some node
        * kill a node (the same or some other)
        * check that we don't move any partitions and node switched to maintenance
          mode smoothly
        * turn maintenance mode off, check that partition balancing resumes
        """
        self.start_redpanda(num_nodes=5)

        self.topic = TopicSpec(partition_count=random.randint(20, 30))
        self.client().create_topic(self.topic)

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        # throttle recovery to simulate long-running partition moves
        self._throttle_recovery(10)

        # choose a node
        node = random.choice(self.redpanda.nodes)
        node_id = self.redpanda.idx(node)

        rpk = RpkTool(self.redpanda)
        admin = Admin(self.redpanda)

        rpk.cluster_maintenance_enable(node, wait=True)
        # the node should now report itself in maintenance mode
        assert admin.maintenance_status(node)["draining"], \
                    f"{node.name} not in expected maintenance mode"

        def entered_maintenance_mode(node):
            try:
                status = admin.maintenance_status(node)
            except IOError:
                return False
            return status["finished"] and not status["errors"] and \
                status["partitions"] > 0

        with self.NodeStopper(self) as ns:
            if kill_same_node:
                to_kill = node
            else:
                to_kill = random.choice([
                    n for n in self.redpanda.nodes
                    if self.redpanda.idx(n) != node_id
                ])

            ns.make_unavailable(to_kill)
            self.wait_until_status(lambda s: s["status"] == "stalled")

            if kill_same_node:
                ns.make_available()

            wait_until(lambda: entered_maintenance_mode(node),
                       timeout_sec=120,
                       backoff_sec=10)

            # return back to normal
            rpk.cluster_maintenance_disable(node)
            # use raw admin interface to avoid waiting for the killed node
            admin.patch_cluster_config(
                {"raft_learner_recovery_rate": 100_000_000})

            if kill_same_node:
                self.wait_until_ready()
            else:
                self.wait_until_ready(expected_unavailable_node=to_kill)
                self.check_no_replicas_on_node(to_kill)

        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=CONSUMER_TIMEOUT)

    @cluster(num_nodes=7,
             log_allow_list=CHAOS_LOG_ALLOW_LIST + STARTUP_SEQUENCE_ABORTED)
    @matrix(kill_same_node=[True, False], decommission_first=[True, False])
    def test_decommission(self, kill_same_node, decommission_first):
        """
        Test interaction with decommissioning: the balancer should not schedule
        any movements to decommissioning nodes and cancel any movements from
        decommissioning nodes.

        In this test we test that decommission finishes in the following 4 scenarios:
        * we first kill a node / we first decommission a node
        * we kill the same / different node from the one we decommission
        """

        self.start_redpanda(num_nodes=5)

        self.topic = TopicSpec(partition_count=random.randint(20, 30))
        self.client().create_topic(self.topic)

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        # choose a node
        to_decom = random.choice(self.redpanda.nodes)
        to_decom_id = self.redpanda.idx(to_decom)

        if kill_same_node:
            to_kill = to_decom
            to_kill_id = to_decom_id
        else:
            to_kill = random.choice([
                n for n in self.redpanda.nodes
                if self.redpanda.idx(n) != to_decom_id
            ])
            to_kill_id = self.redpanda.idx(to_kill)

        # throttle recovery to prevent partition moves from finishing
        self.logger.info("throttling recovery")
        self._throttle_recovery(10)

        admin = Admin(self.redpanda)

        if decommission_first:
            self.logger.info(f"decommissioning node: {to_decom.name}")
            admin.decommission_broker(to_decom_id)

        with self.NodeStopper(self) as ns:
            # kill the node and wait for the partition balancer to kick in
            ns.make_unavailable(to_kill,
                                failure_types=[FailureSpec.FAILURE_KILL])
            self.wait_until_status(lambda s: s["status"] == "in_progress")

            if not decommission_first:
                self.logger.info(f"decommissioning node: {to_decom.name}")
                admin.decommission_broker(to_decom_id)

            # A node which isn't being decommed, to use when calling into
            # the admin API from this point onwards.
            survivor_node = random.choice([
                n for n in self.redpanda.nodes
                if self.redpanda.idx(n) not in {to_kill_id, to_decom_id}
            ])
            self.logger.info(
                f"Using survivor node {survivor_node.name} {self.redpanda.idx(survivor_node)}"
            )

            def started_draining():
                b = [
                    b for b in admin.get_brokers(survivor_node)
                    if b['node_id'] == to_decom_id
                ]
                return len(b) > 0 and b[0]["membership_status"] == "draining"

            wait_until(started_draining, timeout_sec=60, backoff_sec=2)

            # wait for balancer tick
            self.wait_until_status(lambda s: s["status"] != "starting")

            # back to normal recovery speed
            self.logger.info("bringing back recovery speed")
            # use raw admin interface to avoid waiting for the killed node
            new_version = admin.patch_cluster_config(
                {"raft_learner_recovery_rate": 100_000_000})['config_version']

            alive_nodes = [
                n for n in self.redpanda.nodes
                if self.redpanda.idx(n) is not to_kill_id
            ]

            def config_applied():
                status = admin.get_cluster_config_status(node=survivor_node)
                self.logger.debug(
                    f"cluster status: {status}, checking for live nodes: {alive_nodes}"
                )
                return all(k["config_version"] >= new_version for k in status
                           if k["node_id"] in alive_nodes)

            wait_until(config_applied, timeout_sec=60, backoff_sec=2)

        # Check that the node has been successfully decommissioned.
        # We have to bring back failed node because otherwise decommission won't finish.
        def node_removed():
            brokers = admin.get_brokers(node=survivor_node)
            return not any(b['node_id'] == to_decom_id for b in brokers)

        wait_until(node_removed, timeout_sec=120, backoff_sec=2)
        # stop the decommissioned node (this must be done manually) so that
        # it doesn't respond to admin API requests.
        self.redpanda.stop_node(to_decom)

        self.wait_until_ready()

        self.run_validation(enable_idempotence=False,
                            consumer_timeout_sec=CONSUMER_TIMEOUT)

    @cluster(num_nodes=4, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_transfer_controller_leadership(self):
        """
        Test that unavailability timeout is correctly restarted after controller
        leadership change. We set the availability timeout to a big value and
        check that partition rebalancing doesn't start after the node is killed
        and leadership changes.
        """
        self.start_redpanda(
            num_nodes=4,
            extra_rp_conf={
                "partition_autobalancing_node_availability_timeout_sec": 3600,
            },
        )
        self.topic = TopicSpec(partition_count=random.randint(20, 30))
        self.client().create_topic(self.topic)

        # throttle recovery to prevent partition moves from finishing
        self.logger.info("throttling recovery")
        self._throttle_recovery(10)

        with self.NodeStopper(self) as ns:
            to_kill = random.choice(self.redpanda.nodes)
            ns.make_unavailable(to_kill,
                                failure_types=[FailureSpec.FAILURE_KILL])
            self.wait_until_ready()

            controller = self.redpanda.controller()
            controller_id = self.redpanda.idx(controller)
            admin = Admin(self.redpanda)

            transfer_to = random.choice([
                n for n in self.redpanda.nodes
                if n.name != to_kill.name and n.name != controller.name
            ])
            transfer_to_idx = self.redpanda.idx(transfer_to)
            self.logger.info(
                f"transferring raft0 leadership from {controller_id} to {transfer_to_idx} "
                f"(killed node: {self.redpanda.idx(to_kill)})")

            admin.transfer_leadership_to(namespace='redpanda',
                                         topic='controller',
                                         partition=0,
                                         leader_id=controller_id,
                                         target_id=transfer_to_idx)

            self.wait_until_ready()
