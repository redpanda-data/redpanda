# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import sys
import time
import re
import random
import ducktape.errors
from typing import Optional
import requests

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.types import TopicSpec
from rptest.clients.ping_pong import PingPong
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.rpk_producer import RpkProducer
from rptest.services.kaf_producer import KafProducer
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.metrics_check import MetricCheck

ELECTION_TIMEOUT = 10

# Logs that may appear when a node is network-isolated
ISOLATION_LOG_ALLOW_LIST = [
    # rpc - server.cc:91 - vectorized internal rpc protocol - Error[shutting down] remote address: 10.89.0.16:60960 - std::__1::system_error (error system:32, sendmsg: Broken pipe)
    "(kafka|rpc) - .*Broken pipe",
]


class RaftAvailabilityTest(RedpandaTest):
    """
    Validates key availability properties of the system using a single
    partition.
    """
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        super(RaftAvailabilityTest, self).__init__(
            test_ctx,
            *args,
            extra_rp_conf={
                # Disable leader balancer to enable testing
                # leadership stability.
                "enable_leader_balancer": False,
                "id_allocator_replication": 3
            },
            **kwargs)

    def _get_leader(self):
        """
        :returns: 2 tuple of (leader, [replica ids])
        """
        return KafkaCat(self.redpanda).get_partition_leader(self.topic, 0)

    def _wait_for_leader(self, condition=None, timeout=None):
        if timeout is None:
            timeout = ELECTION_TIMEOUT

        t1 = time.time()
        result = {}

        if condition is None:
            condition = lambda x: x is not None

        def check():
            result[0] = self._get_leader()
            return condition(result[0][0])

        wait_until(check,
                   timeout_sec=timeout,
                   backoff_sec=0.5,
                   err_msg=f"No leader emerged!")

        duration = time.time() - t1
        self.logger.info(
            f"New leader {result[0][0]} (of {result[0][1]}) after {duration}s")

        # FIXME: this way of querying leadership is shaky in tests
        # because it depends on metadata dissemination.  We can read
        # one leader result from broker A and immediately after get
        # a different answer if we asked broker B.

        assert result[0] is not None
        return result[0]

    def ping_pong(self):
        return PingPong(self.redpanda.brokers_list(), self.topic, 0,
                        self.logger)

    def _is_available(self, timeout_s=5):
        try:
            # Should fail
            self.ping_pong().ping_pong(timeout_s)
        except:
            return False
        else:
            return True

    def _expect_unavailable(self):
        try:
            # Should fail
            self.ping_pong().ping_pong()
        except:
            self.logger.exception("Cluster is unavailable as expected")
        else:
            assert False, "ping_pong should not have worked "

    def _expect_available(self):
        self.ping_pong().ping_pong()
        self.logger.info("Cluster is available as expected")

    def _transfer_leadership(self, admin: Admin, namespace: str, topic: str,
                             target_id: int) -> None:

        count = 0
        while True:
            count += 1
            self.logger.info(f"Waiting for a leader")
            leader_id = admin.await_stable_leader(topic,
                                                  partition=0,
                                                  namespace=namespace,
                                                  timeout_s=30,
                                                  backoff_s=2)
            self.logger.info(f"Current leader {leader_id}")

            if leader_id == target_id:
                return

            self.logger.info(f"Starting transfer to {target_id}")
            requests.exceptions.HTTPError
            try:
                admin.transfer_leadership_to(topic=topic,
                                             namespace=namespace,
                                             partition=0,
                                             target_id=target_id,
                                             leader_id=leader_id)
            except requests.exceptions.HTTPError as e:
                if count <= 10 and e.response.status_code == 503:
                    self.logger.info(
                        f"Got 503: {leader_id}'s metadata hasn't been updated yet"
                    )
                    time.sleep(1)
                    continue
                raise
            break

        admin.await_stable_leader(topic,
                                  partition=0,
                                  namespace=namespace,
                                  timeout_s=ELECTION_TIMEOUT * 4,
                                  backoff_s=2,
                                  check=lambda node_id: node_id != leader_id)

        self.logger.info(f"Completed transfer to {target_id}")

    @cluster(num_nodes=3)
    def test_one_node_down(self):
        """
        Simplest HA test.  Stop the leader for our partition.  Validate that
        the cluster remains available afterwards, and that the expected
        peer takes over as the new leader.
        """
        # Find which node is the leader
        admin = Admin(self.redpanda)
        initial_leader_id, replicas = self._wait_for_leader()
        assert initial_leader_id == replicas[0]

        self._expect_available()

        allocator_info = admin.wait_stable_configuration(
            "id_allocator",
            namespace="kafka_internal",
            replication=3,
            timeout_s=ELECTION_TIMEOUT * 2)

        leader_node = self.redpanda.get_node_by_id(initial_leader_id)
        self.logger.info(
            f"Initial leader {initial_leader_id} {leader_node.account.hostname}"
        )
        self.logger.info(f"id_allocator leader {allocator_info.leader}")

        # Priority mechanism should reliably select next replica in list
        expect_new_leader_id = replicas[1]
        expect_new_leader_node = self.redpanda.get_node_by_id(
            expect_new_leader_id)

        observer_node_id = (set(replicas) -
                            {expect_new_leader_id, initial_leader_id}).pop()
        observer_node = self.redpanda.get_node_by_id(observer_node_id)
        self.logger.info(
            f"Tracking stats on observer node {observer_node_id} {observer_node.account.hostname}"
        )
        self.logger.info(
            f"Tracking stats on expected new leader node {expect_new_leader_id} {expect_new_leader_node.account.hostname}"
        )

        observer_metrics = MetricCheck(self.logger, self.redpanda,
                                       observer_node,
                                       re.compile("vectorized_raft_.*"),
                                       {'topic': self.topic})

        new_leader_metrics = MetricCheck(self.logger, self.redpanda,
                                         expect_new_leader_node,
                                         re.compile("vectorized_raft_.*"),
                                         {'topic': self.topic})

        self.logger.info(
            f"Stopping node {initial_leader_id} ({leader_node.account.hostname})"
        )
        self.redpanda.stop_node(leader_node)

        if allocator_info.leader == initial_leader_id:
            hosts = [
                n.account.hostname for n in self.redpanda.nodes
                if self.redpanda.idx(n) != initial_leader_id
            ]
            admin.await_stable_leader(
                "id_allocator",
                namespace="kafka_internal",
                replication=3,
                timeout_s=ELECTION_TIMEOUT * 2,
                hosts=hosts,
                check=lambda node_id: node_id != initial_leader_id)

        new_leader, _ = self._wait_for_leader(
            lambda l: l == expect_new_leader_id)
        self.logger.info(f"Leadership moved to {new_leader}")

        self._expect_available()

        # Check that metrics have progressed in the expected direction.  Not doing exact
        # value checks (for e.g. how many elections happened) because a sufficiently
        # noisy test environment can violate even quite long timeouts (e.g. the 1500ms
        # election timeout).
        #
        # It would be good to impose stricter checks, to detect bugs that manifest as
        # elections taking more iterations than expected, once we have a less contended
        # test environment to execute in.
        observer_metrics.expect([
            ("vectorized_raft_leadership_changes_total", lambda a, b: b == a),
            ("vectorized_raft_leader_for", lambda a, b: int(b) == 0),
            ("vectorized_raft_received_vote_requests_total",
             lambda a, b: b > a),
        ])

        new_leader_metrics.expect([
            ("vectorized_raft_leadership_changes_total", lambda a, b: b > a),
            ("vectorized_raft_leader_for", lambda a, b: int(b) == 1),
            ("vectorized_raft_received_vote_requests_total",
             lambda a, b: b == a),
        ])

    @cluster(num_nodes=3)
    def test_two_nodes_down(self):
        """
        Validate that when two nodes are down, the cluster becomes unavailable, and
        that when one of those nodes is restored, the cluster becomes available again.
        """
        admin = Admin(self.redpanda)

        # Find which node is the leader
        initial_leader_id, replicas = self._wait_for_leader()

        self.ping_pong().ping_pong()

        leader_node = self.redpanda.get_node_by_id(initial_leader_id)
        other_node_id = (set(replicas) - {initial_leader_id}).pop()
        other_node = self.redpanda.get_node_by_id(other_node_id)

        self.logger.info(
            f"Stopping {initial_leader_id} ({leader_node.account.hostname}) and {other_node_id} ({other_node.account.hostname})"
        )
        self.redpanda.stop_node(leader_node)
        self.redpanda.stop_node(other_node)

        # 2/3 nodes down, cluster should be unavailable for acks=-1
        self._expect_unavailable()

        # Bring back one node (not the original leader)
        self.redpanda.start_node(self.redpanda.get_node_by_id(other_node_id))

        hosts = [
            n.account.hostname for n in self.redpanda.nodes
            if self.redpanda.idx(n) != initial_leader_id
        ]
        admin.wait_stable_configuration("id_allocator",
                                        namespace="kafka_internal",
                                        replication=3,
                                        timeout_s=ELECTION_TIMEOUT * 2,
                                        hosts=hosts)

        # This will be a slow election because priorities have to adjust down
        # (our two live nodes are the lower-priority ones of the three)
        # We have to wait for availability rather than leader state, because
        # leader state may already be reported as the expected leader from
        # stale pre-shutdown metadata.
        wait_until(lambda: self._is_available() is True,
                   timeout_sec=ELECTION_TIMEOUT * 2,
                   backoff_sec=0.5,
                   err_msg=f"Cluster did not become available!")

        new_leader, _ = self._wait_for_leader(
            lambda l: l is not None and l != initial_leader_id,
            timeout=ELECTION_TIMEOUT * 2)

        # 1/3 nodes down, cluster should be available
        self._expect_available()

    @cluster(num_nodes=3)
    def test_leader_restart(self):
        """
        Validate that when a leader node is stopped and restarted,
        leadership remains stable with the new leader elected after
        the original leader stopped.
        """
        initial_leader_id, replicas = self._wait_for_leader()
        initial_leader_node = self.redpanda.get_node_by_id(initial_leader_id)

        self.logger.info(
            f"Stopping initial leader {initial_leader_id} {initial_leader_node.account.hostname}"
        )
        self.redpanda.stop_node(initial_leader_node)

        new_leader_id, _ = self._wait_for_leader(
            lambda l: l is not None and l != initial_leader_id)
        self.logger.info(
            f"New leader is {new_leader_id} {self.redpanda.get_node_by_id(new_leader_id).account.hostname}"
        )

        self.logger.info(
            f"Starting initial leader {initial_leader_id} {initial_leader_node.account.hostname}"
        )
        self.redpanda.start_node(initial_leader_node)

        # Leadership should remain with the new leader, not revert back
        time.sleep(ELECTION_TIMEOUT)
        assert new_leader_id == self._get_leader()[0]

        # On a subsequent restarts of the no-longer-leader, leadership should stay where it is.
        # Do this more than once to avoid missing nondeterministic issues.
        iterations = 1 if self.scale.local else 10
        for i in range(0, iterations):
            self.logger.info(
                f"Restarting original leader node try={i} {initial_leader_id} {initial_leader_node.account.hostname}"
            )
            self.redpanda.stop_node(initial_leader_node)
            time.sleep(ELECTION_TIMEOUT)
            self.redpanda.start_node(initial_leader_node)
            time.sleep(ELECTION_TIMEOUT)
            assert new_leader_id == self._get_leader()[0]

    @cluster(num_nodes=3)
    def test_leadership_transfer(self):
        """
        Validate that when a leadership is transfered to another node, the new leader is able to
        continue serving requests.
        """
        initial_leader_id, replicas = self._wait_for_leader()
        initial_leader_node = self.redpanda.get_node_by_id(initial_leader_id)

        metric_checks = {}
        for n in self.redpanda.nodes:
            metric_checks[self.redpanda.node_id(n)] = MetricCheck(
                self.logger, self.redpanda, n,
                re.compile("vectorized_raft_leadership_changes_total"),
                {'topic': self.topic})

        self.logger.info(f"Transferring leadership  of {self.topic}/0")
        admin = Admin(self.redpanda)
        admin.transfer_leadership_to(topic=self.topic,
                                     namespace="kafka",
                                     partition=0,
                                     target_id=None,
                                     leader_id=initial_leader_id)
        hosts = [n.account.hostname for n in self.redpanda.nodes]
        new_leader_id = admin.await_stable_leader(
            topic=self.topic,
            partition=0,
            hosts=hosts,
            check=lambda l: l is not None and l != initial_leader_id)
        new_leader_node = self.redpanda.get_node_by_id(new_leader_id)
        assert new_leader_node is not None
        self.logger.info(
            f"New leader is {new_leader_id} {new_leader_node.account.hostname}"
        )

        def metrics_updated():
            results = []
            for [id, metric_check] in metric_checks.items():
                # the metric should be updated only on the node that was elected as a leader
                if id == new_leader_id:
                    results.append(
                        metric_check.evaluate([
                            ("vectorized_raft_leadership_changes_total",
                             lambda initial, current: current == initial + 1),
                        ]))
                else:
                    results.append(
                        metric_check.evaluate([
                            ("vectorized_raft_leadership_changes_total",
                             lambda initial, current: current == initial),
                        ]))

            return all(results)

        wait_until(
            metrics_updated, 30, 1,
            "Leadership changes metric should be updated only on the leader")

    @cluster(num_nodes=4)
    @parametrize(acks=1)
    @parametrize(acks=-1)
    def test_leader_transfers_recovery(self, acks):
        """
        Validate that leadership transfers complete successfully
        under acks=1 writes that prompt the leader to frequently
        activate recovery_stm.

        When acks=1, this is a reproducer for
        https://github.com/redpanda-data/redpanda/issues/2580

        When acks=-1, this is a reproducer rfor
        https://github.com/redpanda-data/redpanda/issues/2606
        """

        leader_node_id, replicas = self._wait_for_leader()

        if acks == -1:
            producer = RpkProducer(self._ctx,
                                   self.redpanda,
                                   self.topic,
                                   16384,
                                   sys.maxsize,
                                   acks=acks)
        else:
            # To reproduce acks=1 issue, we need an intermittent producer that
            # waits long enough between messages to let recovery_stm go to sleep
            # waiting for follower_state_change

            # KafProducer is intermittent because it starts a fresh process for
            # each message, whereas RpkProducer writes a continuous stream.
            # TODO: create a test traffic generator that has inter-message
            # delay as an explicit parameter, rather than relying on implementation
            # details of the producer helpers.
            producer = KafProducer(self._ctx, self.redpanda, self.topic)

        producer.start()

        # Pass leadership around in a ring
        self.logger.info(f"Initial leader of {self.topic} is {leader_node_id}")

        transfer_count = 50

        # FIXME: with a transfer count >100, we tend to see
        # reactor stalls and corresponding nondeterministic behaviour/failures.
        # This appears unrelated to the functionality under test, something else
        # is tripping up the cluster when we have so many leadership transfers.
        # https://github.com/redpanda-data/redpanda/issues/2623

        admin = Admin(self.redpanda, retry_codes=[])

        initial_leader_id = leader_node_id
        for n in range(0, transfer_count):
            target_idx = (initial_leader_id + n) % len(self.redpanda.nodes)
            target_node_by_id_id = target_idx + 1

            self._transfer_leadership(admin, "kafka", self.topic,
                                      target_node_by_id_id)

            # Wait til we can see producer progressing, to avoid a situation where
            # we do leadership transfers so quickly that we stall the producer
            # and time out the SSH session to it.  This is generally very
            # quick, but can take as long as it takes a client to time out
            # and refresh metadata.
            output_count = producer.output_line_count
            wait_until(
                lambda: producer.output_line_count > output_count,
                timeout_sec=20,
                # Fast poll because it's local state
                backoff_sec=0.1)

        self.logger.info(f"Completed {transfer_count} transfers successfully")

        # Explicit stop of producer so that we see any errors
        producer.stop()
        producer.wait()
        producer.free()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_follower_isolation(self):
        """
        Simplest HA test.  Stop the leader for our partition.  Validate that
        the cluster remains available afterwards, and that the expected
        peer takes over as the new leader.
        """
        admin = Admin(self.redpanda)
        # Find which node is the leader
        initial_leader_id, replicas = self._wait_for_leader()
        assert initial_leader_id == replicas[0]

        self._expect_available()

        leader_node = self.redpanda.get_node_by_id(initial_leader_id)
        self.logger.info(
            f"Initial leader {initial_leader_id} {leader_node.account.hostname}"
        )

        allocator_info = admin.wait_stable_configuration(
            "id_allocator",
            namespace="kafka_internal",
            replication=3,
            timeout_s=ELECTION_TIMEOUT * 2)

        follower = None
        for node in replicas:
            if node == initial_leader_id:
                continue
            if node == allocator_info.leader:
                continue
            follower = node
            break

        assert follower != None

        with FailureInjector(self.redpanda) as fi:
            # isolate one of the followers
            fi.inject_failure(
                FailureSpec(FailureSpec.FAILURE_ISOLATE,
                            self.redpanda.get_node_by_id(follower)))

            # expect messages to be produced and consumed without a timeout
            connection = self.ping_pong()
            connection.ping_pong(timeout_s=10, retries=10)
            for i in range(0, 127):
                connection.ping_pong()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_id_allocator_leader_isolation(self):
        """
        Isolate id allocator leader. This test validates whether the cluster
        is still available when `kafka_internal/id_allocator` leader has been isolated.
        """
        admin = Admin(self.redpanda)
        self._expect_available()
        # Find which node is the leader for id allocator partition
        admin.wait_stable_configuration(namespace='kafka_internal',
                                        topic='id_allocator',
                                        replication=3)
        initial_leader_id = admin.get_partition_leader(
            namespace='kafka_internal', topic='id_allocator', partition=0)
        leader_node = self.redpanda.get_node_by_id(initial_leader_id)
        self.logger.info(
            f"kafka_internal/id_allocator/0 leader: {initial_leader_id}, node: {leader_node.account.hostname}"
        )

        self._expect_available()

        with FailureInjector(self.redpanda) as fi:
            # isolate id_allocator
            fi.inject_failure(
                FailureSpec(FailureSpec.FAILURE_ISOLATE,
                            self.redpanda.get_node_by_id(initial_leader_id)))

            # expect messages to be produced and consumed without a timeout
            connection = self.ping_pong()
            connection.ping_pong(timeout_s=10, retries=10)
            for i in range(0, 127):
                connection.ping_pong()

    @cluster(num_nodes=3)
    def test_initial_leader_stability(self):
        """
        Redpanda optimistically initializes a partition's leader in the partition
        leader table to its expected leader before any election has occurred.

        On a healthy system, that guess should be accurate and the leadership
        should not flip back to -1 at any stage during startup.

        Reproducer for https://github.com/redpanda-data/redpanda/issues/2546
        """

        leader_node_id, replicas = self._get_leader()

        # Initial leader should not be none, because we populate partition_leader_table
        # with the anticipated leader when creating the topic
        assert leader_node_id is not None

        # Leadership shouhld remain the same across many queries using
        # individually constructed clients:
        #  - Because all nodes have the same initial guess
        #  and
        #  - Because the anticipated leader should always win the first election
        for n in range(0, 20):
            assert self._get_leader()[0] == leader_node_id

    @cluster(num_nodes=3)
    def test_controller_node_isolation(self):
        """
        Isolate controller node, expect cluster to be available
        """
        def controller_available():
            return self.redpanda.controller() is not None

        admin = Admin(self.redpanda)

        # wait for controller
        wait_until(controller_available,
                   timeout_sec=ELECTION_TIMEOUT * 2,
                   backoff_sec=1)

        initial_leader_id, replicas = self._wait_for_leader()
        assert initial_leader_id == replicas[0]
        self._expect_available()

        allocator_info = admin.wait_stable_configuration(
            "id_allocator",
            namespace="kafka_internal",
            replication=3,
            timeout_s=ELECTION_TIMEOUT * 2)

        # isolate controller
        with FailureInjector(self.redpanda) as fi:
            controller_id = self.redpanda.idx(
                self.redpanda.controller().account.hostname)
            fi.inject_failure(
                FailureSpec(FailureSpec.FAILURE_ISOLATE,
                            self.redpanda.controller()))

            if allocator_info.leader == controller_id:
                hosts = [
                    n.account.hostname for n in self.redpanda.nodes
                    if self.redpanda.idx(n) != controller_id
                ]
                admin.await_stable_leader(
                    "id_allocator",
                    namespace="kafka_internal",
                    replication=3,
                    timeout_s=ELECTION_TIMEOUT * 2,
                    hosts=hosts,
                    check=lambda node_id: node_id != controller_id)

        connection = self.ping_pong()
        connection.ping_pong(timeout_s=10, retries=10)
        for i in range(0, 127):
            connection.ping_pong()
