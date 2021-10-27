# Copyright 2020 Vectorized, Inc.
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
import requests
import random

from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.rpk_producer import RpkProducer
from rptest.services.kaf_producer import KafProducer

ELECTION_TIMEOUT = 10


class MetricCheckFailed(Exception):
    def __init__(self, metric, old_value, new_value):
        self.metric = metric
        self.old_value = old_value
        self.new_value = new_value

    def __str__(self):
        return f"MetricCheckFailed<{self.metric} old={self.old_value} new={self.new_value}>"


class MetricCheck(object):
    def __init__(self, logger, redpanda, node, metrics, labels):
        """
        :param redpanda: a RedpandaService
        :param logger: a Logger
        :param node: a ducktape Node
        :param metrics: a list of metric names, or a single compiled regex (use re.compile())
        :param labels: dict, to filter metrics as we capture and check.
        """
        self.redpanda = redpanda
        self.node = node
        self.labels = labels
        self.logger = logger

        self._initial_samples = self._capture(metrics)

    def _capture(self, check_metrics):
        metrics = self.redpanda.metrics(self.node)

        samples = {}
        for family in metrics:
            for sample in family.samples:
                if isinstance(check_metrics, re.Pattern):
                    include = bool(check_metrics.match(sample.name))
                else:
                    include = sample.name in check_metrics

                if not include:
                    continue

                label_mismatch = False
                for k, v in self.labels.items():
                    if sample.labels.get(k, None) != v:
                        label_mismatch = True
                        continue

                if label_mismatch:
                    continue

                if sample.name in samples:
                    raise RuntimeError(
                        f"Labels {self.labels} on {sample.name} not specific enough"
                    )

                self.logger.info(
                    f"  Captured {sample.name}={sample.value} {sample.labels}")
                samples[sample.name] = sample.value

        return samples

    def expect(self, expectations):
        # Gather current values for all the metrics we are going to
        # apply expectations to (this may be a subset of the metrics
        # we originally gathered at construction time).
        samples = self._capture([e[0] for e in expectations])

        error = None
        for (metric, comparator) in expectations:
            try:
                old_value = self._initial_samples[metric]
            except KeyError:
                self.logger.error(
                    f"Missing metrics {metric} on {self.node.account.hostname}.  Have metrics: {list(self._initial_samples.keys())}"
                )
                raise

            new_value = samples[metric]
            ok = comparator(old_value, new_value)
            if not ok:
                error = MetricCheckFailed(metric, old_value, new_value)
                # Log each bad metric, raise the last one as our actual exception below
                self.logger.error(str(error))

        if error:
            raise error


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
                'enable_leader_balancer': False
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

    def _ping_pong(self):
        kc = KafkaCat(self.redpanda)
        rpk = RpkTool(self.redpanda)

        payload = str(random.randint(0, 1000))

        offset = rpk.produce(self.topic, "tkey", payload, timeout=5)
        consumed = kc.consume_one(self.topic, 0, offset)
        self.logger.info(
            f"_ping_pong produced '{payload}' consumed '{consumed}'")
        if consumed['payload'] != payload:
            raise RuntimeError(f"expected '{payload}' got '{consumed}'")

    def _is_available(self):
        try:
            # Should fail
            self._ping_pong()
        except RpkException:
            return False
        else:
            return True

    def _expect_unavailable(self):
        try:
            # Should fail
            self._ping_pong()
        except RpkException:
            self.logger.info("Cluster is unavailable as expected")
        else:
            assert False, "ping_pong should not have worked "

    def _expect_available(self):
        self._ping_pong()
        self.logger.info("Cluster is available as expected")

    @cluster(num_nodes=3)
    def test_one_node_down(self):
        """
        Simplest HA test.  Stop the leader for our partition.  Validate that
        the cluster remains available afterwards, and that the expected
        peer takes over as the new leader.
        """
        # Find which node is the leader
        initial_leader_id, replicas = self._wait_for_leader()
        assert initial_leader_id == replicas[0]

        self._expect_available()

        leader_node = self.redpanda.get_node(initial_leader_id)
        self.logger.info(
            f"Initial leader {initial_leader_id} {leader_node.account.hostname}"
        )

        # Priority mechanism should reliably select next replica in list
        expect_new_leader_id = replicas[1]
        expect_new_leader_node = self.redpanda.get_node(expect_new_leader_id)

        observer_node_id = (set(replicas) -
                            {expect_new_leader_id, initial_leader_id}).pop()
        observer_node = self.redpanda.get_node(observer_node_id)
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

        new_leader, _ = self._wait_for_leader(
            lambda l: l == expect_new_leader_id)
        self.logger.info(f"Leadership moved to {new_leader}")

        self._expect_available()

        # Metric checks are relaxed to tolerate a particular bug where redpanda sometimes
        # goes through multiple elections to select a new leader, even though it should
        # be able to do it in 1.
        # Ref: https://github.com/vectorizedio/redpanda/issues/2444
        if False:
            observer_metrics.expect([
                # 'leadership changes' increments by 1 when observer sees an append_entries
                # from the new leader
                ("vectorized_raft_leadership_changes_total",
                 lambda a, b: b == a + 1),
                ("vectorized_raft_leader_for", lambda a, b: int(b) == 0),
                ("vectorized_raft_received_vote_requests_total",
                 lambda a, b: b == a + 2),
            ])

            new_leader_metrics.expect([
                # 'leadership changes' includes going to candidate, then going to leader, so
                # increments by 2 (vote_stm::vote calls trigger_leadership_notification when
                # we start an election, before the leadership has really changed)
                ("vectorized_raft_leadership_changes_total",
                 lambda a, b: b == a + 2),
                ("vectorized_raft_leader_for", lambda a, b: int(b) == 1),

                # The new leader should see heartbeat errors sending to the now-offline
                # original leader
                ("vectorized_raft_heartbeat_requests_errors_total",
                 lambda a, b: b > a),

                # This node initiated the vote, so it should not have received any votes
                ("vectorized_raft_received_vote_requests_total",
                 lambda a, b: b == a),
            ])
        else:
            # Relaxed checks that don't care how many elections happened, just that
            # >1 happened.  Tolerant of the cluster going through spurious elections even
            # though it should elect a new leader in a single pass.
            observer_metrics.expect([
                ("vectorized_raft_leadership_changes_total",
                 lambda a, b: b > a),
                ("vectorized_raft_leader_for", lambda a, b: int(b) == 0),
                ("vectorized_raft_received_vote_requests_total",
                 lambda a, b: b > a),
            ])

            new_leader_metrics.expect([
                ("vectorized_raft_leadership_changes_total",
                 lambda a, b: b > a),
                ("vectorized_raft_leader_for", lambda a, b: int(b) == 1),
                ("vectorized_raft_heartbeat_requests_errors_total",
                 lambda a, b: b > a),
                ("vectorized_raft_received_vote_requests_total",
                 lambda a, b: b == a),
            ])

    @cluster(num_nodes=3)
    def test_two_nodes_down(self):
        """
        Validate that when two nodes are down, the cluster becomes unavailable, and
        that when one of those nodes is restored, the cluster becomes available again.
        """
        # Find which node is the leader
        initial_leader_id, replicas = self._wait_for_leader()

        self._ping_pong()

        leader_node = self.redpanda.get_node(initial_leader_id)
        other_node_id = (set(replicas) - {initial_leader_id}).pop()
        other_node = self.redpanda.get_node(other_node_id)

        self.logger.info(
            f"Stopping {initial_leader_id} ({leader_node.account.hostname}) and {other_node_id} ({other_node.account.hostname})"
        )
        self.redpanda.stop_node(leader_node)
        self.redpanda.stop_node(other_node)

        # 2/3 nodes down, cluster should be unavailable for acks=-1
        self._expect_unavailable()

        # Bring back one node (not the original leader)
        self.redpanda.start_node(self.redpanda.get_node(other_node_id))

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
        initial_leader_node = self.redpanda.get_node(initial_leader_id)

        self.logger.info(
            f"Stopping initial leader {initial_leader_id} {initial_leader_node.account.hostname}"
        )
        self.redpanda.stop_node(initial_leader_node)

        new_leader_id, _ = self._wait_for_leader(
            lambda l: l is not None and l != initial_leader_id)
        self.logger.info(
            f"New leader is {new_leader_id} {self.redpanda.get_node(new_leader_id).account.hostname}"
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

    @cluster(num_nodes=4)
    @parametrize(acks=1)
    @parametrize(acks=-1)
    def test_leader_transfers_recovery(self, acks):
        """
        Validate that leadership transfers complete successfully
        under acks=1 writes that prompt the leader to frequently
        activate recovery_stm.

        When acks=1, this is a reproducer for
        https://github.com/vectorizedio/redpanda/issues/2580

        When acks=-1, this is a reproducer rfor
        https://github.com/vectorizedio/redpanda/issues/2606
        """

        # Redpanda lies on startup, claims there's a leader before there is one
        # https://github.com/vectorizedio/redpanda/issues/2546
        time.sleep(5)

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

        initial_leader_id = leader_node_id
        for n in range(0, transfer_count):
            target_idx = (initial_leader_id + n) % len(self.redpanda.nodes)
            target_node_id = target_idx + 1

            api_host = self.redpanda.nodes[leader_node_id - 1].account.hostname

            url = "http://{}:9644/v1/partitions/kafka/{}/{}/transfer_leadership?target={}".format(
                api_host, self.topic, 0, target_node_id)
            self.logger.info(f"Starting transfer to {target_node_id}")

            # Leadership transfer is a blocking API endpoint, important to use a timeout
            # to avoid a test hang if something goes hangs on the server side.
            r = requests.post(url, timeout=30)
            if r.status_code not in (200, 500):
                r.raise_for_status()

            self._wait_for_leader(
                lambda l: l is not None and l == target_node_id,
                timeout=ELECTION_TIMEOUT * 2)
            self.logger.info(f"Completed transfer to {target_node_id}")
            leader_node_id = target_node_id

        self.logger.info(f"Completed {transfer_count} transfers successfully")

        # Explicit stop of producer so that we see any errors
        producer.stop()
        producer.wait()
        producer.free()
