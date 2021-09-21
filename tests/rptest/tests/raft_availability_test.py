# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import json
import re
import random

from ducktape.utils.util import wait_until

from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import redpanda_cluster
from rptest.services.kaf_producer import KafProducer
from rptest.services.producer_swarm import ProducerSwarm

ELECTION_TIMEOUT = 10


class MetricCheckFailed(Exception):
    def __init__(self, metric, old_value, new_value):
        self.metric = metric
        self.old_value = old_value
        self.new_value = new_value

    def __str__(self):
        return f"MetricCheckFailed<{self.metric} old={self.old_value} new={self.new_value}>"


def label_match(candidate, conditions):
    for k, v in conditions.items():
        if candidate.get(k, None) != v:
            return False

    return True


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

                if not label_match(sample.labels, self.labels):
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
            self.logger.debug(
                f"MetricCheck.expect {metric} old={old_value} new={new_value} ok={ok}"
            )
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

    def __init__(self, *args, **kwargs):
        super(RaftAvailabilityTest, self).__init__(
            *args,
            extra_rp_conf={
                # Disable leader balancer to enable testing
                # leadership stability.
                'enable_leader_balancer': False,
                # 100MB instead of default 1GB segments in order
                # to make it easier for loaded workloads to encounter
                # segment end/begin behaviours.
                'log_segment_size': 100 * 1024 * 1024,
            },
            **kwargs)

    def _get_leader(self):
        """
        :returns: 2 tuple of (leader, [replica ids])
        """
        kc = KafkaCat(self.redpanda)

        topic_meta = None
        all_metadata = kc.metadata()
        for t in all_metadata['topics']:
            if t['topic'] != self.topic:
                self.logger.warning(f"Unexpected topic {t['topic']}")
            else:
                topic_meta = t
                break

        if topic_meta is None:
            self.logger.error(f"Topic {self.topic} not found!")
            self.logger.error(
                f"KafkaCat metadata: {json.dumps(all_metadata,indent=2)}")
            assert topic_meta is not None

        partition = topic_meta['partitions'][0]
        leader_id = partition['leader']
        replicas = [p['id'] for p in partition['replicas']]
        if leader_id == -1:
            return None, replicas
        else:
            return leader_id, replicas

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

    def setUp(self):
        # Disable default RedpandaTest startup so that `redpanda_cluster`
        # can change resource settings before starting cluster.
        pass

    @redpanda_cluster()
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

    @redpanda_cluster()
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

    @redpanda_cluster()
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

    def _get_metric(self, node, metric, labels):
        for family in self.redpanda.metrics(node):
            for sample in family.samples:
                if sample.name == metric and label_match(
                        sample.labels, labels):
                    return sample.value

        raise RuntimeError(f"Metric '{metric}' with labels {labels} not found")

    def _await_catchup(self, leader, follower, metric, labels):
        """
        Given a named metric and two nodes, wait for the metric
        to equalize across the two nodes.  Require that the follower's
        metric increases continuously while we wait (i.e. early fail
        on hang).
        """

        last_follower_v = None
        while True:
            # Read leader first, so that if we're chasing our tail then
            # we'll have a chance to see the follower >= the leader
            leader_v = self._get_metric(leader, metric, labels)
            follower_v = self._get_metric(follower, metric, labels)

            self.logger.debug(
                f"await_catchup[{metric}] follower={follower_v} leader={leader_v}"
            )

            if follower_v >= leader_v:
                self.logger.info(
                    f"Caught up {metric} follower={follower_v} leader={leader_v}"
                )
                return
            else:
                if last_follower_v is not None and follower_v == last_follower_v:
                    raise RuntimeError(
                        f"Follower {follower.account.hostname} stuck at {metric}={follower_v}!"
                    )
                else:
                    last_follower_v = follower_v
                    time.sleep(10)

    @redpanda_cluster(num_nodes=4)
    def test_follower_recovery(self):
        """
        Validate that when a follower stops for a while while partition
        is under load, it catches up again when it restarts.

        As well as being a general test of recovery, this specifically
        regression tests https://github.com/vectorizedio/redpanda/issues/2101
        (that particular issue requires memory-constrained nodes plus
         substantial load, plus crossing segment barriers)

        """
        initial_leader_id, replicas = self._wait_for_leader()
        initial_leader_node = self.redpanda.get_node(initial_leader_id)
        follower_id = (set(replicas) - {initial_leader_id}).pop()
        follower_node = self.redpanda.get_node(follower_id)

        # Validate catching-up code on an idle system: we should be immediately
        # caught up
        self._await_catchup(initial_leader_node, follower_node,
                            "vectorized_cluster_partition_high_watermark",
                            {"topic": self.topic})

        # FIXME KafProducer is fussy about metadata being all up to date before
        # it starts to produce, so let the cluster settle down.
        #time.sleep(5)
        #producer = KafProducer(self.test_context, self.redpanda, self.topic)

        self.logger.info("Waiting for some pre-restart traffic to accumulate")
        leader_metrics = MetricCheck(
            self.logger, self.redpanda, initial_leader_node,
            ["vectorized_storage_log_log_segments_created_total"],
            {'topic': self.topic})

        producer = ProducerSwarm(self.test_context, self.redpanda, self.topic,
                                 100, 1000)
        producer.start()
        time.sleep(30)
        # Expect that this should have been enough time to roll over at least one
        # log segment
        leader_metrics.expect([
            ('vectorized_storage_log_log_segments_created_total',
             lambda a, b: b > a)
        ])

        self.logger.info("Stopping follower")
        offset_before_outage = self._get_metric(
            initial_leader_node, "vectorized_cluster_partition_high_watermark",
            {"topic": self.topic})
        self.redpanda.stop_node(follower_node)
        time.sleep(30)
        offset_after_outage = self._get_metric(
            initial_leader_node, "vectorized_cluster_partition_high_watermark",
            {"topic": self.topic})

        self.logger.info(
            f"Follower missed range approx {offset_before_outage}-{offset_after_outage} during simulated outage"
        )

        self.logger.info("Restarting follower")
        self.redpanda.start_node(follower_node)
        follower_offset_after_restart = self._get_metric(
            follower_node, "vectorized_cluster_partition_high_watermark",
            {"topic": self.topic})
        self.logger.info(
            f"Follower offset after restart is {follower_offset_after_restart}"
        )

        self.logger.info(
            "Leaving background producer running for a while after node restart..."
        )
        time.sleep(30)
        self.logger.info("Stopping background producer")
        producer.stop_all()

        # Wait for this metric to be equal between the two nodes
        # While waiting, require that it increases.
        self.logger.info("Waiting for recovery to complete")
        self._await_catchup(initial_leader_node, follower_node,
                            "vectorized_cluster_partition_high_watermark",
                            {"topic": self.topic})

    @redpanda_cluster(num_nodes=5)
    def test_shutdown_under_load(self):

        initial_leader_id, replicas = self._wait_for_leader()
        initial_leader_node = self.redpanda.get_node(initial_leader_id)
        follower_id = (set(replicas) - {initial_leader_id}).pop()
        follower_node = self.redpanda.get_node(follower_id)

        producer = ProducerSwarm(self.test_context, self.redpanda, self.topic)
        producer.start()

        # Default shutdown timeouts are long to avoid test noise: for this
        # test we really want to ensure *prompt* shutdown and investigate
        # if something is taking longer than a few seconds
        stop_timeout = 5

        wait_until(lambda: self._get_metric(
            initial_leader_node, "vectorized_cluster_partition_high_watermark",
            {"topic": self.topic}) > 0.0,
                   timeout_sec=30,
                   backoff_sec=0.5,
                   err_msg="Topic high watermark didn't get above zero")

        # A follower: check that it cleanly shuts down and restarts
        t1 = time.time()
        self.redpanda.stop_node(follower_node, timeout_sec=stop_timeout)
        self.logger.info(
            f"Stopped node {follower_node.account.hostname} in {time.time() - t1}"
        )

        time.sleep(10)  # Wait for some recovery backlog to build
        self.redpanda.start_node(follower_node)
        time.sleep(10)  # Inject some post-restart messages

        # Give the follower a chance to catch up
        producer.stop_all()

        self._await_catchup(initial_leader_node, follower_node,
                            "vectorized_cluster_partition_high_watermark",
                            {"topic": self.topic})

        # FIXME somehow release the node used by the previous one, so that the
        # test doesn't have to demand so many nodes
        producer = ProducerSwarm(self.test_context, self.redpanda, self.topic)
        producer.start()

        # A leader: check that it cleanly & promptly shuts down and restarts
        t1 = time.time()
        self.redpanda.stop_node(initial_leader_node, timeout_sec=stop_timeout)
        self.logger.info(
            f"Stopped node {initial_leader_node.account.hostname} in {time.time() - t1}"
        )

        # Leadership should pass to the highest priority follower
        expect_new_leader_id = replicas[1]
        new_leader_id, _ = self._wait_for_leader(
            lambda l: l == expect_new_leader_id)
        new_leader_node = self.redpanda.get_node(new_leader_id)
        wait_until(lambda: self._get_metric(
            new_leader_node, "vectorized_cluster_partition_high_watermark",
            {"topic": self.topic}) > 0.0,
                   timeout_sec=30,
                   backoff_sec=0.5,
                   err_msg="Topic high watermark didn't get above zero")

        time.sleep(10)  # Wait for some recovery backlog to build
        self.redpanda.start_node(initial_leader_node)
        time.sleep(10)  # Inject some post-restart messages

        producer.stop_all()

        self._await_catchup(new_leader_node, initial_leader_node,
                            "vectorized_cluster_partition_high_watermark",
                            {"topic": self.topic})

        producer.stop()
