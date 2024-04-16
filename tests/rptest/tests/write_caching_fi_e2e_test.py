import collections
import time

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierConsumerGroupConsumer,
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.tests.redpanda_test import RedpandaTest


class WriteCachingFailureInjectionE2ETest(RedpandaTest):
    """
    A set of end-to-end tests that validate the interaction between redpanda
    and kafka clients when write caching is enabled and the cluster is
    subject to various types of failures.

    We validate raft, kafka protocol, and the client behavior/implementation.
    """

    MSG_SIZE = 512

    def __init__(self, test_context):
        extra_rp_conf = dict(
            write_caching_default="true",

            # Make data loss more likely by allowing more data to reside in
            # memory only.
            raft_replicate_batch_window_size=1024,
            raft_replica_max_pending_flush_bytes=10 * 1024 * 1024,
            raft_replica_max_flush_delay_ms=120 * 1000,
            append_chunk_size=1024 * 1024,
            segment_appender_flush_timeout_ms=120 * 1000,
        )

        super().__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf=extra_rp_conf,
        )

        self.topic_name = "test"
        self.topics = [TopicSpec(name=self.topic_name)]

        self.rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=5)
    @matrix(use_transactions=[False, True])
    def test_crash_all(self, use_transactions):
        """
        Introduce crashes to validate liveness of the system end-to-end when
        data-loss events are observed.
        """

        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            loop=False,
            continuous=True,
            tolerate_data_loss=not use_transactions,
        )
        consumer.start()

        num_restart_rounds = 2
        num_msg_per_round = 5000

        # Transactions are slower, so we reduce the number of messages to
        # produce and consume to keep the test duration reasonable.
        if use_transactions:
            num_msg_per_round //= 10

        total_produced = 0
        total_lost = 0
        prev_hwm = 0
        for round_ix in range(0, num_restart_rounds + 1):
            if round_ix > 0:
                self._crash_and_restart_cluster()
                self._wait_for_leader()
                hwm = next(self.rpk.describe_topic(self.topic)).high_watermark
                if use_transactions:
                    # Calculating lost messages based on the watermark when
                    # transactions are involved is challenging. In the happy
                    # path, we can multiply the number of messages produced
                    # by 3: transaction start marker, 1 message, and
                    # 1 transaction end marker.
                    lost = 3 * num_msg_per_round - (hwm - prev_hwm)
                else:
                    lost = num_msg_per_round - (hwm - prev_hwm)
                prev_hwm = hwm

                self.logger.info(f"Lost messages: {lost} in round {round_ix}")
                total_lost += lost

            KgoVerifierProducer.oneshot(self.test_context,
                                        self.redpanda,
                                        self.topic_name,
                                        self.MSG_SIZE,
                                        num_msg_per_round,
                                        use_transactions=use_transactions)
            total_produced += num_msg_per_round

            consumer.wait_total_reads(total_produced,
                                      timeout_sec=60,
                                      backoff_sec=1)

        self.logger.info(
            f"Lost offsets: {consumer.consumer_status.validator.lost_offsets}")

        if use_transactions:
            # When transactions are used not data-loss is expected.
            assert total_lost == 0, \
                f"Unexpected data loss when using transactions. {total_lost} messages lost."
            assert consumer.consumer_status.validator.lost_offsets is None
        else:
            assert total_lost > 0, \
                "No lost messages observed. The test is not valid."
            assert total_lost == consumer.consumer_status.validator.lost_offsets['0'], \
                f"kgo reported lost offset count mismatch: expected {total_lost}, got {consumer.consumer_status.validator.lost_offsets['0']}"

    @cluster(num_nodes=5)
    def test_crash_all_with_consumer_group(self):
        """
        Similar to the regular `test_crash_all_or_leader` test but with a
        consumer group. In this test we will restart the consumer in addition to
        redpanda nodes and verify that the consumer group is able to recover
        from the data loss event if it previously committed offsets that are
        now lost.
        """
        consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size=512,
            readers=1,
            loop=False,
            continuous=True,
            tolerate_data_loss=True,
            group_name="test-group",
        )

        # Ensure the node is clean before starting the test.
        # We will skip cleaning in all subsequent restarts to retain all the logs.
        consumer.clean()

        num_restart_rounds = 2
        num_msg_per_round = 10000

        total_produced = 0
        total_lost = 0
        cumulative_lost_offsets = collections.Counter({})
        prev_hwm = 0
        for round_ix in range(0, num_restart_rounds + 1):
            if round_ix > 0:
                self._crash_and_restart_cluster()
                self._wait_for_leader()
                hwm = next(self.rpk.describe_topic(self.topic)).high_watermark
                lost = num_msg_per_round - (hwm - prev_hwm)
                prev_hwm = hwm

                self.logger.info(f"Lost messages: {lost} in round {round_ix}")
                total_lost += lost

            consumer.start(clean=False)

            # It is important to start the consumer before the producer otherwise
            # metadata requests might trigger a linearizable barrier and flush to disk
            # which wouldn't interfere with our data-loss testing.
            KgoVerifierProducer.oneshot(self.test_context, self.redpanda,
                                        self.topic_name, self.MSG_SIZE,
                                        num_msg_per_round // 2)
            total_produced += num_msg_per_round // 2

            # Wait for the consumer to catch up. This ensures it did join the
            # group and done with the metadata requests which may trigger a
            # linearizable barrier and flush to disk.
            consumer.wait_total_reads(num_msg_per_round // 2,
                                      timeout_sec=120,
                                      backoff_sec=3)

            # Produce another round of messages. Expecting some of them to be
            # lost during restart.
            KgoVerifierProducer.oneshot(self.test_context, self.redpanda,
                                        self.topic_name, self.MSG_SIZE,
                                        num_msg_per_round // 2)
            total_produced += num_msg_per_round // 2

            # Wait for the consumer to catch up.
            consumer.wait_total_reads(num_msg_per_round,
                                      timeout_sec=120,
                                      backoff_sec=3)

            self.logger.info(
                f"Consumer lost offsets: {consumer.consumer_status.validator.lost_offsets}"
            )

            cumulative_lost_offsets.update(
                collections.Counter(
                    consumer.consumer_status.validator.lost_offsets))

            # Wait a bit to make sure the consumer commits the offsets.
            time.sleep(5)
            consumer.stop()

        self.logger.info(
            f"Cumulative consumer lost offsets: {cumulative_lost_offsets}")

        # Validate that we have lost some messages. If we haven't lost any
        # messages, the test is not triggering the behavior we want to expose
        # redpanda to.
        assert total_lost > 0, \
                "No lost messages observed. The test is not valid."
        assert total_lost == cumulative_lost_offsets['0'], \
                f"kgo reported lost offset count mismatch: expected {total_lost}, got {cumulative_lost_offsets['0']}"

    def _crash_and_restart_cluster(self):
        """
        Crash all nodes and restart them.
        """
        def _crash_and_start_node(n):
            self.redpanda.signal_redpanda(n)
            self.redpanda.start_node(n)

        self.redpanda.for_nodes(self.redpanda.nodes, _crash_and_start_node)

    def _wait_for_leader(self, *, timeout_sec=10, backoff_sec=1):
        admin = Admin(self.redpanda)
        wait_until(
            lambda: admin.get_partition_leader(namespace="kafka",
                                               topic=self.topic_name,
                                               partition=0) not in [None, -1],
            timeout_sec=timeout_sec,
            backoff_sec=backoff_sec,
            err_msg="Timed out waiting for partition leader",
        )
