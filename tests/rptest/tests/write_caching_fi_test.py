from ducktape.utils.util import wait_until
from kafka import KafkaAdminClient, TopicPartition
from kafka import errors as kerr
from kafka.protocol.fetch import FetchRequest_v9

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.failure_injector import FailureSpec, make_failure_injector
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.redpanda import RedpandaService
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result


class WriteCachingFailureInjectionTest(RedpandaTest):
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

            # Disable leader balancer to avoid unexpected epoch increments
            # during test.
            enable_leader_balancer=False,
        )

        super().__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf=extra_rp_conf,
        )

        self.topic_name = "test"
        self.topics = [TopicSpec(name=self.topic_name)]

        self.rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=4)
    def test_crash_leader(self):
        """
        This test shouldn't observe any data loss.

        Write caching and `acks=-1` (kgo default) means that produce
        requests are acknowledged once they have been replicated to majority of
        replicas which will form a new quorum after leader failure.
        """

        num_restart_rounds = 3
        num_msg_per_round = 5000

        total_produced = 0

        for round_ix in range(0, num_restart_rounds + 1):
            self.logger.info(f"Round {round_ix}/{num_restart_rounds}")

            if round_ix > 0:
                self._crash_and_restart_leader(wait_for_new_leader=True)

                hwm = next(self.rpk.describe_topic(self.topic)).high_watermark
                if total_produced != hwm:
                    raise Exception(
                        f"Lost messages: {total_produced - hwm} in round {round_ix}"
                    )

            KgoVerifierProducer.oneshot(self.test_context, self.redpanda,
                                        self.topic_name, self.MSG_SIZE,
                                        num_msg_per_round)
            total_produced += num_msg_per_round

    @cluster(num_nodes=4)
    def test_crash_all(self):
        """
        Crash all nodes and restart them. This test should observe data loss
        and validate the liveness of the system.
        """

        num_restart_rounds = 2
        num_msg_per_round = 5000

        total_produced = 0
        total_lost = 0
        prev_hwm = 0
        for round_ix in range(0, num_restart_rounds + 1):
            self.logger.info(f"Round {round_ix}/{num_restart_rounds}")

            if round_ix > 0:
                self._crash_and_restart_all()
                self._wait_for_leader()
                hwm = next(self.rpk.describe_topic(self.topic)).high_watermark
                lost = num_msg_per_round - (hwm - prev_hwm)
                prev_hwm = hwm

                self.logger.info(f"Lost messages: {lost} in round {round_ix}")
                total_lost += lost

            KgoVerifierProducer.oneshot(self.test_context, self.redpanda,
                                        self.topic_name, self.MSG_SIZE,
                                        num_msg_per_round)
            total_produced += num_msg_per_round

        assert total_lost > 0, "No lost messages observed. The test is not valid."

        # Validate that we can still consume all messages, less lost ones,
        # and that no funny things happened inside redpanda.
        final_consumer = KgoVerifierSeqConsumer.oneshot(self.test_context,
                                                        self.redpanda,
                                                        self.topic_name,
                                                        loop=False)
        assert (total_produced -
                total_lost == final_consumer.consumer_status.validator.
                valid_reads), "unexpected number of consumed messages"

    @cluster(num_nodes=4)
    def test_leader_epoch(self):
        MAX_ATTEMPTS = 10

        # We produce in 2 rounds. First round is to ensure that we have a
        # leader and to allow us to get the epoch. Second round is to produce
        # more messages and observe data loss.
        # We need to do this because metadata request might cause a flush.
        num_msg_per_round = 5000
        expected_hwm = 0

        for attempt_ix in range(MAX_ATTEMPTS):

            # Produce a message to ensure that we have a leader and to get the
            # initial leader epoch.
            KgoVerifierProducer.oneshot(self.test_context, self.redpanda,
                                        self.topic_name, self.MSG_SIZE, 1)
            initial_leader_epoch = next(self.rpk.describe_topic(
                self.topic)).leader_epoch

            # The describe_topic call might cause a flush, so we need to produce
            # more messages to observe data loss.
            KgoVerifierProducer.oneshot(self.test_context, self.redpanda,
                                        self.topic_name, self.MSG_SIZE,
                                        num_msg_per_round - 1)

            self._crash_and_restart_all()
            self._wait_for_leader()
            hwm = next(self.rpk.describe_topic(self.topic)).high_watermark
            expected_hwm += num_msg_per_round
            lost = expected_hwm - hwm

            if lost == 0:
                # In debug builds futures reordering causes out-of-order message
                # delivery which triggers raft recovery. Raft recovery fsync
                # writes to disk which prevents data loss.
                # Continue trying until we hit the data loss scenario.
                self.logger.info(
                    f"No data loss observed. Retrying {attempt_ix=}.")
                continue

            self.logger.info(
                f"High watermark after restart: {hwm}. Lost messages: {lost}.")

            # Produce more messages. We expect these to be produced at a
            # higher epoch than before restart.
            for i in range(10):
                self.rpk.produce(self.topic_name, "", f"foo {i}")

            # Fetch from HWM + 1 but with previous leader epoch. This fetch
            # request should get fenced and return an error.
            test_client = KafkaTestAdminClient(self.redpanda)

            response = test_client.fetch(
                TopicPartition(self.topic_name, 0),
                current_leader_epoch=initial_leader_epoch,
                fetch_offset=hwm + 1)
            partition_response = response.topics[0][1][0]
            error_code = partition_response[1]
            assert error_code == FencedLeaderEpoch.errno, \
                f"Expected fenced leader epoch error in fetch but got {partition_response}"

            # Success.
            return

        assert False, "Failed to observe data loss after multiple attempts. " \
            "This test must observe data loss after restart to be able to "   \
            "validate leader epoch handling."

    @cluster(num_nodes=4)
    def test_unavoidable_data_loss(self):
        """
        Test the scenario where data-loss is unavoidable without fsync as
        described in the following article:
        https://redpanda.com/blog/why-fsync-is-needed-for-data-safety-in-kafka-or-non-byzantine-protocols

        We have this test to ensure that leader election can still
        happen/doesn't hang.

        This test led to the discovery of the issue described in
        https://github.com/redpanda-data/redpanda/pull/17498
        """

        # Produce some initial data.
        KgoVerifierProducer.oneshot(self.test_context, self.redpanda,
                                    self.topic_name, self.MSG_SIZE, 5000)

        # Network partition the first node.
        fi = make_failure_injector(self.redpanda)
        fi.inject_failure(
            FailureSpec(FailureSpec.FAILURE_ISOLATE, self.redpanda.nodes[0]))

        # Produce more data that will be replicated only to the second and
        # third node of the cluster.
        KgoVerifierProducer.oneshot(self.test_context, self.redpanda,
                                    self.topic_name, self.MSG_SIZE, 5000)

        # Crash the second node. Since it didn't fsync the data, the data
        # some messages will be locally lost on the next restart.
        self.redpanda.stop_node(self.redpanda.nodes[1], forced=True)

        # Isolate the 3rd node. This is the only node that has all the data.
        fi.inject_failure(
            FailureSpec(FailureSpec.FAILURE_ISOLATE, self.redpanda.nodes[2]))

        # Revive the first 2 nodes and let them form a quorum.
        fi._heal(self.redpanda.nodes[0])
        self.redpanda.start_node(self.redpanda.nodes[1])

        def _hwm():
            return next(self.rpk.describe_topic(self.topic)).high_watermark

        second_quorum_hwm = wait_until_result(_hwm,
                                              timeout_sec=30,
                                              backoff_sec=1,
                                              retry_on_exc=True)

        lost = 10000 - second_quorum_hwm
        assert lost > 0, "This test must observe data loss to be valid."
        self.logger.info(
            f"High watermark after restart: {second_quorum_hwm}. Lost messages: {lost}."
        )

        # Verify that we can still produce messages.
        KgoVerifierProducer.oneshot(self.test_context, self.redpanda,
                                    self.topic_name, self.MSG_SIZE, 5000)

        # Heal the network partition. This should allow the 3rd node to join
        # the cluster.
        fi._heal_all()

        # Kill the first node just to make sure that second and third can form
        # a new quorum.
        self.redpanda.stop_node(self.redpanda.nodes[0], forced=True)

        third_quorum_hwm = wait_until_result(_hwm,
                                             timeout_sec=30,
                                             backoff_sec=1,
                                             retry_on_exc=True)
        # We produced 3 times 5000 messages. We expect the new HWM to be that
        # minus the lost messages.
        expected_hwm = 3 * 5000 - lost
        assert third_quorum_hwm == expected_hwm, "Data loss observed after network partition healing."

        # Verify that we can still produce messages.
        expected_hwm += 5000
        KgoVerifierProducer.oneshot(self.test_context, self.redpanda,
                                    self.topic_name, self.MSG_SIZE, 5000)

        final_hwm = wait_until_result(_hwm,
                                      timeout_sec=30,
                                      backoff_sec=1,
                                      retry_on_exc=True)

        assert final_hwm == expected_hwm

    def teardown(self):
        make_failure_injector(self.redpanda)._heal_all()

    def _crash_and_restart_all(self):
        """
        Crash all nodes and restart them.
        """
        def _crash_and_start_node(n):
            self.redpanda.stop_node(n, forced=True)
            self.redpanda.start_node(n)

        self.redpanda.for_nodes(self.redpanda.nodes, _crash_and_start_node)

    def _crash_and_restart_leader(self, wait_for_new_leader=True):
        """
        Crash the leader of the first partition and restart it. By default, wait
        for a new leader to be elected before returning.

        :param wait_for_new_leader: If True, wait for a new leader to be elected before
            returning.
        """
        admin = Admin(self.redpanda)
        leader_id = admin.get_partition_leader(namespace="kafka",
                                               topic=self.topic_name,
                                               partition=0)
        node = self.redpanda.get_node(leader_id)

        self.logger.debug(f"Restarting leader node: {node.name}")
        self.redpanda.stop_node(node, forced=True)

        def _wait_new_leader():
            new_leader_id = admin.get_partition_leader(namespace="kafka",
                                                       topic=self.topic_name,
                                                       partition=0)
            self.logger.debug(
                f"Currently reported leader: {new_leader_id}; old leader: {leader_id}"
            )
            return new_leader_id != -1 and new_leader_id != leader_id

        if wait_for_new_leader:
            self.logger.debug("Waiting for new leader")
            wait_until(_wait_new_leader,
                       timeout_sec=30,
                       backoff_sec=1,
                       err_msg="New leader not elected")

        self.logger.debug(f"Starting back old leader node: {node.name}")
        self.redpanda.start_node(node)

    def _wait_for_leader(self, *, timeout_sec=30, backoff_sec=3):
        admin = Admin(self.redpanda)
        wait_until(
            lambda: admin.get_partition_leader(namespace="kafka",
                                               topic=self.topic_name,
                                               partition=0) not in [None, -1],
            timeout_sec=timeout_sec,
            backoff_sec=backoff_sec,
            err_msg="Timed out waiting for partition leader",
        )


class FencedLeaderEpoch(kerr.BrokerResponseError):
    errno = 74
    message = 'FENCED_LEADER_EPOCH'
    description = 'The leader epoch in the request is older than the epoch on the broker.'


class KafkaTestAdminClient():
    """
    A wrapper around KafkaAdminClient to facilitate testing write caching via
    using directly the Kafka protocol.
    """
    def __init__(self, redpanda: RedpandaService):
        self._bootstrap_servers = redpanda.brokers()
        self._admin = KafkaAdminClient(
            bootstrap_servers=self._bootstrap_servers)

    def fetch(
        self,
        partition: TopicPartition,
        current_leader_epoch: int = 0,
        fetch_offset: int = 0,
    ):
        # Ensure the client has metadata about the topic.
        f = self._admin._client.add_topic(partition.topic)
        self._admin._wait_for_futures([f])

        # Find the leader for the partition.
        leader_id = self._admin._client.cluster.leader_for_partition(partition)
        if not leader_id or leader_id == -1:
            f = self._admin._client.cluster.request_update()
            self._admin._wait_for_futures([f])
            raise Exception("No leader for partition")

        # v9 is the first version to support KIP-320 fencing so we are gonna
        # use it for these tests.
        request = FetchRequest_v9(
            replica_id=-1,
            max_wait_time=5000,
            min_bytes=1,
            max_bytes=1024 * 1024,
            isolation_level=0,
            session_id=0,
            session_epoch=-1,
            topics=[
                (partition.topic, [(partition.partition, current_leader_epoch,
                                    fetch_offset, 0, 1024 * 1024)]),
            ],
            forgotten_topics_data=[],
        )

        f = self._admin._send_request_to_node(leader_id, request)
        self._admin._wait_for_futures([f])

        response = f.value

        error_type = kerr.for_code(response.error_code)
        if error_type is not kerr.NoError:
            raise error_type("Error in fetch response")

        return response
