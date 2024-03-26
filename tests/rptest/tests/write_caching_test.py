# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# TODO(nv): update kgo infrastructure to support eager status polling from
#   tests rather than relying on the periodic status polling
# TODO(nv): figure out if we can put a tight bound on the number of messages lost
# TODO(nv): Figure out how to test with cloud storage.

import random
from enum import Enum

from ducktape.mark import ignore, matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.failure_injector import FailureSpec, make_failure_injector
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.tests.redpanda_test import RedpandaTest


# no StrEnum support in test python version
class WriteCachingMode(str, Enum):
    ON = "on"
    OFF = "off"
    DISABLED = "disabled"

    def __bool__(self):
        return self.value == self.ON

    def __str__(self):
        return self.value


class WriteCachingPropertiesTest(RedpandaTest):
    WRITE_CACHING_PROP = "write.caching"
    FLUSH_MS_PROP = "flush.ms"
    FLUSH_BYTES_PROP = "flush.bytes"

    WRITE_CACHING_DEBUG_KEY = "write_caching_enabled"
    FLUSH_MS_DEBUG_KEY = "flush_ms"
    FLUSH_BYTES_DEBUG_KEY = "flush_bytes"

    def __init__(self, test_context):
        super(WriteCachingPropertiesTest,
              self).__init__(test_context=test_context, num_brokers=3)
        self.topic_name = "test"
        self.topics = [TopicSpec(name=self.topic_name)]

    def configs_converged(self, write_caching: WriteCachingMode, flush_ms: int,
                          flush_bytes: int):
        """Checks that the desired write caching configuration is set in topic & partition (raft) states"""
        assert self.rpk
        assert self.admin

        configs = self.rpk.describe_topic_configs(self.topic_name)
        assert self.WRITE_CACHING_PROP in configs.keys(), configs
        assert self.FLUSH_MS_PROP in configs.keys(), configs
        assert self.FLUSH_BYTES_PROP in configs.keys(), configs

        properties_check = configs[self.WRITE_CACHING_PROP][0] == str(
            write_caching) and configs[self.FLUSH_MS_PROP][0] == str(
                flush_ms) and configs[self.FLUSH_BYTES_PROP][0] == str(
                    flush_bytes)

        if not properties_check:
            return False

        partition_state = self.admin.get_partition_state(
            "kafka", self.topic_name, 0)
        replicas = partition_state["replicas"]
        assert len(replicas) == 3

        def validate_flush_ms(val: int) -> bool:
            # account for jitter
            return flush_ms - 5 <= val <= flush_ms + 5

        for replica in replicas:
            raft_state = replica["raft_state"]
            assert self.WRITE_CACHING_DEBUG_KEY in raft_state.keys(
            ), raft_state
            assert self.FLUSH_MS_DEBUG_KEY in raft_state.keys(), raft_state
            assert self.FLUSH_BYTES_DEBUG_KEY in raft_state.keys(), raft_state

            replica_check = raft_state[self.WRITE_CACHING_DEBUG_KEY] == bool(
                write_caching) and validate_flush_ms(
                    raft_state[self.FLUSH_MS_DEBUG_KEY]) and raft_state[
                        self.FLUSH_BYTES_DEBUG_KEY] == flush_bytes

            if not replica_check:
                return False

        return True

    def validate_topic_configs(self, write_caching: WriteCachingMode,
                               flush_ms: int, flush_bytes: int):
        self.redpanda.wait_until(
            lambda: self.configs_converged(write_caching, flush_ms, flush_bytes
                                           ),
            timeout_sec=30,
            backoff_sec=2,
            err_msg="Write caching configuration failed to converge")

    def set_cluster_config(self, key: str, value):
        self.rpk.cluster_config_set(key, value)

    def set_topic_properties(self, key: str, value):
        self.rpk.alter_topic_config(self.topic_name, key, value)

    @cluster(num_nodes=3)
    def test_properties(self):
        self.admin = self.redpanda._admin
        self.rpk = RpkTool(self.redpanda)

        write_caching_conf = "write_caching"
        flush_ms_conf = "raft_replica_max_flush_delay_ms"
        flush_bytes_conf = "raft_replica_max_pending_flush_bytes"

        # Validate cluster defaults
        self.validate_topic_configs(WriteCachingMode.OFF, 100, 262144)

        # Changing cluster level configs
        self.set_cluster_config(write_caching_conf, "on")
        self.validate_topic_configs(WriteCachingMode.ON, 100, 262144)
        self.set_cluster_config(flush_ms_conf, 200)
        self.validate_topic_configs(WriteCachingMode.ON, 200, 262144)
        self.set_cluster_config(flush_bytes_conf, 32768)
        self.validate_topic_configs(WriteCachingMode.ON, 200, 32768)

        # Turn off write caching at topic level
        self.set_topic_properties(self.WRITE_CACHING_PROP, "off")
        self.validate_topic_configs(WriteCachingMode.OFF, 200, 32768)

        # Turn off write caching at cluster level but enable at topic level
        # topic properties take precedence
        self.set_cluster_config(write_caching_conf, "off")
        self.set_topic_properties(self.WRITE_CACHING_PROP, "on")
        self.validate_topic_configs(WriteCachingMode.ON, 200, 32768)

        # Kill switch test, disable write caching feature globally,
        # should override topic level property
        self.set_cluster_config(write_caching_conf, "disabled")
        self.validate_topic_configs(WriteCachingMode.DISABLED, 200, 32768)

        # Try to update the topic property now, should throw an error
        try:
            self.set_topic_properties(self.WRITE_CACHING_PROP, "on")
            assert False, "No exception thrown when updating topic propertes in disabled mode."
        except RpkException as e:
            assert "INVALID_CONFIG" in str(e)

        # Enable again
        self.set_cluster_config(write_caching_conf, "on")
        self.set_topic_properties(self.WRITE_CACHING_PROP, "on")
        self.validate_topic_configs(WriteCachingMode.ON, 200, 32768)


class WriteCachingWithFailures(RedpandaTest):
    """
    A set of tests that validate the behavior of Redpanda when write caching is
    enabled and the cluster is subjected to various types of failures.

    We test positive scenarios (data loss observed) and negative scenarios
    (no data loss observed). To have certainty that the test is valid, we
    design the tests to mirror each other as closely as possible. The only
    difference is the type of failure introduced.
    """
    def __init__(self, test_context):
        extra_rp_conf = dict(
            write_caching="on",

            # Make data loss more likely by allowing more data to reside in
            # memory.
            raft_replicate_batch_window_size=1024,
            raft_replica_max_pending_flush_bytes=10 * 1024 * 1024,
            raft_replica_max_flush_delay_ms=30 * 1000,
            append_chunk_size=1024 * 1024,
        )

        super().__init__(
            test_context=test_context,
            num_brokers=3,
            extra_rp_conf=extra_rp_conf,
        )

        self.topic_name = "test"
        self.topics = [TopicSpec(name=self.topic_name)]

    # This test shouldn't observe data loss because we are replicating to
    # majority of replicas and the leader returns after the election.
    @cluster(num_nodes=5)
    def test_crash_leader(self):
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            loop=False,
            continuous=True,
            tolerate_data_loss=False,
        )
        consumer.start()

        num_restarts = 5
        num_acked_per_iter = 1000
        num_consumed_per_iter = 1000

        msg_size = 512
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size,
            1_000_000_000,
            batch_max_bytes=2 * msg_size,
            max_buffered_records=1,
            tolerate_data_loss=False,
        )
        producer.start()

        acks_count = 0
        consume_count = 0

        # Introduce crashes to trigger data loss but verify that topic is still
        # functioning correctly:
        #   a) data is still being produced,
        #   b) data is still being consumed.
        for i in range(0, num_restarts):
            acks_count = producer._status.acked

            # Validate producer health.
            self.logger.debug(
                f"Waiting producer acked to reach {acks_count + num_acked_per_iter} "
                f"messages in iteration {i}/{num_restarts}")
            producer.wait_for_acks(acks_count + num_acked_per_iter,
                                   timeout_sec=60,
                                   backoff_sec=1)
            acks_count = producer._status.acked

            # Validate consumer health.
            self.logger.debug(
                f"Waiting for consumer to reach {consume_count + num_consumed_per_iter} messages"
            )
            wait_until(lambda: consumer._status.validator.total_reads >=
                       consume_count + num_consumed_per_iter,
                       timeout_sec=60,
                       backoff_sec=1)
            consume_count = consumer._status.validator.total_reads

            # Introduce failure.
            admin = Admin(self.redpanda)
            leader_id = admin.get_partition_leader(namespace="kafka",
                                                   topic=self.topic_name,
                                                   partition=0)
            node = self.redpanda.get_node(leader_id)

            self.logger.debug(f"Restarting leader node: {node.name}")
            self.redpanda.signal_redpanda(node)

            def wait_new_leader():
                new_leader_id = admin.get_partition_leader(
                    namespace="kafka", topic=self.topic_name, partition=0)
                return new_leader_id != -1 and new_leader_id != leader_id

            self.logger.debug("Waiting for new leader")
            wait_until(wait_new_leader,
                       timeout_sec=30,
                       backoff_sec=1,
                       err_msg="New leader not elected")

            self.logger.debug(f"Starting back old leader node: {node.name}")
            self.redpanda.start_node(node)

        producer.stop()

        # Stop consumer.
        consumer.wait()
        consumer.stop()

        self.logger.info(
            f"Lost offsets: {consumer._status.validator.lost_offsets}")

        # This test doesn't tolerate data loss.
        assert consumer._status.validator.lost_offsets is None

    @cluster(num_nodes=5)
    @matrix(use_transactions=[False, True])
    def test_crash_all_or_leader(self, use_transactions):
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            loop=False,
            continuous=True,
            tolerate_data_loss=not use_transactions,
        )
        consumer.start()

        num_restarts = 5
        num_acked_per_iter = 5000
        num_consumed_per_iter = 5000

        if self.debug_mode:
            num_acked_per_iter //= 10
            num_consumed_per_iter //= 10

        if use_transactions:
            num_acked_per_iter //= 3
            num_consumed_per_iter //= 3

        msg_size = 512
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size,
            1_000_000_000,
            batch_max_bytes=2 * msg_size,
            max_buffered_records=1,
            tolerate_data_loss=not use_transactions,
            use_transactions=use_transactions,
        )
        producer.start()

        acks_count = 0
        consume_count = 0

        # Introduce crashes to trigger data loss but verify that topic is still
        # functioning correctly:
        #   a) data is still being produced,
        #   b) data is still being consumed.
        for i in range(0, num_restarts):
            acks_count = producer._status.acked

            # Validate producer health.
            self.logger.debug(
                f"Waiting producer acked to reach {acks_count + num_acked_per_iter} "
                f"messages in iteration {i}/{num_restarts}")
            producer.wait_for_acks(
                acks_count + num_acked_per_iter,
                timeout_sec=120,
                backoff_sec=3,
            )
            acks_count = producer._status.acked

            # Validate consumer health.
            self.logger.debug(
                f"Waiting for consumer to reach {consume_count + num_consumed_per_iter} messages"
            )
            wait_until(lambda: consumer._status.validator.total_reads >=
                       consume_count + num_consumed_per_iter,
                       timeout_sec=120,
                       backoff_sec=3)
            consume_count = consumer._status.validator.total_reads

            # Introduce failure.
            def kill_and_start(n):
                self.redpanda.signal_redpanda(n)
                self.redpanda.start_node(n)

            # Sometime kill all nodes, sometime kill just the leader to verify
            # that re-election is possible.
            if random.choice([True, False]):
                self.logger.info("Restarting all redpanda nodes")
                self.redpanda.for_nodes(self.redpanda.nodes, kill_and_start)
            else:
                admin = Admin(self.redpanda)
                leader_id = admin.get_partition_leader(namespace="kafka",
                                                       topic=self.topic_name,
                                                       partition=0)
                node = self.redpanda.get_node(leader_id)

                self.logger.info(f"Restarting leader node: {node.name}")
                self.redpanda.signal_redpanda(node)

                def wait_new_leader():
                    new_leader_id = admin.get_partition_leader(
                        namespace="kafka", topic=self.topic_name, partition=0)
                    self.logger.debug(
                        f"Currently reported leader: {new_leader_id}")
                    return new_leader_id != -1 and new_leader_id != leader_id

                self.logger.debug("Waiting for new leader")
                wait_until(wait_new_leader,
                           timeout_sec=30,
                           backoff_sec=3,
                           err_msg="New leader not elected")

                self.logger.debug(
                    f"Starting back old leader node: {node.name}")
                self.redpanda.start_node(node)

        producer.stop()

        # Stop consumer.
        consumer.wait()
        consumer.stop()

        self.logger.info(
            f"Lost offsets: {consumer._status.validator.lost_offsets}")

        if use_transactions:
            # When using transactions not data loss is tolerated.
            assert consumer._status.validator.lost_offsets is None
        else:
            # Validate that we have lost some messages. If we haven't lost any
            # messages, the test is not triggering the behavior we want to expose
            # redpanda to.
            assert consumer._status.validator.lost_offsets['0'] > 0, \
                "No lost messages observed. The test is not valid."

    @cluster(num_nodes=5)
    def test_crash_all_or_leader_with_consumer_group(self):
        """
        Similar to the regular `test_crash_all_or_leader` test but with a
        consumer group. In this test we will restart the consumer in addition to
        redpanda nodes and verify that the consumer group is able to recover
        from the data loss even if it previously committed offsets that are
        now lost.
        """
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            loop=False,
            continuous=True,
            tolerate_data_loss=True,
        )
        consumer.start()

        num_restarts = 5
        num_acked_per_iter = 1000

        num_consumed_per_iter = 10000
        if self.debug_mode:
            num_consumed_per_iter = 1000

        msg_size = 512
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size,
            1_000_000_000,
            batch_max_bytes=2 * msg_size,
            max_buffered_records=1,
            tolerate_data_loss=True,
        )
        producer.start()

        acks_count = 0
        consume_count = 0

        # Introduce crashes to trigger data loss but verify that topic is still
        # functioning correctly:
        #   a) data is still being produced,
        #   b) data is still being consumed.
        for i in range(0, num_restarts):
            acks_count = producer._status.acked

            # Validate producer health.
            self.logger.debug(
                f"Waiting producer acked to reach {acks_count + num_acked_per_iter} "
                f"messages in iteration {i}/{num_restarts}")
            producer.wait_for_acks(acks_count + num_acked_per_iter,
                                   timeout_sec=60,
                                   backoff_sec=1)
            acks_count = producer._status.acked

            # Validate consumer health.
            self.logger.debug(
                f"Waiting for consumer to reach {consume_count + num_consumed_per_iter} messages"
            )
            wait_until(lambda: consumer._status.validator.total_reads >=
                       consume_count + num_consumed_per_iter,
                       timeout_sec=60,
                       backoff_sec=1)
            consume_count = consumer._status.validator.total_reads

            # Introduce failure.
            def kill_and_start(n):
                self.redpanda.signal_redpanda(n)
                self.redpanda.start_node(n)

            # Sometime kill all nodes, sometime kill just the leader to verify
            # that re-election is possible.
            if random.choice([True, False]):
                self.logger.debug("Restarting all redpanda nodes")
                self.redpanda.for_nodes(self.redpanda.nodes, kill_and_start)
            else:
                admin = Admin(self.redpanda)
                leader_id = admin.get_partition_leader(namespace="kafka",
                                                       topic=self.topic_name,
                                                       partition=0)
                node = self.redpanda.get_node(leader_id)

                self.logger.debug(f"Restarting leader node: {node.name}")
                self.redpanda.signal_redpanda(node)

                def wait_new_leader():
                    new_leader_id = admin.get_partition_leader(
                        namespace="kafka", topic=self.topic_name, partition=0)
                    self.logger.debug(
                        f"Currently reported leader: {new_leader_id}")
                    return new_leader_id != -1 and new_leader_id != leader_id

                self.logger.debug("Waiting for new leader")
                wait_until(wait_new_leader,
                           timeout_sec=30,
                           backoff_sec=3,
                           err_msg="New leader not elected")

                self.logger.debug(
                    f"Starting back old leader node: {node.name}")
                self.redpanda.start_node(node)

        producer.stop()

        # Stop consumer.
        consumer.wait()
        consumer.stop()

        self.logger.info(
            f"Lost offsets: {consumer._status.validator.lost_offsets}")

        # Validate that we have lost some messages. If we haven't lost any
        # messages, the test is not triggering the behavior we want to expose
        # redpanda to.
        assert consumer._status.validator.lost_offsets['0'] > 0, \
            "No lost messages observed. The test is not valid."

    # Test the scenario where data-loss is unavoidable. We do this to ensure
    # that leader election can still happen/doesn't hang. This scenario is
    # described in the following article:
    # https://redpanda.com/blog/why-fsync-is-needed-for-data-safety-in-kafka-or-non-byzantine-protocols
    @ignore  # Producer gets stuck after a while. Supposedly new leader can't be elected.
    @cluster(num_nodes=5)
    def test_unavoidable_data_loss(self):
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            loop=False,
            continuous=True,
            tolerate_data_loss=True,
        )
        consumer.start()

        num_restarts = 3
        num_acked_per_iter = 1000
        num_consumed_per_iter = 1000

        msg_size = 512
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.topic_name,
            msg_size,
            1_000_000_000,
            batch_max_bytes=2 * msg_size,
            max_buffered_records=1,
            tolerate_data_loss=True,
        )
        producer.start()

        acks_count = 0
        consume_count = 0

        fi = make_failure_injector(self.redpanda)

        # Shuffle the nodes.
        # node_order = self.redpanda.nodes

        # Introduce crashes to trigger data loss but verify that topic is still
        # functioning correctly:
        #   a) data is still being produced,
        #   b) data is still being consumed.
        for i in range(0, num_restarts):
            self.logger.info(f"Round {i+1}/{num_restarts}")

            acks_count = producer._status.acked

            # Network partition the first node.
            fi.inject_failure(
                FailureSpec(FailureSpec.FAILURE_ISOLATE,
                            self.redpanda.nodes[0]))

            # Validate producer health.
            self.logger.debug(
                f"Waiting producer acked to reach {acks_count + num_acked_per_iter} "
                f"messages in iteration {i+1}/{num_restarts}")
            producer.wait_for_acks(acks_count + num_acked_per_iter,
                                   timeout_sec=60,
                                   backoff_sec=1)
            acks_count = producer._status.acked

            # Validate consumer health.
            self.logger.debug(
                f"Waiting for consumer to reach {consume_count + num_consumed_per_iter} messages"
            )
            wait_until(lambda: consumer._status.validator.total_reads >=
                       consume_count + num_consumed_per_iter,
                       timeout_sec=120,
                       backoff_sec=1)
            consume_count = consumer._status.validator.total_reads

            # Introduce failure.
            self.redpanda.signal_redpanda(self.redpanda.nodes[1])

            # Isolate the 3rd node.
            fi.inject_failure(
                FailureSpec(FailureSpec.FAILURE_ISOLATE,
                            self.redpanda.nodes[2]))

            # Let the first 2 nodes form a quorum.
            fi._heal(self.redpanda.nodes[0])
            self.redpanda.start_node(self.redpanda.nodes[1])

            admin = Admin(self.redpanda)

            def wait_new_leader():
                new_leader_id = admin.get_partition_leader(
                    namespace="kafka", topic=self.topic_name, partition=0)
                return new_leader_id != -1

            self.logger.debug("Waiting for new leader")
            wait_until(wait_new_leader,
                       timeout_sec=30,
                       backoff_sec=1,
                       err_msg="New leader not elected")

            fi._heal_all()

        producer.stop()

        self.logger.info(
            f"Lost offsets: {consumer._status.validator.lost_offsets}")

        # Stop consumer.
        consumer.wait()
        consumer.stop()

        # Validate that we have lost some messages. If we haven't lost any
        # messages, the test is not triggering the behavior we want to expose
        # redpanda to.
        assert consumer._status.validator.lost_offsets['0'] > 0, \
            "No lost messages observed. The test is not valid."

    def teardown(self):
        make_failure_injector(self.redpanda)._heal_all()
