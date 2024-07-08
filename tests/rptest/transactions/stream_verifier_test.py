# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.transactions.verifiers.stream_verifier import StreamVerifier
from rptest.tests.redpanda_test import RedpandaTest


class StreamVerifierTest(RedpandaTest):
    # Max time to wait for the cluster to be healthy once more.
    HEALTHY_WAIT_SECONDS = 20 * 60

    # Up to 5 min to stop the node with a lot of topics
    STOP_TIMEOUT = 60 * 5

    def __init__(self, test_context):
        # Save context for future use
        self.test_context = test_context

        # topic names
        self.source_topic = "stream_topic_src"
        self.target_topic = "stream_topic_dst"
        # topic parameters
        self.num_partitions = 1
        self.num_replicas = 3

        extra_rp_conf = {
            "id_allocator_replication": self.num_replicas,
            "default_topic_replications": self.num_replicas,
            "default_topic_partitions": self.num_partitions
        }
        # Init RP cluster with 3 brokers
        super(StreamVerifierTest, self).__init__(num_brokers=3,
                                                 test_context=test_context,
                                                 extra_rp_conf=extra_rp_conf)

        # Calculated speed of producer is ~100k messages per minute on EC2,
        # and 7.2k per minute in docker. Which is ~1500/sec and 120/sec
        # respectively. Amtomic flow processes ~250/sec and ~20/sec messages.
        # Idea is to have atomic processing going while producer is still
        # producing messages. This will slow down single node Stream producer,
        # but will check several incoming requests on RP for the same topic.

        # Additional goal is to have this test running for at least 3 min
        # with constant load, which is different on EC2 and in docker:
        # EC2: atomic timing 25000 / 250 = 100 seconds = ~1.5 min
        # Docker: atomic timing 5000 / 20 = 250 seconds = ~4 min
        if not self.redpanda.dedicated_nodes:
            self.default_message_count = 5000
        else:
            self.default_message_count = 25000

        # Consumer sleep time when EOF
        self.consume_sleep_time_s = 10

    def create_topics(self):
        # Source topics
        self.client().create_topic(
            TopicSpec(name=self.source_topic,
                      partition_count=self.num_partitions,
                      replication_factor=self.num_replicas))

        self.client().create_topic(
            TopicSpec(name=self.target_topic,
                      partition_count=self.num_partitions,
                      replication_factor=self.num_replicas))

    def start_producer(self,
                       wait_msg_count=50,
                       messages_per_sec=0,
                       timeout=30):
        self.verifier.remote_start_produce(self.source_topic,
                                           self.default_message_count,
                                           messages_per_sec=messages_per_sec)
        self.logger.info(f"Waiting for {wait_msg_count} produces messages")
        self.verifier.wait_for_processed_count('produce', wait_msg_count,
                                               timeout)
        self.logger.info(f"Produce action reached {wait_msg_count} messages.")

    def start_atomic(self, wait_msg_count=50, messages_per_sec=0, timeout=30):
        self.verifier.remote_start_atomic(self.source_topic,
                                          self.target_topic,
                                          messages_per_sec=messages_per_sec)
        self.verifier.wait_for_processed_count('atomic', wait_msg_count,
                                               timeout)

        # Milestone check
        self.logger.info(f"Atomic action reached {wait_msg_count} messages. ")

    def run_consumer(self):
        self.verifier.remote_start_verify(self.target_topic)
        self.verifier.remote_wait_action('consume')
        verify_status = self.verifier.remote_stop_verify()
        self.logger.info(
            f"Consume action finished:\n{json.dumps(verify_status, indent=2)}")

    def verify(self):
        produce_status = self.verifier.get_produce_status()
        atomic_status = self.verifier.get_atomic_status()
        verify_status = self.verifier.get_verify_status()
        produced_count = produce_status['processed_messages']
        atomic_count = atomic_status['processed_messages']
        verify_count = verify_status['processed_messages']

        assert produced_count == atomic_count, \
            "Produced/Atomic message count mismatch: " \
            f"{produced_count}/{atomic_count}"

        assert atomic_count == verify_count, \
            "Atomic/Consumed message count mismatch: " \
            f"{atomic_count}/{verify_count}"

        errors = "\n".join(verify_status['errors'])
        assert len(verify_status['errors']) < 1, \
            f"Consume action has validation errors:\n{errors}"

    @cluster(num_nodes=4)
    def test_simple_produce_consume_txn_with_add_node(self):
        self.verifier = StreamVerifier(self.test_context, self.redpanda)
        self.verifier.start()

        self.create_topics()

        # Consumer thread will work faster than any producer
        # If we start to consume when produce is not finished yet
        # consumer will hit EOF earlier than produce finishes
        # sleep mode will wait for 'consume_sleep_time_s'
        # and check if new messages appeared
        self.verifier.update_service_config({
            "consume_sleep_time_s": self.consume_sleep_time_s,
            "consume_stop_criteria": "sleep",
            "msg_per_txn": 1
        })

        # Produce
        self.logger.info("Starting producer")
        # Start and wait for 2 second worth of Atomic action messages
        self.start_producer(wait_msg_count=500, timeout=60)

        # Atomic
        self.logger.info("Starting to atomic consume/produce")
        # Start and make sure messages coming through
        self.start_atomic(wait_msg_count=250, messages_per_sec=10, timeout=120)

        # Milestone check
        self.logger.info("Waiting for produce to finish")
        self.verifier.remote_wait_action('produce')

        # Make sure that producer is stopped and get the status
        produce_status = self.verifier.remote_stop_produce()
        self.logger.info(
            f"Produce action finished:\n{json.dumps(produce_status, indent=2)}"
        )

        # Wait for atomic action to finish and get the status
        self.verifier.remote_wait_action('atomic')
        atomic_status = self.verifier.remote_stop_atomic()
        self.logger.info(
            f"Atomic action finished:\n{json.dumps(atomic_status, indent=2)}")

        # Consume and verify
        self.run_consumer()
        self.verify()
