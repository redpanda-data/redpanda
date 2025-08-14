# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from ducktape.tests.test import TestContext

from rptest.services.direct_consumer_verifier import (
    DirectConsumerVerifier, CreateDirectConsumerRequest, BrokerAddress,
    DirectConsumerConfiguration, OffsetResetPolicy, IsolationLevel,
    AssignPartitionsRequest, TopicAssignment, PartitionAssignment,
    GetConsumerStateRequest)
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest


#TODO: This test must be enabled once the direct consumer verifier support
# is added to vtools.
class DirectConsumerVerifierTest(RedpandaTest):
    def __init__(self, test_context: TestContext, **kwargs: Any):
        super().__init__(test_context, **kwargs)

    @cluster(num_nodes=4)
    def test_basic_consuming_from_topic(self):
        topic_name = "test-topic"
        msg_count = 200000
        msg_size = 128
        client_id = "test-consumer"

        topic_spec = TopicSpec(name=topic_name,
                               partition_count=128,
                               replication_factor=3)

        self.client().create_topic(topic_spec)

        KgoVerifierProducer.oneshot(self.test_context,
                                    self.redpanda,
                                    topic_name,
                                    msg_size=msg_size,
                                    msg_count=msg_count)

        verifier = DirectConsumerVerifier(self.test_context, log_level="DEBUG")
        verifier.start()

        try:
            # check if the verifier is alive
            verifier.status()

            brokers = [
                BrokerAddress(host=node.account.hostname, port=9092)
                for node in self.redpanda.nodes
            ]

            consumer_config = DirectConsumerConfiguration(
                min_bytes=1,
                max_fetch_size=1024 * 1024,
                partition_max_bytes=1024 * 1024,
                reset_policy=OffsetResetPolicy.EARLIEST,
                max_wait_time_ms=5000,
                isolation_level=IsolationLevel.READ_UNCOMMITTED,
                max_buffered_bytes=10 * 1024 * 1024,
                max_buffered_elements=200)

            create_request = CreateDirectConsumerRequest(
                client_id=client_id,
                initial_brokers=brokers,
                consumer_configuration=consumer_config)

            verifier.create_consumer(create_request)

            topic_assignment = TopicAssignment(
                topic=topic_name,
                partitions=[
                    PartitionAssignment(partition_id=i)
                    for i in range(topic_spec.partition_count)
                ])

            assign_request = AssignPartitionsRequest(
                client_id=client_id, topic_assignments=[topic_assignment])

            verifier.assign_partitions(assign_request)

            state_request = GetConsumerStateRequest(
                client_id=client_id, include_partition_states=True)

            def check_consumption():
                state = verifier.get_consumer_state(state_request)
                self.logger.info(
                    f"Consumer state: consumed {state.total_consumed_messages} messages"
                )
                return state.total_consumed_messages >= msg_count

            wait_until(check_consumption, timeout_sec=60, backoff_sec=2)

            final_state = verifier.get_consumer_state(state_request)
            assert final_state.total_consumed_messages == msg_count, \
                f"Expected {msg_count} messages, got {final_state.total_consumed_messages}"

            self.logger.info(
                f"Successfully consumed {final_state.total_consumed_messages} messages"
            )

        finally:
            verifier.stop()
