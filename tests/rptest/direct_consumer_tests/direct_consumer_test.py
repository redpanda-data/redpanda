# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any

import random
import threading
from enum import Enum
from typing import Callable

from ducktape.tests.test import TestContext
from ducktape.mark import matrix
from rptest.services.cluster import cluster
from rptest.services.admin import Admin

from rptest.clients.types import TopicSpec
from rptest.services.direct_consumer_verifier import (
    AssignPartitionsRequest,
    BrokerAddress,
    CreateDirectConsumerRequest,
    DirectConsumerConfiguration,
    DirectConsumerVerifier,
    GetConsumerStateRequest,
    IsolationLevel,
    OffsetResetPolicy,
    PartitionAssignment,
    TopicAssignment,
)
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_with_progress_check
from rptest.utils.node_operations import (
    FailureInjectorBackgroundThread,
)


class FailureMode(str, Enum):
    NONE = "NONE"
    LEADERSHIP_TRANSFER = "LEADERSHIP_TRANSFER"
    RANDOM = "RANDOM"


class NoopThread:
    def stop(self):
        pass

    def start(self):
        pass


class LoopThread(threading.Thread):
    """Run loop func until graceful cancellation is requested"""

    def _loop(self, *args, **kwargs):
        while not self.stopped():
            self._loop_func(*args, **kwargs)

    def __init__(self, *args, loop_func: Callable[[], None], **kwargs):
        super(LoopThread, self).__init__(*args, target=self._loop, **kwargs)
        self._stop_event = threading.Event()
        self._loop_func = loop_func

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


# TODO: This test must be enabled once the direct consumer verifier support
# is added to vtools.
class DirectConsumerVerifierTest(RedpandaTest):
    def __init__(self, test_context: TestContext, **kwargs: Any):
        super().__init__(test_context, **kwargs)

    def shuffle_one_leader(self, topic_spec: TopicSpec) -> None:
        """randomly chooses a partition in the given topic spec and moves its leader one replica over"""
        namespace = "kafka"
        topic_name = topic_spec.name
        partition_number = random.choice(range(topic_spec.partition_count))
        admin = Admin(self.redpanda)
        # logs to debug
        transfer_succeeded = admin.transfer_leadership_to(
            namespace="kafka",
            topic=topic_spec.name,
            partition=random.randrange(0, topic_spec.partition_count),
        )
        self.redpanda.logger.debug(
            f"transfer of ntp {namespace}/{topic_name}/{partition_number} success? {transfer_succeeded}"
        )

    def create_troublemaker_thread(
        self, topic_spec: TopicSpec, thread_name="stream_thread"
    ) -> LoopThread:
        """creates a background thread to drive paritition leadership transfers"""
        thread = LoopThread(
            name=thread_name, loop_func=self.shuffle_one_leader, args=(topic_spec,)
        )
        return thread

    def get_failure_thread(self, failure_mode: FailureMode, topic_spec: str):
        match failure_mode:
            case FailureMode.LEADERSHIP_TRANSFER:
                return self.create_troublemaker_thread(topic_spec)
            case FailureMode.RANDOM:
                return FailureInjectorBackgroundThread(
                    self.redpanda,
                    self.logger,
                    max_inter_failure_time=30,
                    min_inter_failure_time=10,
                    max_suspend_duration_seconds=7,
                )
            case FailureMode.NONE:
                return NoopThread()

    @cluster(num_nodes=5)
    @matrix(
        failure_mode=[
            FailureMode.NONE,
            FailureMode.LEADERSHIP_TRANSFER,
            FailureMode.RANDOM,
        ]
    )
    def test_basic_consuming_from_topic(self, failure_mode):
        topic_name = "test-topic"
        msg_count = 200000
        msg_size = 128
        client_id = "test-consumer"

        topic_spec = TopicSpec(
            name=topic_name, partition_count=128, replication_factor=3
        )

        self.client().create_topic(topic_spec)

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic_name,
            msg_size=msg_size,
            msg_count=msg_count,
        )
        producer.start()

        verifier = DirectConsumerVerifier(self.test_context, log_level="DEBUG")
        verifier.start()

        troublemaker = self.get_failure_thread(failure_mode, topic_spec)
        troublemaker.start()

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
                max_buffered_elements=200,
            )

            create_request = CreateDirectConsumerRequest(
                client_id=client_id,
                initial_brokers=brokers,
                consumer_configuration=consumer_config,
            )

            verifier.create_consumer(create_request)

            topic_assignment = TopicAssignment(
                topic=topic_name,
                partitions=[
                    PartitionAssignment(partition_id=i)
                    for i in range(topic_spec.partition_count)
                ],
            )

            assign_request = AssignPartitionsRequest(
                client_id=client_id, topic_assignments=[topic_assignment]
            )

            verifier.assign_partitions(assign_request)

            state_request = GetConsumerStateRequest(
                client_id=client_id, include_partition_states=True
            )

            def get_consumption():
                state = verifier.get_consumer_state(state_request)
                self.logger.debug(
                    f"Expecting: {msg_count} Consumer state: consumed {state.total_consumed_messages} messages"
                )
                return state.total_consumed_messages

            wait_until_with_progress_check(
                get_consumption,
                condition=lambda: get_consumption() >= msg_count,
                timeout_sec=600,
                progress_sec=10,
                backoff_sec=2,
                err_msg="Stopped consuming",
                logger=self.logger,
            )

            final_state = verifier.get_consumer_state(state_request)

            # assertions
            assert final_state.total_consumed_messages == msg_count, (
                f"Expected {msg_count} messages, got {final_state.total_consumed_messages}"
            )

            assert int(final_state.non_monotonic_fetches) == 0, (
                f"Non-monotonic fetches found, number of nm fetches: {final_state.non_monotonic_fetches}"
            )

            self.logger.info(
                f"Successfully consumed {final_state.total_consumed_messages} messages"
            )

        finally:
            troublemaker.stop()
            verifier.stop()
            producer.wait(timeout_sec=600)
            producer.stop()
