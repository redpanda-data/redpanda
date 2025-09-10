# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import datetime
import time
from dataclasses import dataclass
from typing import Callable

from confluent_kafka import Producer

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest


def expect_failure_callback(err, msg):
    assert err is not None, "Expected failure"


def expect_success_callback(err, msg):
    assert err is None, "Expected success"


@dataclass
class TestCase:
    key: str
    value: str
    timestamp: int
    callback: Callable


class TimestampPolicyTest(RedpandaTest):
    topics = (
        TopicSpec(
            partition_count=1,
            replication_factor=1,
            cleanup_policy=TopicSpec.CLEANUP_DELETE,
        ),
    )

    @cluster(num_nodes=1)
    def test_produce_timestamps(self):
        """
        Test that bounds set by `message.timestamp.{before/after}.max.ms` are respected
        when producing.
        """

        # Bound valid timestamps by one hour. Expect any records with timestamps
        # outside this bound to be rejected at time of production.
        hour = 3600 * 1000
        self.client().alter_topic_config(
            self.topic, "message.timestamp.before.max.ms", hour
        )

        self.client().alter_topic_config(
            self.topic, "message.timestamp.after.max.ms", hour
        )

        producer = Producer({"bootstrap.servers": self.redpanda.brokers()})

        test_cases = [
            TestCase(
                "Roads?",
                "Where we're going, we don't need roads.",
                int(datetime.datetime(1955, 11, 5, 0, 0).timestamp() * 1000),
                expect_failure_callback,
            ),
            TestCase(
                "Do you ever have deja vu?",
                "I don't think so, but I could check with the kitchen.",
                int(datetime.datetime(1993, 2, 2, 0, 0).timestamp() * 1000),
                expect_failure_callback,
            ),
            TestCase(
                "Drat.",
                "Just barely missed it!",
                int((time.time() - 61 * 60) * 1000),
                expect_failure_callback,
            ),
            TestCase(
                "I'll be back.",
                "...in 59 minutes.",
                int((time.time() - 59 * 60) * 1000),
                expect_success_callback,
            ),
            TestCase(
                "There's no time like the present.",
                "Unless the present has traffic.",
                int(time.time() * 1000),
                expect_success_callback,
            ),
            TestCase(
                "If I could turn back time...",
                "...I’d go back 59 minutes and grab another donut.",
                int((time.time() + 59 * 60) * 1000),
                expect_success_callback,
            ),
            TestCase(
                "Oops.",
                "Missed it by a hair!",
                int((time.time() + 61 * 60) * 1000),
                expect_failure_callback,
            ),
            TestCase(
                "Be excellent to each other.",
                "Party on, dudes!",
                int(datetime.datetime(2688, 1, 1, 0, 0).timestamp() * 1000),
                expect_failure_callback,
            ),
            TestCase(
                "Year 3000!",
                "Here's to another lousy millennium.",
                int(datetime.datetime(2999, 12, 31, 0, 0).timestamp() * 1000),
                expect_failure_callback,
            ),
        ]

        for test_case in test_cases:
            producer.produce(
                topic=self.topic,
                key=test_case.key.encode("utf-8"),
                value=test_case.value.encode("utf-8"),
                timestamp=test_case.timestamp,
                callback=test_case.callback,
            )
            producer.flush()
