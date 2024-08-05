# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random

from rptest.clients.default import DefaultClient
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.services.failure_injector import FailureSpec
from rptest.services.redpanda import make_redpanda_service, CHAOS_LOG_ALLOW_LIST
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.e2e_finjector import Finjector
from rptest.util import Scale


class AvailabilityTests(EndToEndTest):
    def __init__(self, test_context, *args, **kwargs):
        self.scale = Scale(test_context)
        super(AvailabilityTests, self).__init__(test_context, *args, **kwargs)

    def validate_records(self):
        min_records = 40000
        producer_timeout_sec = 60
        consumer_timeout_sec = 60

        if self.scale.ci or self.scale.release:
            min_records = 100000
            producer_timeout_sec = 180
            consumer_timeout_sec = 180

        self.run_validation(min_records=min_records,
                            enable_idempotence=False,
                            producer_timeout_sec=producer_timeout_sec,
                            consumer_timeout_sec=consumer_timeout_sec)

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_availability_when_one_node_failed(self):
        self.redpanda = make_redpanda_service(
            self.test_context,
            3,
            extra_rp_conf={
                "partition_autobalancing_mode": "node_add",
                "group_topic_partitions": 1,
                "default_topic_replications": 3,
                # set disk timeout to value greater than max suspend time
                # not to emit spurious errors
                "raft_io_timeout_ms": 20000,
            })

        self.redpanda.start()
        spec = TopicSpec(name="test-topic",
                         partition_count=6,
                         replication_factor=3)

        DefaultClient(self.redpanda).create_topic(spec)
        self.topic = spec.name

        self.start_producer(1, throughput=10000)
        self.start_consumer(1)
        self.await_startup()
        # run failure injector loop with default parameters
        with Finjector(self.redpanda, self.scale).finj_thread():
            self.validate_records()

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_recovery_after_catastrophic_failure(self):

        self.redpanda = make_redpanda_service(
            self.test_context,
            3,
            extra_rp_conf={
                "partition_autobalancing_mode": "node_add",
                "group_topic_partitions": 1,
                "default_topic_replications": 3,
                # set disk timeout to value greater than max suspend time
                # not to emit spurious errors
                "raft_io_timeout_ms": 20000,
            })

        self.redpanda.start()
        spec = TopicSpec(name="test-topic",
                         partition_count=6,
                         replication_factor=3)

        DefaultClient(self.redpanda).create_topic(spec)
        self.topic = spec.name

        self.start_producer(1, throughput=10000)
        self.start_consumer(1)
        self.await_startup()

        with Finjector(self.redpanda, self.scale).finj_manual() as finj:
            # inject permanent random failure
            f_spec = FailureSpec(random.choice(FailureSpec.FAILURE_TYPES),
                                 random.choice(self.redpanda.nodes[0:1]))
            finj(f_spec)
            # inject transient failure on other node
            f_spec = FailureSpec(random.choice(FailureSpec.FAILURE_TYPES),
                                 self.redpanda.nodes[2],
                                 length=2.0 if self.scale.local else 15.0)
            finj(f_spec)
            self.validate_records()
