# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.services.compacted_verifier import CompactedVerifier, Workload

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec


class CompactedVerifierTest(RedpandaTest):
    partition_count = 2
    topics = [TopicSpec(partition_count=partition_count, replication_factor=3)]

    def __init__(self, test_context):
        extra_rp_conf = {
            "id_allocator_replication": 3,
            "default_topic_replications": 3,
            "default_topic_partitions": 1
        }

        super(CompactedVerifierTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=4)
    def test_idempotency(self):
        verifier = CompactedVerifier(self.test_context, self.redpanda,
                                     Workload.IDEMPOTENCY)
        verifier.start()

        verifier.remote_start_producer(self.redpanda.brokers(), self.topic,
                                       self.partition_count)
        self.logger.info(f"Waiting for 1000 writes")
        verifier.ensure_progress(1000, 30)
        self.logger.info(f"Done")
        verifier.remote_stop_producer()
        verifier.remote_wait_producer()
        verifier.remote_start_consumer()
        verifier.remote_wait_consumer()

    @cluster(num_nodes=4)
    def test_tx(self):
        verifier = CompactedVerifier(self.test_context, self.redpanda,
                                     Workload.TX)
        verifier.start()

        verifier.remote_start_producer(self.redpanda.brokers(), self.topic,
                                       self.partition_count)
        self.logger.info(f"Waiting for 100 writes")
        verifier.ensure_progress(100, 30)
        self.logger.info(f"Done")
        verifier.remote_stop_producer()
        verifier.remote_wait_producer()
        verifier.remote_start_consumer()
        verifier.remote_wait_consumer()

    @cluster(num_nodes=4)
    def test_tx_unique_keys(self):
        verifier = CompactedVerifier(self.test_context, self.redpanda,
                                     Workload.TX_UNIQUE_KEYS)
        verifier.start()

        verifier.remote_start_producer(self.redpanda.brokers(), self.topic,
                                       self.partition_count)
        self.logger.info(f"Waiting for 100 writes")
        verifier.ensure_progress(100, 30)
        self.logger.info(f"Done")
        verifier.remote_stop_producer()
        verifier.remote_wait_producer()
        verifier.remote_start_consumer()
        verifier.remote_wait_consumer()