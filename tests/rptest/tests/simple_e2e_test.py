# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest


class SimpleEndToEndTest(EndToEndTest):
    @cluster(num_nodes=6)
    def test_correctness_while_evicitng_log(self):
        '''
        Validate that all the records will be delivered to consumers when there
        are multiple producers and log is evicted
        '''
        # use small segment size to enable log eviction
        self.start_redpanda(num_nodes=3,
                            extra_rp_conf={
                                "log_segment_size": 1048576,
                                "retention_bytes": 5242880,
                                "default_topic_replications": 3,
                            })

        spec = TopicSpec(name="topic", partition_count=1, replication_factor=1)
        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(2, throughput=10000)
        self.start_consumer(1)
        self.await_startup()

        self.run_validation(min_records=100000,
                            producer_timeout_sec=300,
                            consumer_timeout_sec=300)

    @cluster(num_nodes=5)
    def test_consumer_interruption(self):
        '''
        This test validates if verifiable consumer is exiting early when consumed from unexpected offset
        '''
        # use small segment size to enable log eviction
        self.start_redpanda(num_nodes=3)

        spec = TopicSpec(partition_count=1, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(1, throughput=10000)
        self.start_consumer(1)
        self.await_startup()
        #
        self.client().delete_topic(spec.name)

        self.client().create_topic(spec)
        error = None
        try:
            self.run_validation(min_records=100000,
                                producer_timeout_sec=300,
                                consumer_timeout_sec=300)
        except AssertionError as e:
            error = e

        assert error is not None
        assert "Consumed from an unexpected" in str(error)
