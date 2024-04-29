# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import threading
import time
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import SISettings
from rptest.tests.end_to_end import EndToEndTest
from rptest.utils.mode_checks import skip_debug_mode
from ducktape.mark import parametrize


class SimpleEndToEndTest(EndToEndTest):
    def __init__(self, test_context, *args, **kwargs):

        super(SimpleEndToEndTest, self).__init__(test_context=test_context,
                                                 *args,
                                                 **kwargs)

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

        self.start_redpanda(num_nodes=3)

        spec = TopicSpec(partition_count=1, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name

        self.start_producer(1, throughput=1000)
        self.start_consumer(1)
        # wait for at least 15000 records to be consumed
        self.await_startup(min_records=15000)
        self.client().delete_topic(spec.name)

        self.client().create_topic(spec)
        error = None
        try:
            self.run_validation(min_records=30000,
                                producer_timeout_sec=300,
                                consumer_timeout_sec=300)
        except AssertionError as e:
            error = e

        assert error is not None
        assert "Consumed from an unexpected" in str(
            error) or "is behind the current committed offset" in str(error)

    @skip_debug_mode
    @cluster(num_nodes=6)
    @parametrize(write_caching=True, disable_batch_cache=False)
    @parametrize(write_caching=True, disable_batch_cache=True)
    @parametrize(write_caching=False)
    def test_relaxed_acks(self, write_caching, disable_batch_cache=False):
        stop_fi_ev = threading.Event()

        def inject_failures():
            fi = FailureInjector(self.redpanda)
            while not stop_fi_ev.is_set():
                node = random.choice(self.redpanda.nodes)
                fi.inject_failure(
                    FailureSpec(type=FailureSpec.FAILURE_KILL,
                                length=0,
                                node=node))
                time.sleep(10)

        (acks, wc_conf) = (-1, "true") if write_caching else (1, "false")
        # use small segment size to enable log eviction
        self.start_redpanda(num_nodes=3,
                            si_settings=SISettings(
                                test_context=self.test_context,
                                fast_uploads=True),
                            extra_rp_conf={
                                "log_segment_size":
                                1048576,
                                "retention_bytes":
                                5242880,
                                "default_topic_replications":
                                3,
                                "write_caching_default":
                                wc_conf,
                                "raft_replica_max_pending_flush_bytes":
                                1024 * 1024 * 1024 * 1024,
                                "raft_replica_max_flush_delay_ms":
                                3000000,
                                "disable_batch_cache":
                                disable_batch_cache,
                            })

        self.logger.debug(
            f"Using configuration: acks: {acks}, write_caching: {wc_conf}")
        spec = TopicSpec(name="verify-leader-ack",
                         partition_count=16,
                         replication_factor=3)
        self.client().create_topic(spec)

        self.topic = spec.name

        self.start_producer(2, throughput=10000, acks=acks)

        self.start_consumer(1)
        self.await_startup()

        fi_thread = threading.Thread(target=inject_failures, daemon=True)
        fi_thread.start()
        self.run_validation(min_records=100000,
                            producer_timeout_sec=300,
                            consumer_timeout_sec=300)
        stop_fi_ev.set()
        fi_thread.join()
