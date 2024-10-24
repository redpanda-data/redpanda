# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster_spec import ClusterSpec

import os
import time

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierRandomConsumer


class ShadowIndexingCacheSpaceLeakTest(RedpandaTest):
    """
    The test checks that SI cache doesn't exhibit a resource leak.
    In order to do this the test puts pressure to SI cache by settings its
    size to the minimum value. Then it uses KgoVerifierProducer(Consumer)
    to produce and consume data via SI. The retention on the topic has to be
    small enough in order for SI to be involved. The test checks that no segment
    files are opened in the cache directory.
    """

    # Random consumer loops indefinitely, doing this many reads each
    # loop: this must be small enough to complete promptly once we
    # enter wait().  Because we use a tiny cache, readers will often
    # encounter the tiered storage cache's throttling, so doing
    # just this many reads will not be very fast in this particular test.
    rand_consumer_msgs_per_pass = 100

    topics = (TopicSpec(partition_count=100, replication_factor=3), )
    test_defaults = {
        'default':
        dict(log_segment_size=1024 * 1024,
             cloud_storage_cache_size=10 * 1024 * 1024),
    }

    def __init__(self, test_context, *args, **kwargs):
        test_name = test_context.test_name
        si_params = self.test_defaults.get(
            test_name) or self.test_defaults.get('default')
        si_settings = SISettings(test_context, **si_params)
        self._segment_size = si_params['log_segment_size']
        extra_rp_conf = {
            'disable_metrics': True,
            'election_timeout_ms': 5000,
            'raft_heartbeat_interval_ms': 500,
            'segment_fallocation_step': 0x1000,
            'retention_local_target_bytes_default': self._segment_size,
            'retention_bytes': self._segment_size * 5,
            'cloud_storage_cache_check_interval': 500,
        }
        super().__init__(test_context,
                         num_brokers=3,
                         extra_rp_conf=extra_rp_conf,
                         si_settings=si_settings)
        self._ctx = test_context
        self._verifier_node = test_context.cluster.alloc(
            ClusterSpec.simple_linux(1))[0]
        self.logger.info(
            f"Verifier node name: {self._verifier_node.name}, segment_size: {self._segment_size}"
        )

    def init_producer(self, msg_size, num_messages):
        self._producer = KgoVerifierProducer(self._ctx, self.redpanda,
                                             self.topic, msg_size,
                                             num_messages,
                                             [self._verifier_node])

    def init_consumer(self, msg_size, concurrency):
        self._consumer = KgoVerifierRandomConsumer(
            self._ctx, self.redpanda, self.topic, msg_size,
            self.rand_consumer_msgs_per_pass, concurrency,
            [self._verifier_node])

    def free_nodes(self):
        super().free_nodes()
        wait_until(lambda: self.redpanda.sockets_clear(self._verifier_node),
                   timeout_sec=120,
                   backoff_sec=10)
        self.test_context.cluster.free_single(self._verifier_node)

    @cluster(num_nodes=4,
             log_allow_list=[r'failed to hydrate chunk.*NotFound'])
    @matrix(message_size=[10000], num_messages=[100000], concurrency=[2])
    def test_si_cache(self, message_size, num_messages, concurrency):
        if self.debug_mode:
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        self.init_producer(message_size, num_messages)
        self._producer.start(clean=False)

        def s3_has_some_data():
            objects = list(self.redpanda.get_objects_from_si())
            total_size = 0
            for o in objects:
                total_size += o.content_length
            return total_size > self._segment_size

        wait_until(s3_has_some_data, timeout_sec=300, backoff_sec=5)

        self.init_consumer(message_size, concurrency)
        self._consumer.start(clean=False)

        self._producer.wait()

        # Verify that all files in cache are being closed
        def cache_files_closed():
            def is_cache_file(fname):
                # We target files in the cloud storage cache directory
                # and deleted files. The deleted files are likely cache
                # files that were deleted by retention previously but
                # still kept open because they're used by the consumer.
                #
                # Do not use an absolute path, because the redpanda data
                # directory may have been a symlink & the path in lsof
                # will be the underlying storage.
                return "data/cloud_storage_cache" in fname or fname == "(deleted)"

            files_count = 0
            for node in self.redpanda.nodes:
                files = self.redpanda.lsof_node(node)
                cache_files = [f for f in files if is_cache_file(f)]
                for f in cache_files:
                    self.logger.debug(f"Open file: {f}")

                files_count += len(cache_files)
            return files_count == 0

        # Reader should eventually trigger some SI cache reads when
        # retention settings evict segment from local disk.
        wait_until(lambda: not cache_files_closed(),
                   timeout_sec=30,
                   backoff_sec=5)

        self._consumer.wait()

        assert self._producer.produce_status.acked >= num_messages
        assert self._consumer.consumer_status.validator.total_reads >= self.rand_consumer_msgs_per_pass * concurrency

        assert not cache_files_closed()
        # Wait until all files are closed. The SI evicts all unused segments
        # after one minute of inactivity.
        wait_until(cache_files_closed, timeout_sec=120, backoff_sec=10)
