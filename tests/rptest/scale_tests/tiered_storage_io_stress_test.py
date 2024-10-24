# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
    KgoVerifierRandomConsumer,
    KgoVerifierConsumerGroupConsumer,
)
from rptest.services.redpanda import CloudStorageType, SISettings, RESTART_LOG_ALLOW_LIST, get_cloud_storage_type
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.si_utils import quiesce_uploads

KGO_LOG_ALLOW_LIST = [
    # rpc - server.cc:116 - kafka rpc protocol - Error[applying protocol] remote address: 172.18.0.31:56896 - std::out_of_range (Invalid skip(n). Expected:1000097, but skipped:524404)
    r'rpc - .* - std::out_of_range'
]

KGO_RESTART_LOG_ALLOW_LIST = KGO_LOG_ALLOW_LIST + RESTART_LOG_ALLOW_LIST


class TieredStorageIoStressTest(PreallocNodesTest):
    MSG_SIZE = 1000000
    PRODUCE_COUNT = 20000
    RANDOM_READ_COUNT = 1000
    RANDOM_READ_PARALLEL = 20
    CONSUMER_GROUP_READERS = 8

    topics = (TopicSpec(partition_count=100, replication_factor=3), )

    def __init__(self, test_context, *args, **kwargs):
        extra_rp_conf = {
            # Disable prometheus metrics, because we are doing lots
            # of restarts with lots of partitions, and current high
            # metric counts make that sometimes cause reactor stalls
            # during shutdown on debug builds.
            'disable_metrics': True,

            # We will run relatively large number of partitions
            # and want it to work with slow debug builds and
            # on noisy developer workstations: relax the raft
            # intervals
            'election_timeout_ms': 5000,
            'raft_heartbeat_interval_ms': 500,
            'log_segment_size_jitter_percent': 5,
        }

        # Enable segment size jitter as this is a stress test and does not
        # rely on exact segment counts.
        extra_rp_conf['log_segment_size_jitter_percent'] = 5

        # Set a modest reader concurrency limit to run safely on GB-per-core
        # test environments
        extra_rp_conf['cloud_storage_max_segment_readers_per_shard'] = 500

        super().__init__(test_context=test_context,
                         node_prealloc_count=1,
                         *args,
                         extra_rp_conf=extra_rp_conf,
                         **kwargs)

        self._producer = KgoVerifierProducer(test_context, self.redpanda,
                                             self.topic, self.MSG_SIZE,
                                             self.PRODUCE_COUNT,
                                             self.preallocated_nodes)
        self._seq_consumer = KgoVerifierSeqConsumer(
            test_context,
            self.redpanda,
            self.topic,
            self.MSG_SIZE,
            nodes=self.preallocated_nodes)
        self._rand_consumer = KgoVerifierRandomConsumer(
            test_context, self.redpanda, self.topic, self.MSG_SIZE,
            self.RANDOM_READ_COUNT, self.RANDOM_READ_PARALLEL,
            self.preallocated_nodes)
        self._cg_consumer = KgoVerifierConsumerGroupConsumer(
            test_context,
            self.redpanda,
            self.topic,
            self.MSG_SIZE,
            self.CONSUMER_GROUP_READERS,
            nodes=self.preallocated_nodes)

        self._consumers = [
            self._seq_consumer, self._rand_consumer, self._cg_consumer
        ]

    def setUp(self):
        # Defer redpanda startup to test body
        pass

    def _workload(self, segment_size: int, interval_uploads: bool):
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic, 'redpanda.remote.write', 'true')
        rpk.alter_topic_config(self.topic, 'redpanda.remote.read', 'true')
        rpk.alter_topic_config(self.topic, 'retention.local.target.bytes',
                               str(segment_size))

        self._producer.start(clean=False)

        # Once we've written a significant amount of data, check that some of it showed up in S3
        self._producer.wait_for_acks(self.PRODUCE_COUNT // 10,
                                     timeout_sec=300,
                                     backoff_sec=5)

        objects = list(self.redpanda.get_objects_from_si())
        assert len(objects) > 0
        for o in objects:
            self.logger.info(f"S3 object: {o.key}, {o.content_length}")

        wrote_at_least = self._producer.produce_status.acked

        self._producer.wait_for_offset_map()
        for consumer in self._consumers:
            consumer.start(clean=False)

        # Wait until we have written all the data we expected to write
        self._producer.wait()
        assert self._producer.produce_status.acked >= self.PRODUCE_COUNT

        # Wait for last iteration of consumers to finish: if they are currently
        # mid-run, they'll run to completion.
        for consumer in self._consumers:
            consumer.wait()

        assert self._seq_consumer.consumer_status.validator.valid_reads >= wrote_at_least
        assert self._rand_consumer.consumer_status.validator.total_reads >= self.RANDOM_READ_COUNT * self.RANDOM_READ_PARALLEL
        assert self._cg_consumer.consumer_status.validator.valid_reads >= wrote_at_least

        if interval_uploads:
            # Ensure that uploads run to completion.
            quiesce_uploads(self.redpanda, [self.topic], timeout_sec=120)

    @skip_debug_mode
    @cluster(num_nodes=4, log_allow_list=KGO_RESTART_LOG_ALLOW_LIST)
    @matrix(
        cloud_storage_type=get_cloud_storage_type(docker_use_arbitrary=True),
        segment_size=[1024 * 1024, 128 * 1024 * 1024],
        interval_uploads=[True, False])
    def test_io_stress(self, cloud_storage_type, segment_size,
                       interval_uploads):
        # We expect to produce & consume at least this fast
        expected_throughput = 100 * 1024 * 1024

        si_settings = SISettings(
            self.test_context,
            log_segment_size=segment_size,
            cloud_storage_segment_max_upload_interval_sec=30
            if interval_uploads else None,

            # A cache large enough to accommodate a respectable read throughput
            cloud_storage_cache_size=SISettings.cache_size_for_throughput(
                expected_throughput))
        self.redpanda.set_si_settings(si_settings)

        self.redpanda.start()
        self._create_initial_topics()

        self._workload(segment_size, interval_uploads)
