# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster_spec import ClusterSpec

import os

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.redpanda import SISettings
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableProducer, FranzGoVerifiableSeqConsumer, FranzGoVerifiableRandomConsumer
from rptest.services.franz_go_verifiable_services import KGO_ALLOW_LOGS


class FranzGoVerifiableBase(PreallocNodesTest):
    """
    Base class for the common logic that allocates a shared
    node for several services and instantiates them.
    """

    MSG_SIZE = None
    PRODUCE_COUNT = None
    RANDOM_READ_COUNT = None
    RANDOM_READ_PARALLEL = None

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context, node_prealloc_count=1, *args, **kwargs)

        self._producer = FranzGoVerifiableProducer(test_context, self.redpanda,
                                                   self.topic, self.MSG_SIZE,
                                                   self.PRODUCE_COUNT,
                                                   self.preallocated_nodes)
        self._seq_consumer = FranzGoVerifiableSeqConsumer(
            test_context, self.redpanda, self.topic, self.MSG_SIZE,
            self.preallocated_nodes)
        self._rand_consumer = FranzGoVerifiableRandomConsumer(
            test_context, self.redpanda, self.topic, self.MSG_SIZE,
            self.RANDOM_READ_COUNT, self.RANDOM_READ_PARALLEL,
            self.preallocated_nodes)


class FranzGoVerifiableTest(FranzGoVerifiableBase):
    MSG_SIZE = 120000
    PRODUCE_COUNT = 100000
    RANDOM_READ_COUNT = 1000
    RANDOM_READ_PARALLEL = 20

    topics = (TopicSpec(partition_count=100, replication_factor=3), )

    @cluster(num_nodes=4, log_allow_list=KGO_ALLOW_LOGS)
    def test_with_all_type_of_loads(self):
        self.logger.info(f"Environment: {os.environ}")
        if os.environ.get('BUILD_TYPE', None) == 'debug':
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        self._producer.start(clean=False)

        # Don't start consumers until the producer has written out its first
        # checkpoint with valid ranges.
        wait_until(lambda: self._producer.produce_status.acked > 0,
                   timeout_sec=30,
                   backoff_sec=0.1)
        wrote_at_least = self._producer.produce_status.acked

        self._seq_consumer.start(clean=False)
        self._rand_consumer.start(clean=False)

        self._producer.wait()
        assert self._producer.produce_status.acked == self.PRODUCE_COUNT

        self._seq_consumer.shutdown()
        self._rand_consumer.shutdown()

        self._seq_consumer.wait()
        self._rand_consumer.wait()

        assert self._seq_consumer.consumer_status.valid_reads >= wrote_at_least
        assert self._rand_consumer.consumer_status.total_reads == self.RANDOM_READ_COUNT * self.RANDOM_READ_PARALLEL


class FranzGoVerifiableWithSiTest(FranzGoVerifiableBase):
    MSG_SIZE = 1000000
    PRODUCE_COUNT = 20000
    RANDOM_READ_COUNT = 1000
    RANDOM_READ_PARALLEL = 20

    segment_size = 5 * 1000000

    topics = (TopicSpec(partition_count=100, replication_factor=3), )

    def __init__(self, ctx):
        si_settings = SISettings(
            log_segment_size=FranzGoVerifiableWithSiTest.segment_size,
            cloud_storage_cache_size=5 *
            FranzGoVerifiableWithSiTest.segment_size,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False)

        super(FranzGoVerifiableWithSiTest, self).__init__(
            test_context=ctx,
            num_brokers=3,
            extra_rp_conf={
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
            },
            si_settings=si_settings)

    @cluster(num_nodes=4, log_allow_list=KGO_ALLOW_LOGS)
    def test_with_all_type_of_loads_and_si(self):
        self.logger.info(f"Environment: {os.environ}")
        if os.environ.get('BUILD_TYPE', None) == 'debug':
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic, 'redpanda.remote.write', 'true')
        rpk.alter_topic_config(self.topic, 'redpanda.remote.read', 'true')
        rpk.alter_topic_config(self.topic, 'retention.bytes',
                               str(self.segment_size))

        self._producer.start(clean=False)

        # Don't start consumers until the producer has written out its first
        # checkpoint with valid ranges.
        wait_until(lambda: self._producer.produce_status.acked > 0,
                   timeout_sec=30,
                   backoff_sec=5.0)

        # nce we've written a lot of data, check that some of it showed up in S3
        wait_until(lambda: self._producer.produce_status.acked > 10000,
                   timeout_sec=300,
                   backoff_sec=5)
        objects = list(self.redpanda.get_objects_from_si())
        assert len(objects) > 0
        for o in objects:
            self.logger.info(f"S3 object: {o.Key}, {o.ContentLength}")

        wrote_at_least = self._producer.produce_status.acked
        self._seq_consumer.start(clean=False)
        self._rand_consumer.start(clean=False)

        # Wait until we have written all the data we expected to write
        self._producer.wait()
        assert self._producer.produce_status.acked >= self.PRODUCE_COUNT

        # Wait for last iteration of consumers to finish: if they are currently
        # mid-run, they'll run to completion.
        self._seq_consumer.shutdown()
        self._rand_consumer.shutdown()
        self._seq_consumer.wait()
        self._rand_consumer.wait()

        assert self._seq_consumer.consumer_status.valid_reads >= wrote_at_least
        assert self._rand_consumer.consumer_status.total_reads == self.RANDOM_READ_COUNT * self.RANDOM_READ_PARALLEL
