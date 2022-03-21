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
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings
from rptest.services.franz_go_verifiable_services import FranzGoVerifiableProducer, FranzGoVerifiableSeqConsumer, FranzGoVerifiableRandomConsumer


class FranzGoVerifiableBase(RedpandaTest):
    """
    Base class for the common logic that allocates a shared
    node for several services and instantiates them.
    """

    MSG_SIZE = None
    PRODUCE_COUNT = None
    RANDOM_READ_COUNT = None
    RANDOM_READ_PARALLEL = None

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context, *args, **kwargs)

        self._node_for_franz_go = test_context.cluster.alloc(
            ClusterSpec.simple_linux(1))
        self.logger.debug(
            f"Allocated verifier node {self._node_for_franz_go[0].name}")

        self._producer = FranzGoVerifiableProducer(test_context, self.redpanda,
                                                   self.topic, self.MSG_SIZE,
                                                   self.PRODUCE_COUNT,
                                                   self._node_for_franz_go)
        self._seq_consumer = FranzGoVerifiableSeqConsumer(
            test_context, self.redpanda, self.topic, self.MSG_SIZE,
            self._node_for_franz_go)
        self._rand_consumer = FranzGoVerifiableRandomConsumer(
            test_context, self.redpanda, self.topic, self.MSG_SIZE,
            self.RANDOM_READ_COUNT, self.RANDOM_READ_PARALLEL,
            self._node_for_franz_go)

    # In the future producer will signal about json creation
    def _create_json_file(self, msg_count):
        small_producer = FranzGoVerifiableProducer(self.test_context,
                                                   self.redpanda, self.topic,
                                                   self.MSG_SIZE, msg_count,
                                                   self._node_for_franz_go)
        small_producer.start()
        small_producer.wait()

    def free_nodes(self):
        # Free the normally allocated nodes (e.g. RedpandaService)
        super().free_nodes()

        assert len(self._node_for_franz_go) == 1

        # The verifier opens huge numbers of connections, which can interfere
        # with subsequent tests' use of the node.  Clear them down first.
        wait_until(
            lambda: self.redpanda.sockets_clear(self._node_for_franz_go[0]),
            timeout_sec=120,
            backoff_sec=10)

        # Free the hand-allocated node that we share between the various
        # verifier services
        self.logger.debug(
            f"Freeing verifier node {self._node_for_franz_go[0].name}")
        self.test_context.cluster.free_single(self._node_for_franz_go[0])


class FranzGoVerifiableTest(FranzGoVerifiableBase):
    MSG_SIZE = 120000
    PRODUCE_COUNT = 100000
    RANDOM_READ_COUNT = 1000
    RANDOM_READ_PARALLEL = 20

    topics = (TopicSpec(partition_count=100, replication_factor=3), )

    @cluster(num_nodes=4)
    def test_with_all_type_of_loads(self):
        self.logger.info(f"Environment: {os.environ}")
        if os.environ.get('BUILD_TYPE', None) == 'debug':
            self.logger.info(
                "Skipping test in debug mode (requires release build)")
            return

        # Need create json file for consumer at first
        self._create_json_file(1000)

        self._producer.start(clean=False)
        self._seq_consumer.start(clean=False)
        self._rand_consumer.start(clean=False)

        self._producer.wait()
        self._seq_consumer.shutdown()
        self._rand_consumer.shutdown()
        self._seq_consumer.wait()
        self._rand_consumer.wait()


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
            FranzGoVerifiableWithSiTest.segment_size)

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

    @cluster(num_nodes=4)
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

        # Need create json file for consumer at first
        self._create_json_file(10000)

        # Verify that we really enabled shadow indexing correctly, such
        # that some objects were written
        objects = list(self.redpanda.get_objects_from_si())
        assert len(objects) > 0
        for o in objects:
            self.logger.info(f"S3 object: {o.Key}, {o.ContentLength}")

        self._producer.start(clean=False)
        self._seq_consumer.start(clean=False)
        self._rand_consumer.start(clean=False)

        self._producer.wait()
        self._seq_consumer.shutdown()
        self._rand_consumer.shutdown()
        self._seq_consumer.wait()
        self._rand_consumer.wait()
