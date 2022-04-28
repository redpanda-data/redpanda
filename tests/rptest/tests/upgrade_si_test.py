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
from rptest.services.kaf_producer import KafProducer
from rptest.services.kaf_consumer import KafConsumer

from rptest.services.rpk_consumer import RpkConsumer
from rptest.services.rpk_producer import RpkProducer
import time

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

        self.test_context = test_context

        self._node_for_franz_go = test_context.cluster.alloc(
            ClusterSpec.simple_linux(1))
        self.logger.debug(
            f"Allocated verifier node {self._node_for_franz_go[0].name}")

        self._producer = FranzGoVerifiableProducer(self.test_context, self.redpanda,
                                                   self.topic, self.MSG_SIZE,
                                                   self.PRODUCE_COUNT,
                                                   self._node_for_franz_go)
        self._seq_consumer = FranzGoVerifiableSeqConsumer(
            self.test_context, self.redpanda, self.topic, self.MSG_SIZE,
            self._node_for_franz_go)
        self._rand_consumer = FranzGoVerifiableRandomConsumer(
            self.test_context, self.redpanda, self.topic, self.MSG_SIZE,
            self.RANDOM_READ_COUNT, self.RANDOM_READ_PARALLEL,
            self._node_for_franz_go)   

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


class UpgradeFranzGoVerifiableWithSiTest(FranzGoVerifiableBase):
    MSG_SIZE = 1000000
    PRODUCE_COUNT = 20000
    RANDOM_READ_COUNT = 1000
    RANDOM_READ_PARALLEL = 20

    segment_size = 5 * 1000000

    topics = (TopicSpec(partition_count=100, replication_factor=3), )

    def __init__(self, ctx):
        si_settings = SISettings(
            log_segment_size=UpgradeFranzGoVerifiableWithSiTest.segment_size,
            cloud_storage_cache_size=5 *
            UpgradeFranzGoVerifiableWithSiTest.segment_size)

        super(UpgradeFranzGoVerifiableWithSiTest, self).__init__(
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
            si_settings=si_settings,
            legacy_config_mode=True)

    @cluster(num_nodes=10)
    def test_with_all_type_of_loads_and_si(self):
        self.logger.info(f"Environment: {os.environ}")

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
        wait_until(lambda: self._producer.produce_status.acked > 1000,
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

        self.kaf_consumer = KafConsumer(self.test_context, self.redpanda, self.topic, self.PRODUCE_COUNT, "oldest")
        self.old = -1
        self.kaf_consumer.start()

        rpk = RpkConsumer(context=self.test_context, redpanda=self.redpanda, topic=self.topic, group="foo")
        rpk.start()
        time.sleep(10)
        rpk.stop()
        rpk.wait()

        def consumed1():
            self.logger.debug(
                f"Offset for consumer: {self.kaf_consumer.offset}")
            self.logger.debug(f"{sum(self.kaf_consumer.offset.values())}")
            ssum = sum(self.kaf_consumer.offset.values())
            res = (ssum == self.old) and (ssum != 0)
            self.old = ssum
            return res;

        wait_until(consumed1, timeout_sec=350, backoff_sec=2)

        for node in self.redpanda.nodes:
            self.redpanda.stop_node(node, timeout=300)
            node.account.ssh("curl -1sLf https://packages.vectorized.io/E4xN1tVe3Xy60GTx/redpanda-unstable/setup.deb.sh | sudo -E bash", allow_fail=False)
            node.account.ssh('sudo apt -o  Dpkg::Options::="--force-confnew" install redpanda', allow_fail=False)
            node.account.ssh("sudo systemctl stop redpanda")
            self.redpanda.start_node(node, None, timeout=300)

        producer = RpkProducer(self.test_context,
                               self.redpanda,
                               self.topic,
                               msg_size=self.MSG_SIZE,
                               msg_count=1000,
                               acks="all",
                               produce_timeout=100)

        producer.start()
        producer.wait()

        self.kaf_consumer = KafConsumer(self.test_context, self.redpanda, self.topic, self.PRODUCE_COUNT + 1000, "oldest")
        self.kaf_consumer.start()

        def consumed():
            self.logger.debug(
                f"Offset for consumer: {self.kaf_consumer.offset}")
            self.logger.debug(f"{sum(self.kaf_consumer.offset.values())}")
            return sum(self.kaf_consumer.offset.values()) >= self.old + 1000

        wait_until(consumed, timeout_sec=350, backoff_sec=2)

        self.kaf_consumer.stop()
