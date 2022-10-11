# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RedpandaService, SISettings
from rptest.services.metrics_check import MetricCheck
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk_remote import RpkRemoteTool
from rptest.util import (segments_count, wait_for_segments_removal)

from rptest.services.kgo_verifier_services import KgoVerifierProducer

from ducktape.mark import parametrize

from rptest.services.kafka import KafkaServiceAdapter
from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService
from ducktape.mark.resource import cluster as ducktape_cluster
from kafkatest.version import V_3_0_0
from ducktape.tests.test import Test
from rptest.clients.default import DefaultClient


class BaseTimeQuery:
    def _test_timequery(self, cluster, cloud_storage: bool, batch_cache: bool):
        local_retain_segments = 4
        total_segments = 12
        record_size = 1024

        topic = TopicSpec(name="tqtopic",
                          partition_count=1,
                          replication_factor=3)

        self.client().create_topic(topic)

        if cloud_storage:
            for k, v in {
                    'redpanda.remote.read': True,
                    'redpanda.remote.write': True,
                    'retention.bytes':
                    self.log_segment_size * local_retain_segments
            }.items():
                self.client().alter_topic_config(topic.name, k, v)

        # Configure topic to trust client-side timestamps, so that
        # we can generate fake ones for the test
        self.client().alter_topic_config(topic.name, 'message.timestamp.type',
                                         "CreateTime")

        # Disable time based retention because we will use synthetic timestamps
        # that may well fall outside of the default 1 week relative to walltime
        self.client().alter_topic_config(topic.name, 'retention.ms', "-1")

        # Use small segments
        self.client().alter_topic_config(topic.name, 'segment.bytes',
                                         self.log_segment_size)

        # Produce a run of messages with CreateTime-style timestamps, each
        # record having a timestamp 1ms greater than the last.
        msg_count = (self.log_segment_size * total_segments) // record_size
        base_ts = 1664453149000
        producer = KgoVerifierProducer(
            context=self.test_context,
            redpanda=cluster,
            topic=topic.name,
            msg_size=record_size,
            msg_count=msg_count,

            # A respectable number of messages per batch so that we are covering
            # the case of looking up timestamps that fall in the middle of a batch,
            # but small enough that we are getting plenty of distinct batches.
            batch_max_bytes=record_size * 10,

            # A totally arbitrary artificial timestamp base in milliseconds
            fake_timestamp_ms=base_ts)
        producer.start()
        producer.wait()

        # Confirm messages written
        rpk = RpkTool(cluster)
        p = next(rpk.describe_topic(topic.name))
        assert p.high_watermark == msg_count

        if cloud_storage:
            # If using cloud storage, we must wait for some segments
            # to fall out of local storage, to ensure we are really
            # hitting the cloud storage read path when querying.
            wait_for_segments_removal(redpanda=cluster,
                                      topic=topic.name,
                                      partition_idx=0,
                                      count=local_retain_segments)

        # Identify partition leader for use in our metrics checks
        leader_node = cluster.get_node(
            next(rpk.describe_topic(topic.name)).leader)

        # We know timestamps, they are generated linearly from the
        # base we provided to kgo-verifier
        timestamps = dict((i, base_ts + i) for i in range(0, msg_count))
        for k, v in timestamps.items():
            self.logger.debug(f"  Offset {k} -> Timestamp {v}")

        # Class defining expectations of timequery results to be checked
        class ex:
            def __init__(self, offset, ts=None, expect_read=True):
                if ts is None:
                    ts = timestamps[offset]
                self.ts = ts
                self.offset = offset
                self.expect_read = expect_read

        # Selection of interesting cases
        expectations = [
            ex(0),  # First message
            ex(msg_count // 4),  # 25%th message
            ex(msg_count // 2),  # 50%th message
            ex(msg_count - 1),  # last message
            ex(0, timestamps[0] - 1000),
            ex(-1, timestamps[msg_count - 1] + 1000,
               False)  # After last message
        ]

        # For when using cloud storage, we expectr offsets ahead
        # of this to still hit raft for their timequeries.  This is approximate,
        # but fine as long as the test cases don't tread too near the gap.
        local_start_offset = msg_count - (
            (local_retain_segments * self.log_segment_size) / record_size)

        is_redpanda = isinstance(cluster, RedpandaService)

        # Remember which offsets we already hit, so that we can
        # make a good guess at whether subsequent hits on the same
        # offset should cause cloud downloads.
        hit_offsets = set()

        kcat = KafkaCat(cluster)
        for e in expectations:
            ts = e.ts
            o = e.offset

            if is_redpanda and cloud_storage:
                cloud_metrics = MetricCheck(
                    self.logger,
                    cluster,
                    leader_node,
                    re.compile("vectorized_cloud_storage_.*"),
                    reduce=sum)

            if is_redpanda and not cloud_storage:
                local_metrics = MetricCheck(
                    self.logger,
                    cluster,
                    leader_node,
                    re.compile("vectorized_storage_.*"),
                    reduce=sum)

            self.logger.info(
                f"Attempting time lookup ts={ts} (should be o={o})")
            offset = kcat.query_offset(topic.name, 0, ts)
            self.logger.info(f"Time query returned offset {offset}")
            assert offset == o

            if is_redpanda and cloud_storage and o < local_start_offset and o not in hit_offsets and e.expect_read:
                # We should have hydrated exactly one segment: this shows we are properly
                # looking up the segment and not e.g. seeking from the start.  We may
                cloud_metrics.expect([
                    ("vectorized_cloud_storage_successful_downloads_total",
                     lambda a, b: b == a + 1)
                ])

            if is_redpanda and not cloud_storage and not batch_cache and e.expect_read:
                # Expect to read at most one segment from disk: this validates that
                # we are correctly looking up the right segment before seeking to
                # the exact offset, and not e.g. reading from the start of the log.
                local_metrics.expect([
                    ("vectorized_storage_log_read_bytes_total",
                     lambda a, b: b > a and b - a < self.log_segment_size)
                ])

            hit_offsets.add(o)

        # TODO: do some double-restarts to generate segments with
        # no user data, to check that their timestamps don't
        # break out indexing.


class TimeQueryTest(RedpandaTest, BaseTimeQuery):
    # We use small segments to enable quickly exercising the
    # lookup of the proper segment for a time index, as well
    # as the lookup of the offset within that segment.
    log_segment_size = 1024 * 1024

    def setUp(self):
        # Don't start up redpanda yet, because we will need the
        # test parameter to set cluster configs before starting.
        pass

    @cluster(num_nodes=4)
    @parametrize(cloud_storage=True, batch_cache=False)
    @parametrize(cloud_storage=False, batch_cache=True)
    @parametrize(cloud_storage=False, batch_cache=False)
    def test_timequery(self, cloud_storage: bool, batch_cache: bool):
        self.redpanda.set_extra_rp_conf({
            # Testing with batch cache disabled is important, because otherwise
            # we won't touch the path in skipping_consumer that applies
            # timestamp bounds
            'disable_batch_cache': not batch_cache,

            # Our time bounds on segment removal depend on the leadership
            # staying in one place.
            'enable_leader_balancer': False,
        })

        if cloud_storage:
            si_settings = SISettings(
                cloud_storage_max_connections=5,
                log_segment_size=self.log_segment_size,

                # Off by default: test is parametrized to turn
                # on if SI is wanted.
                cloud_storage_enable_remote_read=True,
                cloud_storage_enable_remote_write=True,
            )
            self.redpanda.set_si_settings(si_settings)

        self.redpanda.start()

        return self._test_timequery(cluster=self.redpanda,
                                    cloud_storage=cloud_storage,
                                    batch_cache=batch_cache)


class TimeQueryKafkaTest(Test, BaseTimeQuery):
    """
    Time queries are one of the less clearly defined aspects of the
    Kafka protocol, so we run our test procedure against Apache Kafka
    to establish a baseline behavior to ensure our compatibility.
    """
    log_segment_size = 1024 * 1024

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.zk = ZookeeperService(self.test_context,
                                   num_nodes=1,
                                   version=V_3_0_0)

        self.kafka = KafkaServiceAdapter(
            self.test_context,
            KafkaService(self.test_context,
                         num_nodes=3,
                         zk=self.zk,
                         version=V_3_0_0))

        self._client = DefaultClient(self.kafka)

    def client(self):
        return self._client

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def tearDown(self):
        # ducktape handle service teardown automatically, but it is hard
        # to tell what went wrong if one of the services hangs.  Do it
        # explicitly here with some logging, to enable debugging issues
        # like https://github.com/redpanda-data/redpanda/issues/4270

        self.logger.info("Stopping Kafka...")
        self.kafka.stop()

        self.logger.info("Stopping zookeeper...")
        self.zk.stop()

    @ducktape_cluster(num_nodes=5)
    def test_timequery(self):
        self._test_timequery(cluster=self.kafka,
                             cloud_storage=False,
                             batch_cache=True)
