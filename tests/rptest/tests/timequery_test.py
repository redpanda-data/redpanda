# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.rpk_remote import RpkRemoteTool

from rptest.services.kgo_verifier_services import KgoVerifierProducer

from ducktape.mark import parametrize

from rptest.services.kafka import KafkaServiceAdapter
from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService
from ducktape.mark.resource import cluster as ducktape_cluster
from kafkatest.version import V_3_0_0
from ducktape.tests.test import Test
from rptest.clients.default import DefaultClient
from rptest.utils.mode_checks import skip_debug_mode


class BaseTimeQuery:
    def _test_timequery(self):
        total_segments = 12
        record_size = 1024

        topic = TopicSpec(name="tqtopic",
                          partition_count=1,
                          replication_factor=3)

        self.client().create_topic(topic)

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
        producer = KgoVerifierProducer(
            context=self.test_context,
            redpanda=self.redpanda,
            topic=topic.name,
            msg_size=record_size,
            msg_count=msg_count,

            # A respectable number of messages per batch so that we are covering
            # the case of looking up timestamps that fall in the middle of a batch,
            # but small enough that we are getting plenty of distinct batches.
            batch_max_bytes=record_size * 10,

            # A totally arbitrary artificial timestamp base in milliseconds
            fake_timestamp_ms=1664453149000)
        producer.start()
        producer.wait()

        # Confirm messages written
        rpk = RpkTool(self.redpanda)
        p = next(rpk.describe_topic(topic.name))
        assert p.high_watermark == msg_count

        # Read back timestamps
        self.logger.info(f"Retrieving timestamps for {msg_count} messages")
        rpk_remote = RpkRemoteTool(self.redpanda, self.redpanda.nodes[0])
        timestamps = dict(
            rpk_remote.read_timestamps(topic.name,
                                       0,
                                       msg_count,
                                       timeout_sec=30))

        for k, v in timestamps.items():
            self.logger.debug(f"  Offset {k} -> Timestamp {v}")

        # Interesting cases
        offsets = [0, msg_count // 4, msg_count // 2, msg_count - 1]

        kcat = KafkaCat(self.redpanda)
        for o in offsets:
            # read_timestamps gives values in millis
            ts = int(timestamps[o])

            self.logger.info(
                f"Attempting time lookup ts={ts} (should be o={o})")
            offset = kcat.query_offset(topic.name, 0, ts)
            self.logger.info(f"Time query returned offset {offset}")
            assert offset == o

            self.logger.info(
                f"Attempting time-based reader lookup ts={ts} (should be o={o})"
            )
            record = kcat.consume_one(topic.name, 0, first_timestamp=ts)
            self.logger.info(f"Time-based consumer returned record {record}")
            assert record['offset'] == o


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
    @parametrize(batch_cache=True)
    @parametrize(batch_cache=False)
    @skip_debug_mode
    def test_timequery(self, batch_cache: bool):
        self.redpanda.set_extra_rp_conf({
            # Testing with batch cache disabled is important, because otherwise
            # we won't touch the path in skipping_consumer that applies
            # timestamp bounds
            'disable_batch_cache': not batch_cache,
        })

        self.redpanda.start()

        return self._test_timequery()


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

    @property
    def redpanda(self):
        return self.kafka

    def setUp(self):
        self.zk.start()
        self.kafka.start()
        time.sleep(5)

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
        self._test_timequery()
