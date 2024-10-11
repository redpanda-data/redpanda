# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import enum

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import ResourceSettings
from rptest.services.cluster import cluster
from rptest.services.rpk_consumer import RpkConsumer

from rptest.services.producer_swarm import ProducerSwarm
from ducktape.mark import matrix


class CompactionMode(str, enum.Enum):
    NONE = "none"
    REALISTIC = "realistic"
    PATHOLOGICAL = "pathological"


class ManyClientsTest(RedpandaTest):
    PRODUCER_COUNT = 4000

    # Our workload generates tiny I/Os, so we anticipate being IOP-bound.
    # For an i3en.xlarge that can do about 65,000 IOPs, and considering 3x
    # replication, our per-node throughput would be be about 1/3 of that, if
    # we were perfectly IOP-bound.
    TARGET_THROUGHPUT_MB_S_PER_NODE = 40

    def __init__(self, *args, **kwargs):
        # We will send huge numbers of messages, so tune down the log verbosity
        # as this is just a "did we stay up?" test
        kwargs['log_level'] = "info"

        kwargs['extra_rp_conf'] = {
            # Enable segment size jitter as this is a stress test and does not
            # rely on exact segment counts.
            'log_segment_size_jitter_percent':
            5,

            # This limit caps the produce throughput to a sustainable rate for a RP
            # cluster that has 384MB of memory per shard. It is set here to
            # since our current backpressure mechanisms will allow producers to
            # produce at a much higher rate and cause RP to run out of memory.
            'kafka_throughput_limit_node_in_bps':
            self.TARGET_THROUGHPUT_MB_S_PER_NODE * 1024 *
            1024,  # 100MiB/s per node

            # Set higher connection count limits than the redpanda default.
            # Factor of 4: allow each client 3 connections (producer,consumer,admin), plus
            # 1 connection to accomodate reconnects while a previous connection is
            # still live.
            'kafka_connections_max':
            self.PRODUCER_COUNT * 4,
            'kafka_connections_max_per_ip':
            self.PRODUCER_COUNT * 4,
        }
        super().__init__(*args, **kwargs)

    def setUp(self):
        # Delay starting Redpanda, we will customize ResourceSettings inside the test case
        pass

    @cluster(num_nodes=7)
    def test_many_clients_no_compaction(self):
        self._test_many_clients(compaction_mode=CompactionMode.NONE)

    @cluster(num_nodes=7)
    def test_many_clients_realistic_compaction(self):
        self._test_many_clients(compaction_mode=CompactionMode.REALISTIC)

    @cluster(num_nodes=7)
    def test_many_clients_pathological_compaction(self):
        self._test_many_clients(compaction_mode=CompactionMode.PATHOLOGICAL)

    def _test_many_clients(self, compaction_mode):
        """
        Check that redpanda remains stable under higher numbers of clients
        than usual.
        """

        # This test won't work on a debug build, even if you're just testing on
        # a workstation.
        assert not self.debug_mode

        # 20MiB is the largest vector of zeros we can use while keeping
        # the compressed size <1MiB.  snappy is the worst algo for simple arrays
        # of zeros.
        pathological_record_size_mb = 20
        if compaction_mode == CompactionMode.PATHOLOGICAL:
            compacted_record_size_max_mb = pathological_record_size_mb
        else:
            compacted_record_size_max_mb = 1

        num_cpus = 2

        # Compacted topics using compression have a higher
        # memory footprint: they may decompress/compress up to two batches
        # per shard concurrently (one for index updates on the produce path,
        # one for housekeeping)
        memory_mb = 768 if compaction_mode == CompactionMode.NONE else 768 + compacted_record_size_max_mb * 2 * num_cpus

        resource_settings = ResourceSettings(
            num_cpus=num_cpus,
            # Set a low memory size, such that the amount of available memory per client
            # is small (~100k)
            memory_mb=memory_mb)

        # Load the resource settings and start Redpanda
        self.redpanda.set_resource_settings(resource_settings)
        super().setUp()

        partition_count = 100
        producer_count = self.PRODUCER_COUNT

        PRODUCER_TIMEOUT_MS = 5000
        TOPIC_NAME = "manyclients"

        # Realistic conditions: 128MB is the segment size in the cloud
        segment_size = 128 * 1024 * 1024
        retention_size = 8 * segment_size

        cleanup_policy = "compact" if compaction_mode != CompactionMode.NONE else "delete"

        self.client().create_topic(
            TopicSpec(name=TOPIC_NAME,
                      partition_count=partition_count,
                      retention_bytes=retention_size,
                      segment_bytes=segment_size,
                      cleanup_policy=cleanup_policy))

        # Three consumers, just so that we are at least touching consumer
        # group functionality, if not stressing the overall number of consumers.
        # Need enough consumers to grab data before it gets cleaned up by the
        # retention policy
        consumer_a = RpkConsumer(self.test_context,
                                 self.redpanda,
                                 TOPIC_NAME,
                                 group="testgroup",
                                 save_msgs=False)
        consumer_b = RpkConsumer(self.test_context,
                                 self.redpanda,
                                 TOPIC_NAME,
                                 group="testgroup",
                                 save_msgs=False)
        consumer_c = RpkConsumer(self.test_context,
                                 self.redpanda,
                                 TOPIC_NAME,
                                 group="testgroup",
                                 save_msgs=False)

        key_space = 10

        target_throughput_mb_s = self.TARGET_THROUGHPUT_MB_S_PER_NODE * len(
            self.redpanda.nodes)

        producer_kwargs = {}
        if compaction_mode == CompactionMode.NONE:
            producer_kwargs['min_record_size'] = 0
            producer_kwargs['max_record_size'] = 16384

            effective_msg_size = producer_kwargs['min_record_size'] + (
                producer_kwargs['max_record_size'] -
                producer_kwargs['min_record_size']) // 2
        else:
            # Compaction is much more stressful when the clients sends compacted
            # data, because the server must decompress it to index it.
            producer_kwargs['compression_type'] = 'mixed'

            if compaction_mode == CompactionMode.PATHOLOGICAL:
                # Use large compressible payloads, to stress memory consumption: a
                # compressed batch will be accepted by the Kafka API, but
                producer_kwargs['compressible_payload'] = True
                producer_kwargs['min_record_size'] = 16 * 1024 * 1024
                producer_kwargs[
                    'max_record_size'] = pathological_record_size_mb * 1024 * 1024

                # Actual messages are tiny.  Use a heuristic to approximate what kind
                # of throughput one of these "big batch of zeros" messages would
                # correspond to if it was a regular batch.
                effective_msg_size = 256000
            else:
                # Regular data that just happens to be compressed: we take the CPU
                # hit but do not have batches that blow up into huge memory.
                producer_kwargs['min_record_size'] = 0
                producer_kwargs['max_record_size'] = 16384

                effective_msg_size = producer_kwargs['min_record_size'] + (
                    producer_kwargs['max_record_size'] -
                    producer_kwargs['min_record_size']) // 2

            producer_kwargs['keys'] = key_space

            # Clients have to do the compression work on these larger messages,
            # so curb our expectations about how many we may run concurrently.
            producer_count = producer_count // 10

        self.logger.info(
            f"Using mean message size {effective_msg_size} ({producer_kwargs['min_record_size']}-{producer_kwargs['max_record_size']})"
        )

        msg_rate = (target_throughput_mb_s * 1024 * 1024) // effective_msg_size
        messages_per_sec_per_producer = msg_rate // producer_count
        producer_kwargs[
            'messages_per_second_per_producer'] = messages_per_sec_per_producer

        # If this fails, the test was altered to have an impractical ratio of
        # producers to traffic rate.
        assert messages_per_sec_per_producer > 0, "Bad sizing params, need at least 1 MPS"

        target_runtime_s = 30
        records_per_producer = messages_per_sec_per_producer * target_runtime_s

        self.logger.info(
            f"compaction={compaction_mode} mode, {producer_count} producers writing {messages_per_sec_per_producer} msg/s each, {records_per_producer} records each"
        )

        producer = ProducerSwarm(self.test_context,
                                 self.redpanda,
                                 TOPIC_NAME,
                                 producer_count,
                                 records_per_producer,
                                 timeout_ms=PRODUCER_TIMEOUT_MS,
                                 **producer_kwargs)
        producer.start()
        consumer_a.start()
        consumer_b.start()
        consumer_c.start()

        producer.wait()

        expect = producer_count * records_per_producer
        if compaction_mode != CompactionMode.NONE:
            # When using compaction, we may well not see all the original messages, as
            # they could have been compacted away before we read them.
            expect = min(partition_count * key_space, expect)

        def complete():
            self.logger.info(
                f"Message counts: {consumer_a.message_count} {consumer_b.message_count} {consumer_c.message_count} (vs {expect})"
            )
            return consumer_a.message_count + consumer_b.message_count + consumer_c.message_count >= expect

        self.redpanda.wait_until(complete,
                                 timeout_sec=30,
                                 backoff_sec=1,
                                 err_msg="Consumers didn't see all messages")
