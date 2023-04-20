# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec

from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.kgo_repeater_service import repeater_traffic
from rptest.services.kgo_verifier_services import KgoVerifierRandomConsumer, KgoVerifierSeqConsumer, KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.redpanda import SISettings, CloudStorageType, get_cloud_storage_type
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.si_utils import BucketView
from rptest.util import expect_exception
from rptest.utils.mode_checks import skip_debug_mode


class OpenBenchmarkSelfTest(RedpandaTest):
    """
    This test verifies that OpenMessagingBenchmark service
    works as expected.  It is not a test of redpanda itself: this is
    to avoid committing changes that break services that might only
    be used in nightlies and not normally used in tests run on PRS.
    """
    BENCHMARK_WAIT_TIME_MIN = 5

    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)

    @cluster(num_nodes=6)
    @matrix(driver=["SIMPLE_DRIVER"], workload=["SIMPLE_WORKLOAD"])
    def test_default_omb_configuration(self, driver, workload):
        benchmark = OpenMessagingBenchmark(self.test_context, self.redpanda,
                                           driver, workload)
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + self.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        # docker runs have high variance in perf numbers, check only in dedicate node
        # setup.
        benchmark.check_succeed(validate_metrics=self.redpanda.dedicated_nodes)


class KgoRepeaterSelfTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)

    @cluster(num_nodes=4)
    def test_kgo_repeater(self):
        topic = 'test'
        self.client().create_topic(
            TopicSpec(name=topic,
                      partition_count=16,
                      retention_bytes=16 * 1024 * 1024,
                      segment_bytes=1024 * 1024))
        with repeater_traffic(context=self.test_context,
                              redpanda=self.redpanda,
                              topic=topic,
                              msg_size=4096,
                              workers=1) as repeater:
            repeater.await_group_ready()
            repeater.await_progress(1024, timeout_sec=75)


class KgoVerifierSelfTest(PreallocNodesTest):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context,
                         node_prealloc_count=1,
                         *args,
                         **kwargs)

    @cluster(num_nodes=4)
    def test_kgo_verifier(self):
        topic = 'test'
        self.client().create_topic(
            TopicSpec(name=topic,
                      partition_count=16,
                      retention_bytes=16 * 1024 * 1024,
                      segment_bytes=1024 * 1024))

        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic,
                                       16384,
                                       1000,
                                       custom_node=self.preallocated_nodes,
                                       debug_logs=True)
        producer.start()
        producer.wait_for_acks(1000, timeout_sec=30, backoff_sec=1)
        producer.wait_for_offset_map()

        rand_consumer = KgoVerifierRandomConsumer(
            self.test_context,
            self.redpanda,
            topic,
            16384,
            100,
            2,
            nodes=self.preallocated_nodes,
            debug_logs=True,
            trace_logs=True)
        rand_consumer.start(clean=False)

        seq_consumer = KgoVerifierSeqConsumer(self.test_context,
                                              self.redpanda,
                                              topic,
                                              16384,
                                              nodes=self.preallocated_nodes,
                                              debug_logs=True,
                                              trace_logs=True)
        seq_consumer.start(clean=False)

        group_consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topic,
            16384,
            2,
            nodes=self.preallocated_nodes,
            debug_logs=True,
            trace_logs=True)
        group_consumer.start(clean=False)

        producer.wait(timeout_sec=60)
        rand_consumer.wait(timeout_sec=60)
        group_consumer.wait(timeout_sec=60)
        seq_consumer.wait(timeout_sec=60)


class BucketScrubSelfTest(RedpandaTest):
    """
    Verify that if we erase an object from tiered storage,
    the bucket validation will fail.
    """
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context,
                         *args,
                         num_brokers=3,
                         si_settings=SISettings(test_context),
                         **kwargs)

    @skip_debug_mode  # We wait for a decent amount of traffic
    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_missing_segment(self, cloud_storage_type):
        topic = 'test'

        partition_count = 16
        segment_size = 1024 * 1024
        msg_size = 16384

        self.client().create_topic(
            TopicSpec(name=topic,
                      partition_count=partition_count,
                      retention_bytes=16 * segment_size,
                      segment_bytes=segment_size))

        total_write_bytes = segment_size * partition_count * 4

        with repeater_traffic(context=self.test_context,
                              redpanda=self.redpanda,
                              topic=topic,
                              msg_size=msg_size,
                              workers=1) as repeater:
            repeater.await_group_ready()
            repeater.await_progress(total_write_bytes // msg_size,
                                    timeout_sec=120)

        def all_partitions_have_segments():
            view = BucketView(self.redpanda)
            for p in range(0, partition_count):
                if view.cloud_log_segment_count_for_ntp(topic, p) == 0:
                    return False

            return True

        self.redpanda.wait_until(all_partitions_have_segments,
                                 timeout_sec=60,
                                 backoff_sec=5)

        # Initially a bucket scrub should pass
        self.logger.info(f"Running baseline scrub")
        self.redpanda.stop_and_scrub_object_storage()
        self.redpanda._for_nodes(
            self.redpanda.nodes,
            lambda n: self.redpanda.start_node(n, first_start=False),
            parallel=True)

        # Go delete a segment: pick one arbitrarily, but it must be one
        # that is linked into a manifest to constitute a corruption.
        view = BucketView(self.redpanda)
        segment_key = None
        for o in self.redpanda.cloud_storage_client.list_objects(
                self.si_settings.cloud_storage_bucket):
            if ".log" in o.key and view.is_segment_part_of_a_manifest(o):
                segment_key = o.key
                break

        assert segment_key is not None

        tmp_location = f"tmp_{segment_key}_tmp"
        self.logger.info(f"Simulating loss of segment {segment_key}")
        self.redpanda.cloud_storage_client.move_object(
            self.si_settings.cloud_storage_bucket,
            segment_key,
            tmp_location,
            validate=True)

        self.logger.info(f"Running scrub that should discover issue")
        with expect_exception(RuntimeError, lambda e: "fatal" in str(e)):
            self.redpanda.stop_and_scrub_object_storage()

        # Avoid tripping exception during shutdown: reinstate the object
        self.redpanda.cloud_storage_client.move_object(
            self.si_settings.cloud_storage_bucket,
            tmp_location,
            segment_key,
            validate=True)

        self.redpanda._for_nodes(
            self.redpanda.nodes,
            lambda n: self.redpanda.start_node(n, first_start=False),
            parallel=True)
