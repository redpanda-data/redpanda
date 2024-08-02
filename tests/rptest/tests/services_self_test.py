# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from subprocess import CalledProcessError
from ducktape.mark import matrix
from ducktape.mark.resource import cluster as dt_cluster
from ducktape.tests.test import Test

from rptest.tests.redpanda_test import RedpandaMixedTest, RedpandaTest
from rptest.services.cluster import cluster
from rptest.clients.kubectl import is_redpanda_pod, SUPPORTED_PROVIDERS
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.failure_injector import FailureSpec, make_failure_injector
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.kgo_repeater_service import repeater_traffic
from rptest.services.kgo_verifier_services import KgoVerifierRandomConsumer, KgoVerifierSeqConsumer, KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.redpanda import RedpandaServiceCloud, SISettings, CloudStorageType, get_cloud_storage_type, make_redpanda_service, make_redpanda_mixed_service
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.si_utils import BucketView
from rptest.util import expect_exception
from rptest.utils.mode_checks import skip_debug_mode
from rptest.services.producer_swarm import ProducerSwarm


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

    @skip_debug_mode  # Sends meaningful traffic, and not intended to test Redpanda
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


class ProducerSwarmSelfTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=3, **kwargs)

    @skip_debug_mode  # Sends meaningful traffic, and not intended to test Redpanda
    @cluster(num_nodes=4)
    def test_producer_swarm(self):
        spec = TopicSpec(name="test_topic",
                         partition_count=10,
                         replication_factor=3)
        self.client().create_topic(spec)
        topic_name = spec.name

        producer = ProducerSwarm(self.test_context,
                                 self.redpanda,
                                 topic_name,
                                 producers=10,
                                 records_per_producer=500,
                                 messages_per_second_per_producer=10)
        producer.start()
        producer.await_progress(target_msg_rate=8, timeout_sec=40)
        producer.wait()
        producer.stop()

    @cluster(num_nodes=4)
    def test_wait_start_stop(self):
        spec = TopicSpec(partition_count=10, replication_factor=1)
        self.client().create_topic(spec)
        topic_name = spec.name

        producer = ProducerSwarm(self.test_context,
                                 self.redpanda,
                                 topic_name,
                                 producers=10,
                                 records_per_producer=500,
                                 messages_per_second_per_producer=10)

        producer.start()
        assert producer.is_alive()
        producer.wait_for_all_started()
        producer.stop()

        assert not producer.is_alive()

        # check that a subsequent start/stop works
        producer.start()
        assert producer.is_alive()
        producer.wait_for_all_started()
        producer.stop()


class KgoRepeaterSelfTest(RedpandaMixedTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, min_brokers=3, **kwargs)

    @skip_debug_mode  # Sends meaningful traffic, and not intended to test Redpanda
    @cluster(num_nodes=5)
    def test_kgo_repeater(self):
        topic = 'test'
        self.client().create_topic(
            TopicSpec(name=topic,
                      partition_count=16,
                      retention_bytes=16 * 1024 * 1024,
                      segment_bytes=1024 * 1024))
        with repeater_traffic(context=self.test_context,
                              redpanda=self.redpanda,
                              num_nodes=2,
                              topics=[topic],
                              msg_size=4096,
                              workers=1) as repeater:
            repeater.await_group_ready()
            repeater.await_progress(1024, timeout_sec=75)

        # Assert clean service stop.
        for service in self.test_context.services:
            for node in service.nodes:
                cmd = """ps ax | grep -i kgo-repeater | grep -v grep | awk '{print $1}'"""
                pids = [
                    pid
                    for pid in node.account.ssh_capture(cmd, allow_fail=True)
                ]
                self.logger.debug(f"Running kgo-repeater: {pids}")
                assert len(pids) == 0


class KgoVerifierSelfTest(PreallocNodesTest):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context,
                         node_prealloc_count=1,
                         *args,
                         **kwargs)

    @skip_debug_mode  # Sends meaningful traffic, and not intended to test Redpanda
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
    #@matrix(cloud_storage_type=get_cloud_storage_type())
    @matrix(cloud_storage_type=get_cloud_storage_type(
        applies_only_on=[CloudStorageType.S3]))
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
                              topics=[topic],
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
        self.redpanda.for_nodes(
            self.redpanda.nodes,
            lambda n: self.redpanda.start_node(n, first_start=False))

        # Go delete a segment: pick one arbitrarily, but it must be one
        # that is linked into a manifest to constitute a corruption.
        view = BucketView(self.redpanda)
        segment_key = None
        for o in self.redpanda.cloud_storage_client.list_objects(
                self.si_settings.cloud_storage_bucket):
            if ".log" in o.key and view.find_segment_in_manifests(o):
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

        self.redpanda.for_nodes(
            self.redpanda.nodes,
            lambda n: self.redpanda.start_node(n, first_start=False))


class SimpleSelfTest(Test):
    """
    Verify instantiation of a RedpandaServiceBase subclass through the factory method.

    Runs a few methods of RedpandaServiceBase.
    """
    def __init__(self, test_context):
        super(SimpleSelfTest, self).__init__(test_context)
        self.redpanda = make_redpanda_mixed_service(test_context,
                                                    min_brokers=3)

    def setUp(self):
        self.redpanda.start()

    @cluster(num_nodes=3, check_allowed_error_logs=False)
    def test_cloud(self):
        """
        Execute a few of the methods that will connect to the k8s pod.
        """

        node_memory = float(self.redpanda.get_node_memory_mb())
        assert node_memory > 1.0

        node_cpu_count = self.redpanda.get_node_cpu_count()
        assert node_cpu_count > 0

        node_disk_free = self.redpanda.get_node_disk_free()
        assert node_disk_free > 0

        rpk = RpkTool(self.redpanda)
        topic_name = 'rp_ducktape_test_cloud_topic'
        self.logger.info(f'creating topic {topic_name}')
        rpk.create_topic(topic_name)
        self.logger.info(f'deleting topic {topic_name}')
        rpk.delete_topic(topic_name)


class KubectlSelfTest(Test):
    """
    Verify that the kubectl test works. Only does anything when running
    in the cloud.
    """
    def __init__(self, test_context):
        super().__init__(test_context)
        self.redpanda = make_redpanda_mixed_service(test_context)

    def setUp(self):
        self.redpanda.start()

    @cluster(num_nodes=3)
    def test_kubectl_tool(self):
        rp = self.redpanda

        if isinstance(rp, RedpandaServiceCloud):
            version_out = rp.kubectl.cmd(['version', '--client'])
            assert 'Client Version' in version_out, f'Did not find expceted output, output was: {version_out}'

            try:
                # foobar, of course, is not a valid kubectl command
                rp.kubectl.cmd(['foobar'])
                assert False, 'expected this command to throw'
            except CalledProcessError:
                pass


class KubectlLocalOnlyTest(Test):
    @dt_cluster(num_nodes=0)
    def test_is_redpanda_pod(self):
        test_cases = {
            "regular_hit": {
                "pod": {
                    "metadata": {
                        "generateName": "redpanda-broker-",
                        "labels": {
                            "app.kubernetes.io/component":
                            "redpanda-statefulset"
                        }
                    }
                },
                "result": True
            },
            "miss_wrong_generate_name": {
                "pod": {
                    "metadata": {
                        "generateName": "redpanda-broker-configuration-",
                        "labels": {
                            "app.kubernetes.io/component":
                            "redpanda-statefulset"
                        }
                    }
                },
                "result": False
            },
            "miss_wrong_component": {
                "pod": {
                    "metadata": {
                        "generateName": "redpanda-broker-configuration-",
                        "labels": {
                            "app.kubernetes.io/component":
                            "redpanda-post-install"
                        }
                    },
                },
                "result": False
            },
            "hit_legacy_pod_name": {
                "pod": {
                    "metadata": {
                        "name": "rp-CLUSTER_ID-4"
                    }
                },
                "result": True
            },
        }
        for test_name, test_case in test_cases.items():
            pod_obj = test_case["pod"]
            expected_result = test_case["result"]
            try:
                actual_result = is_redpanda_pod(pod_obj, "CLUSTER_ID")
            except KeyError as err:
                self.logger.error(f"KeyError from is_redpanda_pod: {err}")
                actual_result = False
            assert expected_result is actual_result, f"Failed for test case '{test_name}' (expected={expected_result}, actual={actual_result})"


class FailureInjectorSelfTest(Test):
    """
    Verify instantiation of a FailureInjectorBase subclass through the factory method.
    """
    def __init__(self, test_context):
        super(FailureInjectorSelfTest, self).__init__(test_context)
        self.redpanda = make_redpanda_service(test_context, 3)

    def setUp(self):
        self.redpanda.start()

    @cluster(num_nodes=3, check_allowed_error_logs=False)
    def test_finjector(self):
        fi = make_failure_injector(self.redpanda)
        fi.inject_failure(FailureSpec(FailureSpec.FAILURE_ISOLATE, None))
