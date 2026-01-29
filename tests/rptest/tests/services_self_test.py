# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from contextlib import contextmanager
import random
import signal
from subprocess import CalledProcessError
from typing import Any, Callable, Iterator, cast
import time

from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark import matrix, ignore
from ducktape.mark.resource import cluster as dt_cluster
from ducktape.tests.test import Test, TestContext

from rptest.clients.admin.v2 import Admin as AdminV2, debug_pb
from rptest.clients.kubectl import is_redpanda_pod
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import CrashType
from rptest.services.cluster import cluster
from rptest.services.failure_injector import FailureSpec, make_failure_injector
from rptest.services.kgo_repeater_service import repeater_traffic
from rptest.services.kgo_verifier_services import (
    KgoVerifierParams,
    KgoVerifierConsumerGroupConsumer,
    KgoVerifierMultiProducer,
    KgoVerifierMultiConsumerGroupConsumer,
    KgoVerifierMultiRandomConsumer,
    KgoVerifierMultiSeqConsumer,
    KgoVerifierProducer,
    KgoVerifierRandomConsumer,
    KgoVerifierSeqConsumer,
)
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from rptest.services.producer_swarm import ProducerSwarm
from rptest.services.redpanda import (
    CloudStorageType,
    LogSearchLocal,
    LoggingConfig,
    RedpandaService,
    RedpandaServiceCloud,
    SISettings,
    get_cloud_storage_type,
    make_redpanda_mixed_service,
    make_redpanda_service,
)
from rptest.services.utils import BadLogLines, NodeCrash
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception
from rptest.utils.mode_checks import (
    ignore_if_not_asan,
    ignore_if_not_debug,
    ignore_if_not_ubsan,
    skip_debug_mode,
)
from rptest.utils.si_utils import BucketView


class OpenBenchmarkSelfTest(RedpandaTest):
    """
    This test verifies that OpenMessagingBenchmark service
    works as expected.  It is not a test of redpanda itself: this is
    to avoid committing changes that break services that might only
    be used in nightlies and not normally used in tests run on PRS.
    """

    BENCHMARK_WAIT_TIME_MIN = 5

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, num_brokers=3, **kwargs)

    @skip_debug_mode  # Sends meaningful traffic, and not intended to test Redpanda
    @cluster(num_nodes=6)
    @matrix(driver=["SIMPLE_DRIVER"], workload=["SIMPLE_WORKLOAD"])
    def test_default_omb_configuration(self, driver: str, workload: str) -> None:
        benchmark = OpenMessagingBenchmark(
            self.test_context, self.redpanda, driver, workload
        )
        benchmark.start()
        benchmark_time_min = (
            benchmark.benchmark_time_mins() + self.BENCHMARK_WAIT_TIME_MIN
        )
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        # docker runs have high variance in perf numbers, check only in dedicate node
        # setup.
        benchmark.check_succeed(validate_metrics=self.redpanda.dedicated_nodes)


class ProducerSwarmSelfTest(RedpandaTest):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, num_brokers=3, **kwargs)

    @skip_debug_mode  # Sends meaningful traffic, and not intended to test Redpanda
    @cluster(num_nodes=4)
    def test_producer_swarm(self) -> None:
        spec = TopicSpec(name="test_topic", partition_count=10, replication_factor=3)
        self.client().create_topic(spec)
        topic_name = spec.name

        producer = ProducerSwarm(
            self.test_context,
            self.redpanda,
            topic_name,
            producers=10,
            records_per_producer=500,
            messages_per_second_per_producer=10,
        )
        producer.start()
        producer.await_progress(target_msg_rate=8, timeout_sec=40)
        producer.wait()
        producer.stop()

    @cluster(num_nodes=4)
    def test_wait_start_stop(self) -> None:
        spec = TopicSpec(partition_count=10, replication_factor=1)
        self.client().create_topic(spec)
        topic_name = spec.name

        producer = ProducerSwarm(
            self.test_context,
            self.redpanda,
            topic_name,
            producers=10,
            records_per_producer=500,
            messages_per_second_per_producer=10,
        )

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


class KgoRepeaterSelfTest(RedpandaTest):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, num_brokers=3, **kwargs)

    @skip_debug_mode  # Sends meaningful traffic, and not intended to test Redpanda
    @cluster(num_nodes=5)
    def test_kgo_repeater(self) -> None:
        topic = "test"
        self.client().create_topic(
            TopicSpec(
                name=topic,
                partition_count=16,
                retention_bytes=16 * 1024 * 1024,
                segment_bytes=1024 * 1024,
            )
        )
        with repeater_traffic(
            context=self.test_context,
            redpanda=self.redpanda,
            num_nodes=2,
            topics=[topic],
            msg_size=4096,
            workers=1,
        ) as repeater:
            repeater.await_group_ready()
            repeater.await_progress(1024, timeout_sec=75)

        # Assert clean service stop.
        for service in self.test_context.services:
            for node in service.nodes:
                cmd = (
                    """ps ax | grep -i kgo-repeater | grep -v grep | awk '{print $1}'"""
                )
                pids = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True)]
                self.logger.debug(f"Running kgo-repeater: {pids}")
                assert len(pids) == 0


class KgoVerifierSelfTest(PreallocNodesTest):
    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any) -> None:
        super().__init__(
            test_context=test_context, node_prealloc_count=1, *args, **kwargs
        )

    @skip_debug_mode  # Sends meaningful traffic, and not intended to test Redpanda
    @cluster(num_nodes=4)
    def test_kgo_verifier(self) -> None:
        topic = "test"
        self.client().create_topic(
            TopicSpec(
                name=topic,
                partition_count=16,
                retention_bytes=16 * 1024 * 1024,
                segment_bytes=1024 * 1024,
            )
        )

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic,
            16384,
            1000,
            custom_node=self.preallocated_nodes,
            debug_logs=True,
        )
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
            trace_logs=True,
        )
        rand_consumer.start(clean=False)

        seq_consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            topic,
            16384,
            nodes=self.preallocated_nodes,
            debug_logs=True,
            trace_logs=True,
        )
        seq_consumer.start(clean=False)

        group_consumer = KgoVerifierConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topic,
            16384,
            2,
            nodes=self.preallocated_nodes,
            debug_logs=True,
            trace_logs=True,
        )
        group_consumer.start(clean=False)

        producer.wait(timeout_sec=60)
        rand_consumer.wait(timeout_sec=60)
        group_consumer.wait(timeout_sec=60)
        seq_consumer.wait(timeout_sec=60)

    @skip_debug_mode  # Sends meaningful traffic, and not intended to test Redpanda
    @cluster(num_nodes=4)
    def test_kgo_verifier_multi(self):
        topics = [
            KgoVerifierParams(
                TopicSpec(
                    name=n,
                    partition_count=16,
                    retention_bytes=16 * 1024 * 1024,
                    segment_bytes=1024 * 1024,
                ),
                msg_size=random.randint(2**13, 2**14),
                msg_count=random.randint(800, 1200),
                group_name=f"group-{n}",
            )
            for n in [
                "test-1",
                "test-2",
                "test-3",
            ]
        ]

        for topic in topics:
            self.client().create_topic(cast(TopicSpec, topic.topic))

        producer = KgoVerifierMultiProducer(
            self.test_context,
            self.redpanda,
            topics,
            custom_node=self.preallocated_nodes,
            debug_logs=True,
        )

        producer.start()
        producer.wait_for_acks(
            [t.msg_count for t in topics],
            timeout_sec=30,
            backoff_sec=1,
        )
        producer.wait_for_offset_map()

        seq_consumer = KgoVerifierMultiSeqConsumer(
            self.test_context,
            self.redpanda,
            topics,
            producer=producer,
            custom_node=self.preallocated_nodes,
            debug_logs=True,
            trace_logs=True,
        )
        seq_consumer.start(clean=False)

        rand_consumer = KgoVerifierMultiRandomConsumer(
            self.test_context,
            self.redpanda,
            topics,
            100,
            2,  # parallel
            custom_node=self.preallocated_nodes,
            debug_logs=True,
            trace_logs=True,
        )
        rand_consumer.start(clean=False)

        group_consumer = KgoVerifierMultiConsumerGroupConsumer(
            self.test_context,
            self.redpanda,
            topics,
            2,  # readers
            custom_node=self.preallocated_nodes,
            debug_logs=True,
            trace_logs=True,
        )
        group_consumer.start(clean=False)

        producer.wait(timeout_sec=60)
        seq_consumer.wait(timeout_sec=60)
        rand_consumer.wait(timeout_sec=60)
        group_consumer.wait(timeout_sec=60)


class KgoVerifierMultiNodeSelfTest(PreallocNodesTest):
    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any) -> None:
        super().__init__(
            test_context=test_context, node_prealloc_count=2, *args, **kwargs
        )

    @skip_debug_mode
    @cluster(num_nodes=5)
    def test_kgo_verifier_multi_node(self) -> None:
        """
        Test KgoVerifierMulti* with multiple preallocated nodes, including an explicit
        node assignment with each KgoVerifierParams.
        """
        topics = [
            KgoVerifierParams(
                TopicSpec(
                    name=t,
                    partition_count=16,
                    retention_bytes=16 * 1024 * 1024,
                    segment_bytes=1024 * 1024,
                ),
                msg_size=random.randint(2**13, 2**14),
                msg_count=random.randint(800, 1200),
                node=n,
                group_name=f"group-{t}",
            )
            for t, n in zip(
                [
                    "test-1",
                    "test-2",
                ],
                self.preallocated_nodes,
            )
        ]

        for topic in topics:
            self.client().create_topic(cast(TopicSpec, topic.topic))

        producer = KgoVerifierMultiProducer(
            self.test_context,
            self.redpanda,
            topics,
            custom_node=self.preallocated_nodes,
            debug_logs=True,
        )
        producer.start()
        producer.wait_for_acks(
            [t.msg_count for t in topics],
            timeout_sec=30,
            backoff_sec=1,
        )
        producer.wait_for_offset_map()

        seq_consumer = KgoVerifierMultiSeqConsumer(
            self.test_context,
            self.redpanda,
            topics,
            producer=producer,
            custom_node=self.preallocated_nodes,
            debug_logs=True,
            trace_logs=True,
        )
        seq_consumer.start(clean=False)

        producer.wait(timeout_sec=60)
        seq_consumer.wait(timeout_sec=60)

    @skip_debug_mode
    @cluster(num_nodes=5)
    def test_kgo_verifier_multi_node_autoassign(self) -> None:
        """
        Test KgoVerifierMulti* with multiple preallocated nodes, omitting the explicit
        node assignments. KgoVerifierMultiService should assign each producer or consumer
        to exactly one of the preallocated nodes in a round robin fashion.
        """
        topics = [
            KgoVerifierParams(
                TopicSpec(
                    name=t,
                    partition_count=16,
                    retention_bytes=16 * 1024 * 1024,
                    segment_bytes=1024 * 1024,
                ),
                msg_size=random.randint(2**13, 2**14),
                msg_count=random.randint(800, 1200),
                group_name=f"group-{t}",
            )
            for t in [
                "test-1",
                "test-2",
                "test-3",
            ]
        ]

        for topic in topics:
            self.client().create_topic(cast(TopicSpec, topic.topic))

        producer = KgoVerifierMultiProducer(
            self.test_context,
            self.redpanda,
            topics,
            custom_node=self.preallocated_nodes,
            debug_logs=True,
        )
        producer.start()
        producer.wait_for_acks(
            [t.msg_count for t in topics],
            timeout_sec=30,
            backoff_sec=1,
        )
        producer.wait_for_offset_map()

        seq_consumer = KgoVerifierMultiSeqConsumer(
            self.test_context,
            self.redpanda,
            topics,
            producer=producer,
            custom_node=self.preallocated_nodes,
            debug_logs=True,
            trace_logs=True,
        )
        seq_consumer.start(clean=False)

        producer.wait(timeout_sec=60)
        seq_consumer.wait(timeout_sec=60)


class BucketScrubSelfTest(RedpandaTest):
    """
    Verify that if we erase an object from tiered storage,
    the bucket validation will fail.
    """

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any) -> None:
        super().__init__(
            test_context,
            *args,
            num_brokers=3,
            si_settings=SISettings(test_context),
            **kwargs,
        )

    @skip_debug_mode  # We wait for a decent amount of traffic
    @cluster(num_nodes=4)
    # @matrix(cloud_storage_type=get_cloud_storage_type())
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_missing_segment(self, cloud_storage_type: CloudStorageType) -> None:
        topic = "test"

        partition_count = 16
        segment_size = 1024 * 1024
        msg_size = 16384

        self.client().create_topic(
            TopicSpec(
                name=topic,
                partition_count=partition_count,
                retention_bytes=16 * segment_size,
                segment_bytes=segment_size,
            )
        )

        total_write_bytes = segment_size * partition_count * 4

        with repeater_traffic(
            context=self.test_context,
            redpanda=self.redpanda,
            topics=[topic],
            msg_size=msg_size,
            workers=1,
        ) as repeater:
            repeater.await_group_ready()
            repeater.await_progress(total_write_bytes // msg_size, timeout_sec=120)

        def all_partitions_have_segments():
            view = BucketView(self.redpanda)
            for p in range(0, partition_count):
                if view.cloud_log_segment_count_for_ntp(topic, p) == 0:
                    return False

            return True

        self.redpanda.wait_until(
            all_partitions_have_segments, timeout_sec=60, backoff_sec=5
        )

        # Initially a bucket scrub should pass
        self.logger.info("Running baseline scrub")
        self.redpanda.stop_and_scrub_object_storage()
        self.redpanda.for_nodes(
            self.redpanda.nodes,
            lambda n: self.redpanda.start_node(n, first_start=False),
        )

        # Go delete a segment: pick one arbitrarily, but it must be one
        # that is linked into a manifest to constitute a corruption.
        view = BucketView(self.redpanda)
        segment_key = None
        for o in self.redpanda.cloud_storage_client.list_objects(
            self.si_settings.cloud_storage_bucket
        ):
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
            validate=True,
        )

        self.logger.info("Running scrub that should discover issue")
        with expect_exception(RuntimeError, lambda e: "fatal" in str(e)):
            self.redpanda.stop_and_scrub_object_storage()

        # Avoid tripping exception during shutdown: reinstate the object
        self.redpanda.cloud_storage_client.move_object(
            self.si_settings.cloud_storage_bucket,
            tmp_location,
            segment_key,
            validate=True,
        )

        self.redpanda.for_nodes(
            self.redpanda.nodes,
            lambda n: self.redpanda.start_node(n, first_start=False),
        )


class SimpleSelfTest(Test):
    """
    Verify instantiation of a RedpandaServiceABC subclass through the factory method.

    Runs a few methods of RedpandaService.
    """

    def __init__(self, test_context: TestContext) -> None:
        super(SimpleSelfTest, self).__init__(test_context)
        self.redpanda = make_redpanda_mixed_service(test_context, min_brokers=3)

    def setUp(self) -> None:
        self.redpanda.start()

    @cluster(num_nodes=3, check_allowed_error_logs=False)
    def test_cloud(self) -> None:
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
        topic_name = "rp_ducktape_test_cloud_topic"
        self.logger.info(f"creating topic {topic_name}")
        rpk.create_topic(topic_name)
        self.logger.info(f"deleting topic {topic_name}")
        rpk.delete_topic(topic_name)


class KubectlSelfTest(Test):
    """
    Verify that the kubectl test works. Only does anything when running
    in the cloud.
    """

    def __init__(self, test_context: TestContext) -> None:
        super().__init__(test_context)
        self.redpanda = make_redpanda_mixed_service(test_context)

    def setUp(self) -> None:
        self.redpanda.start()

    @cluster(num_nodes=3)
    def test_kubectl_tool(self) -> None:
        rp = self.redpanda

        if isinstance(rp, RedpandaServiceCloud):
            version_out = rp.kubectl.cmd(["version", "--client"])
            assert "Client Version" in version_out, (
                f"Did not find expceted output, output was: {version_out}"
            )

            try:
                # foobar, of course, is not a valid kubectl command
                rp.kubectl.cmd(["foobar"])
                assert False, "expected this command to throw"
            except CalledProcessError:
                pass


class KubectlLocalOnlyTest(Test):
    @dt_cluster(num_nodes=0)
    def test_is_redpanda_pod(self) -> None:
        test_cases: dict[str, dict[str, Any]] = {
            "regular_hit": {
                "pod": {
                    "metadata": {
                        "generateName": "redpanda-broker-",
                        "labels": {
                            "app.kubernetes.io/component": "redpanda-statefulset"
                        },
                    }
                },
                "result": True,
            },
            "miss_wrong_generate_name": {
                "pod": {
                    "metadata": {
                        "generateName": "redpanda-broker-configuration-",
                        "labels": {
                            "app.kubernetes.io/component": "redpanda-statefulset"
                        },
                    }
                },
                "result": False,
            },
            "miss_wrong_component": {
                "pod": {
                    "metadata": {
                        "generateName": "redpanda-broker-configuration-",
                        "labels": {
                            "app.kubernetes.io/component": "redpanda-post-install"
                        },
                    },
                },
                "result": False,
            },
            "hit_legacy_pod_name": {
                "pod": {"metadata": {"name": "rp-CLUSTER_ID-4"}},
                "result": True,
            },
        }
        for test_name, test_case in test_cases.items():
            pod_obj: dict[str, Any] = test_case["pod"]
            expected_result: bool = test_case["result"]
            try:
                actual_result = is_redpanda_pod(pod_obj, "CLUSTER_ID")
            except KeyError as err:
                self.logger.error(f"KeyError from is_redpanda_pod: {err}")
                actual_result = False
            assert expected_result is actual_result, (
                f"Failed for test case '{test_name}' (expected={expected_result}, actual={actual_result})"
            )


class FailureInjectorSelfTest(Test):
    """
    Verify instantiation of a FailureInjectorBase subclass through the factory method.
    """

    def __init__(self, test_context: TestContext) -> None:
        super(FailureInjectorSelfTest, self).__init__(test_context)
        self.redpanda = make_redpanda_service(test_context, 3)

    def setUp(self) -> None:
        self.redpanda.start()

    @cluster(num_nodes=3, check_allowed_error_logs=False)
    def test_finjector(self) -> None:
        fi = make_failure_injector(self.redpanda)
        fi.inject_failure(FailureSpec(FailureSpec.FAILURE_ISOLATE, None))


def _assert_expected_backtrace_contents(
    test: RedpandaTest, needle: str = "::log_backtrace"
) -> None:
    """
    Assert that the backtrace capture file contains needle.
    """
    backtrace_contents = (
        test.redpanda.nodes[0]
        .account.ssh_output(f"cat {RedpandaService.BACKTRACE_CAPTURE}")
        .decode()
    )
    test.logger.debug(f"Backtrace contents: {backtrace_contents}")
    # we look for this characteristic string in the backtrace, which is
    # the method in the admin API that was called to capture the backtrace
    assert needle in backtrace_contents, (
        f"Didn't find expected string '{needle}' in backtrace (see debug log for contents)"
    )


def _assert_log_content(test: RedpandaTest, node: ClusterNode, needle: str) -> None:
    """
    Assert that the redpanda log contains the expected content.
    """
    log_searcher = LogSearchLocal(
        test.test_context,
        [],
        test.redpanda.logger,
        test.redpanda.STDOUT_STDERR_CAPTURE,
    )

    lines = list(log_searcher._capture_log(node, f"'{needle}'"))
    assert lines, f"Did not find expected string '{needle}' in redpanda log"
    test.logger.debug(f"Found matching log line: {lines[0]}")


class RedpandaServiceSelfTest(RedpandaTest):
    @cluster(num_nodes=1)
    @matrix(simple_backtrace=[True, False])
    def test_backtrace(self, simple_backtrace: bool) -> None:
        rp = self.redpanda
        node = rp.nodes[0]

        # before anything happens, the backtrace capture file should not exist
        assert not node.account.exists(RedpandaService.BACKTRACE_CAPTURE), (
            "Backtrace capture file should not exist before test"
        )

        rp._admin.log_backtrace(node, simple_backtrace=simple_backtrace)
        rp.decode_backtraces(raise_on_failure=True)
        _assert_expected_backtrace_contents(self)

    @cluster(num_nodes=1)
    @matrix(fail_test=[False, True])
    def test_cluster_decorator_backtrace(self, fail_test: bool) -> None:
        """This test checks that the @cluster decorator successfully captures the
        backtrace when the wrapped test failed, and only if it fails."""
        rp = self.redpanda
        node = rp.nodes[0]
        tc = self.test_context

        # before anything happens, the backtrace capture file should not exist
        assert not node.account.exists(RedpandaService.BACKTRACE_CAPTURE), (
            "Backtrace capture file should not exist before test"
        )

        rp._admin.log_backtrace(node)

        class FailThisTest(Exception):
            pass

        # We need something that looks like a RedpandaTest to use the @cluster decorator
        class DummyTest:
            def __init__(self) -> None:
                self.redpanda = rp
                self.test_context = tc

            @cluster(num_nodes=1)
            def run(self) -> None:
                if fail_test:
                    raise FailThisTest()

        try:
            DummyTest().run()  # type: ignore[call-arg]
            assert not fail_test, "inner test passed when it shouldn't"
        except FailThisTest:
            assert fail_test, "inner test failed when it shouldn't"

        try:
            _assert_expected_backtrace_contents(self)
            assert fail_test
        except RemoteCommandError:
            # expected when the inner test passed, as if the inner test passed,
            # the backtrace capture file should not exist
            assert not fail_test

    @cluster(num_nodes=1, check_allowed_error_logs=False)
    @ignore_if_not_asan
    def test_asan_backtrace(self) -> None:
        """This test checks that we correctly backtrace from an ASAN crash. This
        backtrace is the one done by ASAN itself, not the decode_backtrace() one
        we do in ducktape in test teardown."""
        self._crash_test_impl(CrashType.ASAN_CRASH)

    @cluster(num_nodes=1, check_allowed_error_logs=False)
    @ignore_if_not_ubsan
    def test_ubsan_backtrace(self) -> None:
        """This test checks that we correctly backtrace from a UBSAN crash. This
        backtrace is the one done by UBSAN itself, not the decode_backtrace() one
        we do in ducktape in test teardown."""
        self._crash_test_impl(CrashType.UBSAN_CRASH)

    def _crash_test_impl(self, crash_type: CrashType) -> None:
        """This test checks that we correctly capture a backtrace from a crash
        of the given type."""
        rp = self.redpanda
        node = rp.nodes[0]
        rp._admin.trigger_crash(node, crash_type)
        # look for this snipptet which will appear at the top of the backtrace
        # if it was properly decoded
        _assert_log_content(self, node, "in (anonymous namespace)::trigger_crash")

    @cluster(num_nodes=1)
    def test_start(self) -> None:
        pass


class RedpandaClusteredServiceSelfTest(RedpandaTest):
    """Same as RedpandaServiceSelfTest but uses a 3-broker cluster.
    Use this only for tests that need more than one broker."""

    def __init__(self, test_context: TestContext) -> None:
        super().__init__(test_context, num_brokers=3)

    @cluster(num_nodes=3, check_allowed_error_logs=False)
    def test_raise_on_bad_logs(self):
        """
        Test that the LogMessage admin API correctly logs messages and that
        ERROR level logs are caught by raise_on_bad_logs.
        """
        admin_v2 = AdminV2(self.redpanda)
        # use the last node for a slightly better test
        node = self.redpanda.nodes[2]

        # Create a unique error message that we can search for
        test_error_msg = "TEST_LOG_MESSAGE_API_ERROR_MARKER_12345"

        # Use the new LogMessage API to log an error
        request = debug_pb.LogMessageRequest(
            message=test_error_msg, level=debug_pb.LOG_LEVEL_ERROR
        )
        admin_v2.debug(node=node).log_message(request)

        # Verify the error message was logged
        _assert_log_content(self, node, test_error_msg)

        def validate_exception(e: BadLogLines) -> bool:
            # should have the marker and also the name of node 2
            exn_str = str(e)
            return test_error_msg in exn_str and node.name in exn_str

        # Now verify that raise_on_bad_logs will catch this error
        with expect_exception(BadLogLines, validate_exception):
            self.redpanda.raise_on_bad_logs(allow_list=[])

    @ignore
    @cluster(num_nodes=3, check_allowed_error_logs=False)
    def test_bll_bench(self):
        """
        Test that the LogMessage admin API correctly logs messages and that
        ERROR level logs are caught by raise_on_bad_logs.

        Ignored by default since we don't want to run benchmarks in CI.
        """
        # create and delete a 1000-partition topic 10 times
        rpk = RpkTool(self.redpanda)

        parts = 1000

        for i in range(10):
            topic_name = f"bll_bench_{i}"

            def _all_partitions_present():
                try:
                    desc = list(rpk.describe_topic(topic_name))
                    return len(desc) == parts
                except Exception:
                    return False

            # 1000 partitions, replication factor 1 to avoid excess resource usage
            rpk.create_topic(topic_name, partitions=parts, replicas=3)
            self.redpanda.wait_until(
                _all_partitions_present, timeout_sec=30, backoff_sec=1
            )
            rpk.delete_topic(topic_name)
            self.logger.warning(f"c d topic {i}")

        start = time.time()
        self.redpanda.raise_on_bad_logs(allow_list=[])
        elapsed = time.time() - start
        self.logger.warning(f"raise_on_bad_logs elapsed {elapsed:.3f}s")


class RedpandaServiceSelfRawTest(Test):
    """This 'raw' test inherits only from Test, so that internally it
    can set up a inner RedpandaTest object, and test behavior of that
    test."""

    # We need something that looks like a RedpandaTest to use the @cluster decorator
    class InnerTest(RedpandaTest):
        def __init__(self, *args: Any) -> None:
            # force the log level here because the behavior differs slightly between info and
            # debug: at debug we pick up a different NodeCrash log line (emitted by crash tracker)
            # which makes the content assertion in test_raise_on_crash fail
            super().__init__(*args, num_brokers=1, log_config=LoggingConfig("info"))

        @cluster(num_nodes=1)
        def run(self, func: Callable[[RedpandaTest], None]) -> None:
            func(self)

    @contextmanager
    def _with_inner(
        self, func: Callable[[RedpandaTest], None]
    ) -> Iterator[RedpandaTest]:
        test = self.InnerTest(self.test_context)
        try:
            test.setUp()
            yield test
        finally:
            test.tearDown()

    @dt_cluster(num_nodes=1)
    @ignore_if_not_debug
    def test_cluster_decorator_backtrace(self) -> None:
        def func(rptest: RedpandaTest) -> None:
            node = rptest.redpanda.nodes[0]
            rptest.redpanda._admin.trigger_crash(node, CrashType.ASSERT)

        with self._with_inner(func) as test:
            try:
                test.run(func=func)  # type: ignore
                raise RuntimeError("inner test passed when it shouldn't")
            except BadLogLines:
                # expected, as the test intentionally emits a bad log line
                pass
            _assert_expected_backtrace_contents(test, "::trigger_crash")

    @dt_cluster(num_nodes=1)
    def test_raise_on_crash(self) -> None:
        def func(rptest: RedpandaTest) -> None:
            node = rptest.redpanda.nodes[0]
            rptest.redpanda.signal_redpanda(node, signal.SIGSEGV)
            raise RuntimeError("test is failing")  # to trigger raise_on_crash

        with self._with_inner(func) as test:
            try:
                test.run(func=func)  # type: ignore
                raise RuntimeError("inner test passed when it shouldn't")
            except NodeCrash as e:
                assert "SIGSEGV" in str(e)
