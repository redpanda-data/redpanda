# Copyright 2023 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import MetricsEndpoint, SISettings
from rptest.util import firewall_blocked, wait_until_result
from rptest.utils.si_utils import BucketView, NTPR
from rptest.clients.types import TopicSpec
from rptest.tests.partition_movement import PartitionMovementMixin
from ducktape.utils.util import wait_until
from ducktape.mark import parametrize
from rptest.utils.mode_checks import skip_debug_mode
from ducktape.mark import ok_to_fail

import concurrent.futures
import random
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


class CloudStorageCheck:
    def __init__(self, name, check):
        self._name = name
        self._check = check

    @property
    def check(self):
        return self._check

    @property
    def name(self):
        return self._name


def cloud_storage_usage_check(test):
    # The usage inferred from the uploaded manifest
    # lags behind the actual reported usage. For this reason,
    # we maintain a sliding window of reported usages and check whether
    # the manifest inferred usage can be found in it. A deque size of 10
    # gives us a look-behind window of roughly 2 seconds (10 * 0.2).
    # This should be fine since the manifest after every batch of segment uploads
    # and on prior to each GC operation (housekeeping happens every 1 second).
    reported_usage_sliding_window = deque(maxlen=10)

    def check():
        try:
            bucket_view = BucketView(test.redpanda)
            manifest_usage = bucket_view.cloud_log_size_for_ntp(test.topic, 0)
            test.logger.debug(
                f"Cloud log usage inferred from manifests: {manifest_usage}")

            reported_usage = test.admin.cloud_storage_usage()
            reported_usage_sliding_window.append(reported_usage)

            test.logger.info(
                f"Expected {manifest_usage.total()} bytes of cloud storage usage"
            )
            test.logger.info(
                f"Reported usages in sliding window: {reported_usage_sliding_window}"
            )
            return manifest_usage.total() in reported_usage_sliding_window
        except Exception as e:
            test.logger.info(f"usage check exception: {e}")
            raise e

    # Manifests are not immediately uploaded after they are mutated locally.
    # For example, during cloud storage housekeeping, the manifest is not uploaded
    # after the 'start_offset' advances, but after the segments are deleted as well.
    # If a request lands mid-housekeeping, the results will not be consistent with
    # what's in the uploaded manifest. For this reason, we wait until the two match.
    wait_until(
        check,
        timeout_sec=test.check_timeout,
        backoff_sec=0.2,
        err_msg="Reported cloud storage usage did not match the actual usage",
        retry_on_exc=True)


class PartitionStatusValidator:
    def __init__(self, test):
        self.test = test
        self._si_settings = test.si_settings
        self._logger = test.logger
        self._validators = [
            PartitionStatusValidator._validate_mode,
            PartitionStatusValidator._validate_cloud_log_size_bytes,
            PartitionStatusValidator._validate_cloud_log_offsets
        ]

    def is_valid(self, status, bucket_view: BucketView, ntpr: NTPR) -> bool:
        return all(
            [v(self, status, bucket_view, ntpr) for v in self._validators])

    def _validate_status_shape(self, status, bucket_view: BucketView,
                               ntpr: NTPR) -> bool:
        expected_keys = [
            "cloud_storage_mode", "total_log_size_bytes",
            "cloud_log_size_bytes", "local_log_size_bytes",
            "cloud_log_segment_count", "local_log_segment_count"
        ]

        not_present = []
        for k in expected_keys:
            if k not in status:
                not_present.append(k)

        if len(not_present) > 0:
            self._logger.info(
                f"Expected keys missing from status: {not_present}")

        return len(not_present) == 0

    def _validate_mode(self, status, bucket_view: BucketView,
                       ntpr: NTPR) -> bool:
        if status["cloud_storage_mode"] != "full":
            self._logger.info(
                f"Unexpected for cloud_storage_mode: {status['cloud_storage_mode']}"
            )
            return False

        return True

    def _validate_cloud_log_size_bytes(self, status, bucket_view: BucketView,
                                       ntpr: NTPR) -> bool:
        # bucket_view.evict_ntp(ntpr.to_ntp)
        cloud_log_size = bucket_view.cloud_log_size_for_ntp(
            ntpr.topic, ntpr.partition, ntpr.ns)

        stm_region = cloud_log_size.stm.accessible
        archive_region = cloud_log_size.archive.total

        reported_stm = status["stm_region_size_bytes"]
        if reported_stm != stm_region:
            self._logger.info(
                f"Reported cloud log size for stm region does not match manifest: {reported_stm}!={stm_region}"
            )
            return False

        reported_archive = status["archive_size_bytes"]
        if reported_stm != stm_region:
            self._logger.info(
                f"Reported cloud log size for archive region does not match manifest: {reported_archive}!={archive_region}"
            )
            return False

        return True

    def _validate_cloud_log_offsets(self, status, bucket_view: BucketView,
                                    ntpr: NTPR) -> bool:
        manifest = bucket_view.get_partition_manifest(ntpr)

        if not manifest:
            return "cloud_log_start_offset" not in status and "cloud_log_last_offset" not in status

        cloud_log_start = BucketView.kafka_start_offset(manifest)
        reported_start = status.get("cloud_log_start_offset", None)

        if cloud_log_start != reported_start:
            self._logger.info(
                f"Reported cloud log start does not match manifest: {reported_start} != {cloud_log_start}"
            )
            return False

        cloud_log_last = BucketView.kafka_last_offset(manifest)
        reported_last = status.get("cloud_log_last_offset", None)

        if cloud_log_last != reported_last:
            self._logger.info(
                f"Reported cloud log end does not match manifest: {reported_last} != {cloud_log_last}"
            )
            return False

        return True


def cloud_storage_status_endpoint_check(test):
    bucket_view = BucketView(test.redpanda)
    reported_status_sliding_window = deque(maxlen=5)
    validator = PartitionStatusValidator(test)

    def check():
        try:
            bucket_view.reset()
            bucket_view._do_listing()

            status = test.admin.get_partition_cloud_storage_status(
                test.topic, 0)
            reported_status_sliding_window.append(status)

            ntpr = NTPR(ns="kafka",
                        topic=test.topic,
                        partition=0,
                        revision=test._initial_revision)
            for status in reported_status_sliding_window:
                if validator.is_valid(status, bucket_view, ntpr):
                    return True

            return False
        except Exception as e:
            test.logger.info(f"status_endpoint_check exception: {e}")
            raise e

    wait_until(
        check,
        timeout_sec=test.check_timeout,
        backoff_sec=0.2,
        err_msg="Cloud storage partition status did not match the manifest",
        retry_on_exc=True)


class CloudStorageTimingStressTest(RedpandaTest, PartitionMovementMixin):
    """
    The tests in this class are intended to be generic cloud storage test.
    They use a workload that enables all operations on the cloud storage log
    (appends, truncations caused by retention, compacted segment reuploads and
    adjacent segment merging. A configurable series of checks are performed
    at every 'check_interval'. If any of the checks result in an exception, or
    fail to complete the test will fail.

    The tests can be extended by creating a new check function and registering
    it in the 'prologue' method.
    """

    mib = 1024 * 1024
    message_size = 32 * 1024  # 32KiB
    log_segment_size = 4 * mib  # 4MiB
    chunk_size = 1 * mib
    produce_byte_rate_per_ntp = 8 * mib  # 8 MiB
    target_runtime = 60  # seconds
    check_interval = 10  # seconds
    check_timeout = 10  # seconds
    allow_runtime_overshoot_by = 2

    topic_spec = TopicSpec(name="test-topic",
                           partition_count=1,
                           replication_factor=3,
                           retention_bytes=60 * log_segment_size)

    def __init__(self, test_context):
        extra_rp_conf = dict(
            log_compaction_interval_ms=1000,
            compacted_log_segment_size=self.log_segment_size,
            cloud_storage_idle_timeout_ms=100,
            cloud_storage_segment_size_target=4 * self.log_segment_size,
            cloud_storage_segment_size_min=2 * self.log_segment_size,
            retention_local_target_bytes_default=2 * self.log_segment_size,
            cloud_storage_enable_segment_merging=True,
            cloud_storage_cache_chunk_size=self.chunk_size,
            cloud_storage_spillover_manifest_size=None)

        si_settings = SISettings(
            test_context,
            log_segment_size=self.log_segment_size,
            cloud_storage_housekeeping_interval_ms=1000,
            cloud_storage_spillover_manifest_max_segments=10,
            cloud_storage_segment_max_upload_interval_sec=10)

        if "googleapis" in si_settings.cloud_storage_api_endpoint:
            # If the test is running on GCS we shouldn't retry earlier than
            # after 1s. GCS throttles uploads if they happen once per sencond
            # or faster (per object).
            extra_rp_conf['cloud_storage_initial_backoff_ms'] = 1000

        super(CloudStorageTimingStressTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf,
                             log_level="trace",
                             si_settings=si_settings)

        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)
        self.checks = []

    def _create_producer(self,
                         cleanup_policy: str = None) -> KgoVerifierProducer:
        bps = self.produce_byte_rate_per_ntp * self.topics[0].partition_count
        bytes_count = bps * self.target_runtime
        msg_count = bytes_count // self.message_size

        self.logger.info(f"Will produce {bytes_count / self.mib}MiB at"
                         f"{bps / self.mib}MiB/s on topic={self.topic}")

        key_set_cardinality = 10 if 'compact' in cleanup_policy else None
        return KgoVerifierProducer(self.test_context,
                                   self.redpanda,
                                   self.topic,
                                   msg_size=self.message_size,
                                   msg_count=msg_count,
                                   rate_limit_bps=bps,
                                   debug_logs=True,
                                   trace_logs=True,
                                   key_set_cardinality=key_set_cardinality)

    def _create_consumer(
            self, producer: KgoVerifierProducer) -> KgoVerifierSeqConsumer:
        bps = self.produce_byte_rate_per_ntp * self.topics[0].partition_count
        bytes_count = bps * self.target_runtime
        msg_count = bytes_count // self.message_size

        self.logger.info(
            f"Will consume at {bps / self.mib}MiB/s from topic={self.topic}")

        return KgoVerifierSeqConsumer(self.test_context,
                                      self.redpanda,
                                      self.topic,
                                      msg_size=self.message_size,
                                      max_throughput_mb=int(bps // self.mib),
                                      debug_logs=True,
                                      trace_logs=True,
                                      producer=producer)

    def _all_uploads_done(self):
        topic_description = self.rpk.describe_topic(self.topic)
        for partition in topic_description:
            hwm = partition.high_watermark

            manifest = None
            try:
                bucket = BucketView(self.redpanda)
                manifest = bucket.manifest_for_ntpr(self.topic, partition.id,
                                                    self._initial_revision)
            except Exception as e:
                self.logger.info(
                    f"Exception thrown while retrieving the manifest: {e}")
                return False

            top_segment = max(manifest['segments'].values(),
                              key=lambda seg: seg['base_offset'])
            uploaded_raft_offset = top_segment['committed_offset']
            uploaded_kafka_offset = uploaded_raft_offset - top_segment[
                'delta_offset_end']
            self.logger.info(
                f"Remote HWM {uploaded_kafka_offset} (raft {uploaded_raft_offset}), local hwm {hwm}"
            )

            # -1 because uploaded offset is inclusive, hwm is exclusive
            if uploaded_kafka_offset < (hwm - 1):
                return False

        return True

    def _check_completion(self):
        producer_complete = self.producer.is_complete()
        if not producer_complete:
            return False, f"Producer did not complete: {self.producer.produce_status}"

        consumed = self.consumer.consumer_status.validator.valid_reads
        produced = self.producer.produce_status.acked
        consumer_complete = consumed >= produced
        if not consumer_complete:
            return False, f"Consumer consumed only {consumed} out of {produced} messages"

        uploads_done = self._all_uploads_done()
        if not uploads_done:
            return False, "There are pending uploads to cloud storage"

        return True, ""

    def is_complete(self):
        complete, reason = self._check_completion()
        if complete:
            return True

        delta = datetime.now() - self.test_start_ts
        max_runtime = self.target_runtime * self.allow_runtime_overshoot_by
        if delta.total_seconds() > max_runtime:
            raise TimeoutError(
                f"Workload did not complete within {max_runtime}s: {reason}")

        return False

    def _get_initial_revision(self):
        def get_revision():
            leaders_info = self.admin.get_leaders_info()
            for p in leaders_info:
                if p['topic'] == self.topic:
                    rev = int(p['partition_revision'])
                    if rev < 0:
                        return False

                    self.logger.info(f"Initial revision is {rev}")
                    return True, rev
            return False

        return wait_until_result(
            get_revision,
            timeout_sec=5,
            backoff_sec=1,
            err_msg="Initial revision not found before timeout",
            retry_on_exc=True)

    def prologue(self, cleanup_policy):
        self.redpanda.set_cluster_config_to_null(
            "cloud_storage_manifest_max_upload_interval_sec")

        self.topic_spec.cleanup_policy = cleanup_policy
        self.topics = [self.topic_spec]
        self._create_initial_topics()

        # Preserve the initial revision to be able to fetch the manifest
        # after the partition moves.
        self._initial_revision = self._get_initial_revision()

        self.register_check("cloud_storage_usage", cloud_storage_usage_check)
        self.register_check("cloud_storage_status_endpoint",
                            cloud_storage_status_endpoint_check)

        self.producer = self._create_producer(cleanup_policy)
        self.consumer = self._create_consumer(self.producer)

        self.producer.start()

        # Sleep for a bit to hit the cloud storage read path when consuming
        time.sleep(3)
        self.consumer.start()

        self.test_start_ts = datetime.now()

    def epilogue(self, cleanup_policy):
        self.producer.wait()
        self.consumer.wait()

        assert self.redpanda.metric_sum(
            "vectorized_cloud_storage_successful_downloads_total") > 0

        assert self.redpanda.metric_sum(
            "redpanda_cloud_storage_spillover_manifest_uploads_total",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS) > 0

        bucket_view = BucketView(self.redpanda)
        bucket_view.assert_segments_deleted(self.topic, partition=0)

        if "compact" in cleanup_policy:
            # Assert that compacted segment re-upload operated during the test
            bucket_view.assert_at_least_n_uploaded_segments_compacted(
                self.topic, partition=0, revision=self._initial_revision, n=1)
        else:
            # Adjacent segment merging is disabled on compacted topics, so
            # check if it occurred only on non-compacted topics.
            assert self.redpanda.metric_sum(
                "redpanda_cloud_storage_jobs_local_segment_reuploads",
                metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS) > 0

    def register_check(self, name, check_fn):
        self.checks.append(CloudStorageCheck(name, check_fn))

    def do_checks(self):
        with ThreadPoolExecutor(max_workers=len(self.checks)) as executor:

            def start_check(check):
                self.logger.info(f"Check {check.name} starting")
                return executor.submit(check.check, self)

            futs = {start_check(check): check for check in self.checks}

            done, not_done = concurrent.futures.wait(
                futs, timeout=self.check_interval)

            failure_count = 0
            for f in done:
                check_name = futs[f].name
                if ex := f.exception():
                    self.logger.error(
                        f"Check {check_name} threw an exception: {ex}")
                    failure_count += 1
                else:
                    self.logger.info(
                        f"Check {check_name} completed successfuly")

            for f in not_done:
                check_name = futs[f].name
                self.logger.error(
                    f"Check {check_name} did not complete within the check interval"
                )

            if failure_count > 0 or len(not_done) > 0:
                raise RuntimeError(
                    f"Failed checks: {failure_count}; Incomplete checks: {len(not_done)}"
                )

            self.logger.info(f"All checks completed successfuly")

    @cluster(
        num_nodes=5,
        log_allow_list=[
            # Reader might hit the tail of the log that is being reaped
            # https://github.com/redpanda-data/redpanda/issues/10851
            r"Error in hydraton loop: .*Connection reset by peer",
            r"failed to hydrate chunk.*Connection reset by peer",
            r"failed to hydrate chunk.*NotFound",
        ])
    @parametrize(cleanup_policy="delete")
    @parametrize(cleanup_policy="compact,delete")
    @skip_debug_mode
    @ok_to_fail
    def test_cloud_storage(self, cleanup_policy):
        """
        This is the baseline test. It runs the workload and performs the checks
        periodically, without any external operations being performed.
        """
        self.prologue(cleanup_policy)

        while not self.is_complete():
            self.do_checks()
            time.sleep(self.check_interval)

        self.epilogue(cleanup_policy)

    @cluster(
        num_nodes=5,
        log_allow_list=[
            # https://github.com/redpanda-data/redpanda/issues/10851
            r"Error in hydraton loop: .*Connection reset by peer",
            r"failed to hydrate chunk.*Connection reset by peer",
            r"failed to hydrate chunk.*NotFound",
            r"cluster.*Can't add segment",
        ])
    @parametrize(cleanup_policy="delete")
    @parametrize(cleanup_policy="compact,delete")
    @skip_debug_mode
    def test_cloud_storage_with_partition_moves(self, cleanup_policy):
        """
        This test adds partition moves on top of the baseline cloud storage workload.
        The idea is to evolve this test into a more generic fuzzing test in the future
        (e.g. isolate/kill nodes, isolate leader from cloud storage, change cloud storage
        topic/cluster configs on the fly).
        """
        self.check_timeout = 15  # seconds

        self.prologue(cleanup_policy)

        partitions = []
        for topic in self.topics:
            partitions.extend([(topic.name, pid)
                               for pid in range(topic.partition_count)])

        while not self.is_complete():
            ntp_to_move = random.choice(partitions)
            self._dispatch_random_partition_move(ntp_to_move[0],
                                                 ntp_to_move[1])

            self.do_checks()
            time.sleep(self.check_interval)

        self.epilogue(cleanup_policy)
