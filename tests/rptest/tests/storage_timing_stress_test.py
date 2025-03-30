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
from rptest.services.redpanda import RedpandaService
from rptest.services.storage import ClusterStorage
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.tests.partition_movement import PartitionMovementMixin
from ducktape.mark import parametrize
from rptest.utils.mode_checks import skip_debug_mode

import concurrent.futures
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


class StorageCheck:
    def __init__(self, name, check):
        self._name = name
        self._check = check

    @property
    def check(self):
        return self._check

    @property
    def name(self):
        return self._name


def check_retention(test):
    topic = test.topic_spec
    test.logger.debug(
        f"checking retention overshooting for topic {topic.name}")

    def disk_size_greater():
        # extract information about disk space usage
        storage: ClusterStorage = test.redpanda.storage(all_nodes=True,
                                                        sizes=True,
                                                        scan_cache=False)
        partitions = list(storage.partitions("kafka", topic.name))
        for part in partitions:
            test.logger.debug(
                f"partition {part.num} total_size {part.total_size}")
        return all(part.total_size > topic.retention_bytes
                   for part in partitions)

    # step 1: wait until disk usage is greater than retention
    if not hasattr(test, '._check_disk_space_started'):
        if not disk_size_greater():
            return
        test._check_disk_space_started = True

    # step 2: assert that for remaining_interval, disk_size will be greater than retention
    assert disk_size_greater(
    ), f"storage usage is less than {topic.retention_bytes=}"


def check_disk_space(test):
    assert isinstance(test.admin, Admin)
    assert isinstance(test.redpanda, RedpandaService)
    assert isinstance(test.rpk, RpkTool)
    test.logger.debug(f"checking disk space for topic {test.topic_spec.name}")

    disk_reservation_percent = int(
        test.rpk.cluster_config_get('disk_reservation_percent'))
    disk_usable_percent = (100 - disk_reservation_percent)
    retention_local_target_capacity_percent = int(
        test.rpk.cluster_config_get('retention_local_target_capacity_percent'))
    # space effectively available for local storage is a percentage of disk_usable_percent. as in, disk_reservation_percent dictates how much space can be used for other size computations
    effective_retention_percent = retention_local_target_capacity_percent * disk_usable_percent / 100

    # gather measures for all the nodes
    measures = [{
        'node':
        n.name,
        'disk_total':
        test.redpanda.get_node_disk_measure(n, 'itotal'),
        'disk_used_percent':
        test.redpanda.get_node_disk_usage(node=n, percent=True),
        'local_storage_use':
        test.admin.get_local_storage_usage(node=n)
    } for n in test.redpanda.nodes]

    # check that disk used is below disk usable percent (this should be enforced by redpanda, triggering cleanup)
    def disk_usage_good():
        return all(m['disk_used_percent'] < disk_usable_percent
                   for m in measures)

    # check that local storage is below effective retention percent
    def local_retention_good():
        # data used a as fraction of total disk space
        total_data_fract = [
            (m['local_storage_use']['data'] + m['local_storage_use']['index'] +
             m['local_storage_use']['compaction']) / m['disk_total']
            for m in measures
        ]
        return all(t < (effective_retention_percent / 100)
                   for t in total_data_fract)

    assert disk_usage_good(
    ), f"disk_space went below {disk_reservation_percent=}%. {measures=}"
    assert local_retention_good(
    ), f"local_retention went above {retention_local_target_capacity_percent=}%. {measures=}"


class StorageTimingStressTest(RedpandaTest, PartitionMovementMixin):
    """
    The tests in this class are intended to be generic ~~cloud~~ storage test.
    It's a copy of CloudStorageTimingStressTest, without cloud storage enabled, done in a effort to trigger trigger bugs like the one described in https://github.com/redpanda-data/redpanda/pull/12176
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
    log_segment_size = 128 * 1024  # 128kb
    produce_byte_rate_per_ntp = 32 * mib  # 32 MiB
    target_runtime = 5 * 60  # seconds
    check_interval = 10  # seconds
    check_timeout = 10  # seconds
    allow_runtime_overshoot_by = 2

    topic_spec = TopicSpec(name=f"topic-StorageTimingStressTest",
                           partition_count=1,
                           replication_factor=3,
                           retention_bytes=60 * log_segment_size)

    def __init__(self, test_context):

        extra_rp_conf = dict(
            log_compaction_interval_ms=1000,
            log_segment_size_min=self.log_segment_size,
            compacted_log_segment_size=self.log_segment_size,
            disk_reservation_percent=
            80,  # require to not use this space, to exercise disk space management
            retention_local_target_capacity_percent=
            10,  # require that local data stays into this threshold
            retention_local_trim_interval=2,  # speedup local trim loop
            cloud_storage_enabled=False)

        super(StorageTimingStressTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf,
                             log_level="trace")

        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)
        self.checks = [
            StorageCheck(fn.__name__, fn)
            for fn in [check_retention, check_disk_space]
        ]

    def _create_producer(self, cleanup_policy: str) -> KgoVerifierProducer:
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

    def _check_completion(self):
        producer_complete = self.producer.is_complete()
        if not producer_complete:
            return False, f"Producer did not complete: {self.producer.produce_status}"

        consumed = self.consumer.consumer_status.validator.valid_reads
        produced = self.producer.produce_status.acked
        consumer_complete = consumed >= produced
        if not consumer_complete:
            return False, f"Consumer consumed only {consumed} out of {produced} messages"

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

    def prologue(self, cleanup_policy):
        self.topic_spec.cleanup_policy = cleanup_policy
        self.topics = [self.topic_spec]
        self._create_initial_topics()

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

    def register_check(self, name, check_fn):
        self.checks.append(StorageCheck(name, check_fn))

    def do_checks(self):
        if len(self.checks) == 0:
            return

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
                        f"Check {check_name} completed successfully")

            for f in not_done:
                check_name = futs[f].name
                self.logger.error(
                    f"Check {check_name} did not complete within the check interval"
                )

            if failure_count > 0 or len(not_done) > 0:
                raise RuntimeError(
                    f"Failed checks: {failure_count}; Incomplete checks: {len(not_done)}"
                )

            self.logger.info(f"All checks completed successfully")

    @cluster(num_nodes=5)
    @parametrize(cleanup_policy="delete")
    @parametrize(cleanup_policy="compact,delete")
    @skip_debug_mode
    def test_storage(self, cleanup_policy):
        """
        This is the baseline test. It runs the workload and performs the checks
        periodically, without any external operations being performed.
        """
        self.prologue(cleanup_policy)

        while not self.is_complete():
            self.do_checks()
            self.redpanda.healthy()
            time.sleep(self.check_interval)

        self.epilogue(cleanup_policy)

    @cluster(num_nodes=5)
    @parametrize(cleanup_policy="delete")
    @parametrize(cleanup_policy="compact,delete")
    @skip_debug_mode
    def test_storage_with_partition_moves(self, cleanup_policy):
        """
        This test adds partition moves on top of the baseline cloud storage workload.
        The idea is to evolve this test into a more generic fuzzing test in the future
        (e.g. isolate/kill nodes, isolate leader from cloud storage, change cloud storage
        topic/cluster configs on the fly).
        """

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
            self.redpanda.healthy()
            time.sleep(self.check_interval)

        self.epilogue(cleanup_policy)
