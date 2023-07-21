# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import concurrent.futures
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer
from rptest.services.redpanda import SISettings, MetricsEndpoint, ResourceSettings
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool
from rptest.utils.si_utils import quiesce_uploads
from ducktape.utils.util import wait_until
from typing import Optional
from ducktape.mark import matrix
import time


class LogStorageTargetSizeTest(RedpandaTest):
    segment_upload_interval = 30
    manifest_upload_interval = 10
    retention_local_trim_interval = 5

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context, *args, **kwargs)

    def setUp(self):
        # defer redpanda startup to the test
        pass

    def _kafka_usage(self):
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(self.redpanda.nodes)) as executor:
            return list(
                executor.map(
                    lambda n: self.redpanda.data_dir_usage("kafka", n),
                    self.redpanda.nodes))

    @cluster(num_nodes=4)
    @matrix(log_segment_size=[1024 * 1024, 100 * 1024 * 1024],
            strict=[True, False])
    def size_within_target_threshold_test(self, log_segment_size, strict):
        if self.redpanda.dedicated_nodes:
            partition_count = 64
            rate_limit_bps = int(120E6)
            target_size = 5 * 1024 * 1024 * 1024
        else:
            partition_count = 16
            rate_limit_bps = int(30E6)
            target_size = 300 * 1024 * 1024

        # we never reclaim the active segment. so at a minimum we could observe
        # a segment per partition nearly full. of course, we can also roll the
        # active segment to make it eligible for reclaim, but this model is
        # intentionally being simple and convservative.
        active_segments_usage = partition_count * log_segment_size

        # accounting for new data being written and old data being removed is
        # not immediate. currently a monitoring loop runs periodically, and
        # during this time data may accumulate that is not subject to being
        # removed because the monitor has not run to notice it.
        accounting_delay_accumulation = rate_limit_bps * self.retention_local_trim_interval

        # consider the case where all the active segments fill up and roll at
        # the same time, and then a full accounting period passes during which
        # fresh data being written will accumulate.
        usage_overhead = active_segments_usage + accounting_delay_accumulation

        # write a lot more data into the system than the target size to make
        # sure that we are definitely exercising target size enforcement.
        data_size = target_size * 10

        # max usage over the target that is considered success. we don't expect
        # to stay completely below the target because there are accounting and
        # removal delays, among other factors affecting precision.
        max_overage = usage_overhead * 2

        msg_size = 16384
        msg_count = data_size // msg_size
        topic_name = "log-storage-target-size-topic"

        # configure and start redpanda
        extra_rp_conf = {
            'cloud_storage_segment_max_upload_interval_sec':
            self.segment_upload_interval,
            'cloud_storage_manifest_max_upload_interval_sec':
            self.manifest_upload_interval,
            'retention_local_trim_interval':
            self.retention_local_trim_interval * 1000,
            'retention_local_target_capacity_bytes': target_size,
            'disk_reservation_percent': 0,
            'retention_local_target_capacity_percent': 100,
        }

        # when local retention is not strict, data can expand past the local
        # retention and this expanded data will be subject to reclaim first in
        # the eviction policy. reduce the expiration age from default 24 hours
        # to 30 seconds in order to make it more likely that we are testing this.
        if not strict:
            extra_rp_conf.update({
                'retention_local_target_ms_default':
                30 * 1000,
            })

        si_settings = SISettings(test_context=self.test_context,
                                 retention_local_strict=strict,
                                 log_segment_size=log_segment_size)
        self.redpanda.set_extra_rp_conf(extra_rp_conf)
        self.redpanda.set_si_settings(si_settings)
        self.redpanda.start()

        # Sanity check test parameters against the nodes we are running on
        disk_space_required = data_size
        assert self.redpanda.get_node_disk_free(
        ) >= disk_space_required, f"Need at least {disk_space_required} bytes space"

        # create the target topic
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(topic_name, partitions=partition_count, replicas=3)

        # setup and start the background producer
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic_name,
                                       msg_size=msg_size,
                                       msg_count=msg_count,
                                       rate_limit_bps=rate_limit_bps,
                                       batch_max_bytes=msg_size * 8)
        producer.start()
        produce_start_time = time.time()

        # helper: bytes to MBs / human readable
        def hmb(bs):
            convert = lambda b: round(b / (1024 * 1024), 1)
            if isinstance(bs, int):
                return convert(bs)
            return [convert(b) for b in bs]

        # while the producer is running, periodically wake up and go see if the
        # kafka storage on every node is meeting the target, subject to the max
        # overage fuzz factor.
        check_interval = 5
        last_report_time = 0
        max_totals = None
        while producer.produce_status.acked < msg_count:
            # calculate and report disk usage across nodes
            if time.time() - last_report_time >= check_interval:
                totals = self._kafka_usage()
                if not max_totals:
                    max_totals = totals
                max_totals = [max(t, m) for t, m in zip(totals, max_totals)]
                overages = [t - target_size for t in totals]
                violation = any(o > max_overage for o in overages)
                self.logger.info(
                    f"Acked messages {producer.produce_status.acked} / {msg_count}"
                )
                self.logger.info(
                    f"Latest totals {hmb(totals)} maxes {hmb(max_totals)}")
                self.logger.info(
                    f"Target {hmb(target_size)} overages {hmb(overages)} max {hmb(max_overage)}"
                )
                assert not violation, f"Overage exceeded max {hmb(max_overage)}"
                last_report_time = time.time()

            # don't sleep long--we want good responsivenses when producing is
            # completed so that our calculated bandwidth is accurate
            time.sleep(1)

        # we shouldn't start waiting until it looks like we are done, so timeout
        # is only really used here as a backup if something get stucks.
        producer.wait(timeout_sec=30)
        produce_duration = time.time() - produce_start_time
        self.logger.info(
            f"Produced {hmb([data_size])} in {produce_duration} seconds, {(data_size/produce_duration)/1E6:.2f}MB/s"
        )
        producer.stop()
        producer.free()
