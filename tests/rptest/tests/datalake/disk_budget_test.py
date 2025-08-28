# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import time
import threading
from rptest.services.catalog_service import CatalogType
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import SISettings
from rptest.services.redpanda import ResourceSettings
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import skip_debug_mode
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from ducktape.utils.util import wait_until
import ducktape.errors

from ducktape.mark import matrix


class DatalakeDiskUsageTest(RedpandaTest):
    """
    In this test we are going to configure redpanda initially to allow a large
    amount of data to accumulate in the datalake staging directory.

    After the data has accumulated we'll use published metrics to observe that
    the size is not decreasing which we use to infer that none of the existing
    triggers is forcing translation to finish (lag, flush bytes, disk space).

    Finally we'll lower the target disk usage and expect that space usage
    declines indicating that it was the target disk space usage monitor that was
    controlling the usage.
    """

    def __init__(self, test_ctx, *args, **kwargs):
        # datalake has a hard coded 10% memory limit. when a translator hits
        # this limit it force finishes. this makes it difficult to accumulate
        # data on disk. so bump up the shard memory limit to make that a bit
        # easier.
        resource_setting = ResourceSettings(num_cpus=2, memory_mb=8192)
        self.target_lag_sec = 5 * 60
        super(DatalakeDiskUsageTest, self).__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            resource_settings=resource_setting,
            extra_rp_conf={
                "iceberg_enabled": True,
                "datalake_disk_space_monitor_enable": True,
                # configure so that data will accumulate in the staging
                # directory. flush/scratch space limits are set high to avoid
                # space limits. lag is set high to avoid finishing. time slice
                # is set low to avoid hitting the per-core memory limit.
                "datalake_scheduler_time_slice_ms": 3000,
                "datalake_translator_flush_bytes": 100 * 2**30,
                "iceberg_target_lag_ms": self.target_lag_sec * 1000,
                "datalake_scratch_space_size_bytes": 100 * 2**30,
            },
            *args,
            **kwargs,
        )

        self.test_ctx = test_ctx
        self.topic_name = "test"

    def datalake_staging_usage(self):
        # returns number of bytes in datalake staging directory
        metric_name = "vectorized_space_management_datalake_disk_usage_bytes"
        return self.redpanda.metric_sum(metric_name, expect_metric=True)

    def create_topic(self, num_partitions):
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            self.topic_name,
            partitions=num_partitions,
            replicas=1,
            config={TopicSpec.PROPERTY_ICEBERG_MODE: "key_value"},
        )

    def produce_until_staging_size(self, target_size):
        # produce some data to the topic and then back off and let datalake do
        # its thang. after that check back in with the broker and if we haven't
        # translated enough data then try again.
        timeout_sec = 120
        start_time = time.time()
        current_size = 0
        while current_size < target_size:
            producer = RpkProducer(
                self.test_ctx,
                self.redpanda,
                self.topic_name,
                2**14,
                2**15,  # ~256mb
                acks=-1,
            )
            producer.start()
            producer.wait()
            producer.free()
            time.sleep(5)
            current_size = self.datalake_staging_usage()
            self.logger.info(f"Staging data usage {current_size}")
            assert (time.time() - start_time) < timeout_sec, (
                f"{current_size} < {target_size}"
            )
        return current_size

    def translation_lag(self):
        metric_name = "vectorized_cluster_partition_iceberg_offsets_pending_translation"
        return self.redpanda.metric_sum(metric_name, expect_metric=True)

    @cluster(num_nodes=2)
    @skip_debug_mode
    @matrix(
        num_partitions=[10, 40],
        concurrent_translations=[4],
        cloud_storage_type=supported_storage_types(),
    )
    def test_idle_finish(
        self, num_partitions, concurrent_translations, cloud_storage_type
    ):
        self.redpanda.set_cluster_config(
            {
                "datalake_scheduler_max_concurrent_translations": concurrent_translations,
            }
        )

        # produce data until we have a nice bit of datalake staging data on disk
        target_size = 2 * 2**30
        self.create_topic(num_partitions)
        idle_staging_size = self.produce_until_staging_size(target_size)

        # based on the configuration (see __init__) we expect that the staging
        # data is not finished and uploaded. so let's sanity check that.
        try:
            wait_until(
                lambda: self.datalake_staging_usage() < idle_staging_size,
                timeout_sec=30,
                backoff_sec=2,
            )
            assert False, f"{self.datalake_staging_usage()} < {idle_staging_size}"
        except ducktape.errors.TimeoutError:
            # success, the data usage didn't shrink
            pass

        # now we will halve the target size and we expect to see that the
        # staging directory usage decreases below this value.
        new_target_size = target_size // 2
        self.redpanda.set_cluster_config(
            {
                "datalake_scratch_space_size_bytes": new_target_size,
            }
        )
        wait_until(
            lambda: self.datalake_staging_usage() <= new_target_size,
            timeout_sec=60,
            backoff_sec=2,
        )

        # start a thread that tracks the max observed usage
        self.max_usage_observed = 0
        self.stopped = threading.Event()

        def usage_monitor():
            while not self.stopped.is_set():
                usage = self.datalake_staging_usage()
                lag = self.translation_lag()
                self.max_usage_observed = max(self.max_usage_observed, usage)
                self.logger.info(
                    f"Max usage observed {self.max_usage_observed} lag {lag} current usage {usage}"
                )
                time.sleep(1)

        usage_monitor_thread = threading.Thread(target=usage_monitor, daemon=True)
        usage_monitor_thread.start()

        try:
            # now let's go ham with the producing
            lag_start = self.translation_lag()
            producer = RpkProducer(
                self.test_ctx,
                self.redpanda,
                self.topic_name,
                2**14,  # 16kb message size
                2**18,  # 4gb total data written
                acks=-1,
            )
            producer.start()

            # make sure stuff looks like it is happening!
            wait_until(
                lambda: self.translation_lag() > lag_start,
                timeout_sec=60,
                backoff_sec=5,
            )

            producer.wait()
            producer.free()

            # after we finish producing, wait for translation to finish. since
            # we had to set the lag time high to increase data that lands on
            # disk, we also need to wait a long time for lag to go to zero.
            # currently it doens't look like we can dynamically change that.
            wait_until(
                lambda: self.translation_lag() == 0,
                timeout_sec=self.target_lag_sec * 2,
                backoff_sec=10,
                err_msg=f"lag={self.translation_lag()}",
            )

            self.logger.info("Finished waiting on translation to complete")

            assert (
                self.max_usage_observed > 0
                and self.max_usage_observed <= new_target_size
            ), f"{self.max_usage_observed} > {new_target_size}"
        finally:
            self.stopped.set()
            usage_monitor_thread.join()
