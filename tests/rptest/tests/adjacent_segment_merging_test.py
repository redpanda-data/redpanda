# Copyright 2023 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import time

from ducktape.mark import matrix

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import (
    SISettings,
    get_cloud_storage_type,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_timeout, wait_until
from rptest.utils.si_utils import BucketView

# Log errors expected when connectivity between redpanda and the S3
# backend is disrupted
CONNECTION_ERROR_LOGS = [
    # e.g. archival - [fiber1] - service.cc:484 - Failed to upload 3 segments out of 4
    r"archival - .*Failed to upload \d+ segments"
]


class AdjacentSegmentMergingTestBase(RedpandaTest):
    s3_topic_name = "panda-topic"

    def __init__(self, test_context, extra_rp_conf: dict[str, str] = {}, **kwargs):
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            log_segment_size=1024 * 1024,
            cloud_storage_segment_max_upload_interval_sec=1,
            cloud_storage_enable_remote_write=True,
        )

        xtra_conf = dict(
            cloud_storage_housekeeping_interval_ms=10000,
            cloud_storage_idle_timeout_ms=200,
            cloud_storage_segment_size_target=1024 * 1024 * 10,
            cloud_storage_segment_size_min=1024 * 1024 * 8,
        )

        self.bucket_name = si_settings.cloud_storage_bucket

        super().__init__(
            test_context=test_context,
            extra_rp_conf={**xtra_conf, **extra_rp_conf},
            si_settings=si_settings,
            **kwargs,
        )

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    def setUp(self):
        super().setUp()  # topic is created here


class AdjacentSegmentMergingTest(AdjacentSegmentMergingTestBase):
    topics = (
        TopicSpec(
            name=AdjacentSegmentMergingTestBase.s3_topic_name,
            partition_count=1,
            replication_factor=3,
        ),
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @cluster(num_nodes=3)
    @matrix(acks=[-1, 1], cloud_storage_type=get_cloud_storage_type())
    def test_reupload_of_local_segments(self, acks, cloud_storage_type):
        """Test adjacent segment merging using using local data.
        The test starts by uploading large number of very small segments.
        The total amount of data produced is smaller than the target segment
        size. Because of that, after the housekeeping we should end up with
        only one segment in the cloud.
        The retention is not enable so the reupload process can use data
        available locally.
        """
        for _ in range(10):
            # Every 'produce' call should create at least one segment
            # in the cloud which is 1MiB
            self.kafka_tools.produce(self.topic, 1024, 1024, acks)
            time.sleep(1)
        time.sleep(5)

        def manifest_has_one_segment():
            try:
                num_good = 0
                for ntp, manifest in BucketView(
                    self.redpanda
                ).partition_manifests.items():
                    target_lower_bound = 1024 * 1024 * 8
                    for name, meta in manifest["segments"].items():
                        self.logger.info(f"segment {name}, segment_meta: {meta}")
                        if meta["size_bytes"] >= target_lower_bound:
                            # we will only see large segments with size
                            # greater than lower bound if housekeeping
                            # is working
                            num_good += 1
                return num_good > 0
            except Exception as err:
                import traceback

                self.logger.info(
                    "".join(
                        traceback.format_exception(type(err), err, err.__traceback__)
                    )
                )
                return False

        wait_until(manifest_has_one_segment, 60)


class AdjacentSegmentMergingToggleCompactionTest(AdjacentSegmentMergingTestBase):
    topics = (
        TopicSpec(
            name=AdjacentSegmentMergingTestBase.s3_topic_name,
            partition_count=1,
            replication_factor=1,
            cleanup_policy=TopicSpec.CLEANUP_COMPACT,
            min_cleanable_dirty_ratio=0.0,
            max_compaction_lag_ms=3000,
        ),
    )

    def __init__(self, test_context, *args, **kwargs):
        xtra_conf = dict(
            cloud_storage_enable_compacted_topic_reupload=False,
            cloud_storage_enable_segment_merging=True,
            log_compaction_interval_ms=50,
            log_compaction_use_sliding_window=False,
            compacted_log_segment_size=1024 * 512,
            max_compaction_lag_ms=3000,
        )
        self.test_context = test_context
        super().__init__(
            test_context, extra_rp_conf=xtra_conf, num_brokers=1, *args, **kwargs
        )

    @cluster(num_nodes=2)
    @matrix(
        acks=[
            -1,
            1,
        ],
        cloud_storage_type=get_cloud_storage_type(),
    )
    def test_reupload_of_local_segments(self, acks, cloud_storage_type):
        """Test adjacent segment merging using using local data.
        The test starts by uploading large number of very small segments.
        The total amount of data produced is smaller than the target segment
        size. Because of that, after the housekeeping we should end up with
        only one segment in the cloud.
        The retention is not enable so the reupload process can use data
        available locally.
        """

        def produce_some():
            for _ in range(10):
                KgoVerifierProducer.oneshot(
                    context=self.test_context,
                    redpanda=self.redpanda,
                    topic=self.topic,
                    msg_size=1024,
                    msg_count=1024,
                    key_set_cardinality=1,
                )
                # # Every 'produce' call should create at least one segment
                # # in the cloud which is 1MiB
                # self.kafka_tools.produce(self.topic, 1024, 1024, acks)
                time.sleep(1)
            time.sleep(5)

        produce_some()

        self.rpk.alter_topic_config(
            self.topic, TopicSpec.PROPERTY_CLEANUP_POLICY, TopicSpec.CLEANUP_DELETE
        )

        self.redpanda.set_cluster_config(
            {"log_compaction_use_sliding_window": True}, expect_restart=True
        )

        def manifest_has_large_segment():
            try:
                num_good = 0
                for ntp, manifest in BucketView(
                    self.redpanda
                ).partition_manifests.items():
                    target_lower_bound = 1024 * 1024 * 8
                    for name, meta in manifest["segments"].items():
                        self.logger.info(f"segment {name}, segment_meta: {meta}")
                        if meta["size_bytes"] >= target_lower_bound:
                            # we will only see large segments with size
                            # greater than lower bound if housekeeping
                            # is working
                            num_good += 1
                return num_good > 0
            except Exception as err:
                import traceback

                self.logger.info(
                    "".join(
                        traceback.format_exception(type(err), err, err.__traceback__)
                    )
                )
                return False

        self.logger.debug(
            "The log is full of small compacted segments, so housekeeping shouldn't have any effect"
        )
        with expect_timeout():
            wait_until(manifest_has_large_segment, 30)

        self.logger.debug(
            "Produce some more small segments with compaction off. Housekeeping should make progress now"
        )
        produce_some()
        wait_until(manifest_has_large_segment, 60)
