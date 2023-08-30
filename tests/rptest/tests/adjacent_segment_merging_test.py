# Copyright 2023 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import CloudStorageType, SISettings, get_cloud_storage_type

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.util import (
    wait_until, )
from rptest.utils.si_utils import BucketView

from ducktape.mark import matrix

import time

# Log errors expected when connectivity between redpanda and the S3
# backend is disrupted
CONNECTION_ERROR_LOGS = [
    # e.g. archival - [fiber1] - service.cc:484 - Failed to upload 3 segments out of 4
    r"archival - .*Failed to upload \d+ segments"
]


class AdjacentSegmentMergingTest(RedpandaTest):
    s3_topic_name = "panda-topic"
    topics = (TopicSpec(name=s3_topic_name,
                        partition_count=1,
                        replication_factor=3), )

    def __init__(self, test_context):
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            log_segment_size=1024 * 1024,
            cloud_storage_segment_max_upload_interval_sec=1,
            cloud_storage_enable_remote_write=True)

        xtra_conf = dict(
            cloud_storage_housekeeping_interval_ms=10000,
            cloud_storage_idle_timeout_ms=200,
            cloud_storage_segment_size_target=1024 * 1024 * 10,
            cloud_storage_segment_size_min=1024 * 1024 * 8,
        )

        self.bucket_name = si_settings.cloud_storage_bucket

        super(AdjacentSegmentMergingTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=xtra_conf,
                             si_settings=si_settings)

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    def setUp(self):
        super().setUp()  # topic is created here

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
                        self.redpanda).partition_manifests.items():
                    target_lower_bound = 1024 * 1024 * 8
                    for name, meta in manifest["segments"].items():
                        self.logger.info(
                            f"segment {name}, segment_meta: {meta}")
                        if meta["size_bytes"] >= target_lower_bound:
                            # we will only see large segments with size
                            # greater than lower bound if housekeeping
                            # is working
                            num_good += 1
                return num_good > 0
            except Exception as err:
                import traceback
                self.logger.info("".join(
                    traceback.format_exception(type(err), err,
                                               err.__traceback__)))
                return False

        wait_until(manifest_has_one_segment, 60)
