# Copyright 2021 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RedpandaService, SISettings

from rptest.services.admin import Admin
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.util import (
    produce_until_segments,
    wait_for_segments_removal,
)
from ducktape.utils.util import wait_until

# Log errors expected when connectivity between redpanda and the S3
# backend is disrupted
CONNECTION_ERROR_LOGS = [
    "archival - .*Failed to create archivers",

    # e.g. archival - [fiber1] - service.cc:484 - Failed to upload 3 segments out of 4
    r"archival - .*Failed to upload \d+ segments"
]


class SIAdminApiTest(RedpandaTest):
    log_segment_size = 1048576  # 1MB

    topics = (TopicSpec(), )

    def __init__(self, test_context):
        si_settings = SISettings(
            cloud_storage_reconciliation_interval_ms=500,
            cloud_storage_max_connections=5,
            log_segment_size=self.log_segment_size,
            cloud_storage_enable_remote_read=True,
            cloud_storage_enable_remote_write=True,
        )
        self.s3_bucket_name = si_settings.cloud_storage_bucket

        extra_rp_conf = dict(log_segment_size=self.log_segment_size)

        super(SIAdminApiTest, self).__init__(test_context=test_context,
                                             extra_rp_conf=extra_rp_conf,
                                             si_settings=si_settings)

        self._s3_port = si_settings.cloud_storage_api_endpoint_port
        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        self.superuser = self.redpanda.SUPERUSER_CREDENTIALS
        self.admin = Admin(self.redpanda,
                           auth=(self.superuser.username,
                                 self.superuser.password))

    def setUp(self):
        super().setUp()
        # We can't set 'admin_api_require_auth' in 'extra_rp_conf'. The
        # test will fail to start in this case. But we can swith the
        # feature on after start.
        self.redpanda.set_cluster_config({'admin_api_require_auth': True})

    def tearDown(self):
        self.s3_client.empty_bucket(self.s3_bucket_name)
        super().tearDown()

    @cluster(num_nodes=3, log_allow_list=CONNECTION_ERROR_LOGS)
    def test_bucket_validation(self):
        """
        The test produces to the partition and waits untils the
        data is uploaded to S3 and the oldest segments are picked
        up by the retention. After that it uses admin api to invoke
        bucket validation and checks the result.

        The check is performed using describe topic command. The
        start offset returned by the command is provided by the 
        shadow indexing subsystem. After validation the starting
        offset have to change from initial 0 to some other value.
        """
        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_BYTES: 1024,
            },
        )

        produce_until_segments(redpanda=self.redpanda,
                               topic=self.topic,
                               partition_idx=0,
                               count=10,
                               acks=-1)

        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.topic,
                                  partition_idx=0,
                                  count=5)

        rpk = RpkTool(self.redpanda)

        for partition in rpk.describe_topic(self.topic):
            self.logger.info(f"describe topic result {partition}")
            assert partition.start_offset == 0, f"start-offset of the partition is supposed to be 0 instead of {partition.start_offset}"

        segment_to_remove = self.find_deletion_candidate()
        self.logger.info(f"trying to remove segment {segment_to_remove}")
        self.s3_client.delete_object(self.s3_bucket_name, segment_to_remove,
                                     True)

        self.logger.info("trying to sync remote partition")
        for node in self.redpanda.nodes:
            validation_result = self.admin.si_sync_local_state(
                self.topic, 0, node)
            self.logger.info(f"sync result {validation_result}")

        def start_offset_not_zero():
            for partition in rpk.describe_topic(self.topic):
                self.logger.info(f"describe topic result {partition}")
                return partition.start_offset > 0

        wait_until(start_offset_not_zero, 60)

        for part in self.rpk.describe_topic(self.topic):
            self.logger.info(f"describe topic result {part}")
            assert part.start_offset > 0, f"start-offset of the partition is {part.start_offset}, should be greater than 0"

    def find_deletion_candidate(self):
        for obj in self.s3_client.list_objects(self.s3_bucket_name):
            key = obj.Key[:-2]
            if key.endswith("/0-1-v1.log"):
                return obj.Key
        return None
