# Copyright 2021 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import re

import requests
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import CloudStorageType, SISettings, get_cloud_storage_type
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import (
    expect_http_error,
    produce_until_segments,
    wait_for_local_storage_truncate,
)
from random import choice

# Log errors expected when connectivity between redpanda and the S3
# backend is disrupted
CONNECTION_ERROR_LOGS = [
    "archival - .*Failed to create archivers",

    # e.g. archival - [fiber1] - service.cc:484 - Failed to upload 3 segments out of 4
    r"archival - .*Failed to upload \d+ segments"
]


class SIAdminApiTest(RedpandaTest):

    log_segment_size = 1048576  # 1MB
    local_retention = log_segment_size * 2  # Retain 2 segments

    topics = (TopicSpec(), )

    def __init__(self, test_context):
        si_settings = SISettings(test_context,
                                 cloud_storage_max_connections=5,
                                 log_segment_size=self.log_segment_size,
                                 cloud_storage_enable_remote_read=True,
                                 cloud_storage_enable_remote_write=True)
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

    @cluster(num_nodes=3, log_allow_list=CONNECTION_ERROR_LOGS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_bucket_validation(self, cloud_storage_type):
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
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                self.local_retention,
            },
        )

        produce_until_segments(redpanda=self.redpanda,
                               topic=self.topic,
                               partition_idx=0,
                               count=10,
                               acks=-1)

        wait_for_local_storage_truncate(self.redpanda,
                                        self.topic,
                                        target_bytes=self.local_retention)
        rpk = RpkTool(self.redpanda)

        for partition in rpk.describe_topic(self.topic):
            self.logger.info(f"describe topic result {partition}")
            assert partition.start_offset == 0, f"start-offset of the partition is supposed to be 0 instead of {partition.start_offset}"

        self.redpanda.si_settings.set_expected_damage({"missing_segments"})
        segment_to_remove = self.find_deletion_candidate()
        self.logger.info(f"trying to remove segment {segment_to_remove}")
        self.cloud_storage_client.delete_object(self.s3_bucket_name,
                                                segment_to_remove, True)

        self.logger.info("trying to sync remote partition")

        node = choice(self.redpanda.nodes)
        validation_result = self.admin.si_sync_local_state(self.topic, 0, node)
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
        for obj in self.cloud_storage_client.list_objects(self.s3_bucket_name):
            if re.match(r'.*/0-[\d-]*-1-v1.log\.\d+$', obj.key):
                return obj.key
        return None

    def _get_non_controller_node(self):
        self.admin.wait_stable_configuration('controller',
                                             namespace='redpanda')
        controller_leader = self.admin.get_partition_leader(
            namespace='redpanda', topic='controller', partition=0)
        not_controller = next(
            (node_id for node_id in
             {self.redpanda.node_id(node)
              for node in self.redpanda.nodes}
             if node_id != controller_leader))
        return self.redpanda.get_node(not_controller)

    @cluster(num_nodes=3)
    def test_topic_recovery_redirects_to_controller_leader(self):
        response = self.admin.initiate_topic_scan_and_recovery(
            node=self._get_non_controller_node(), allow_redirects=False)
        assert response.status_code == requests.status_codes.codes[
            'temporary_redirect']

    @cluster(num_nodes=3)
    def test_topic_recovery_on_leader(self):
        response = self.admin.initiate_topic_scan_and_recovery(
            node=self._get_non_controller_node())
        assert response.status_code == requests.status_codes.codes['accepted']
        assert response.json() == {'status': 'recovery started'}

    @cluster(num_nodes=3)
    def test_topic_recovery_request_validation(self):
        for payload in ({
                'x': 1
        }, {
                'topic_names_pattern': 'x',
                'retention_ms': 1,
                'retention_bytes': 1
        }):
            try:
                response = self.admin.initiate_topic_scan_and_recovery(
                    payload=payload)
            except requests.exceptions.HTTPError as e:
                assert e.response.status_code == requests.status_codes.codes[
                    'bad_request'], f'unexpected status code: {e.response} for {payload}'

    @cluster(num_nodes=3)
    def test_topic_recovery_status_to_non_controller(self):
        self.admin.initiate_topic_scan_and_recovery()
        response = self.admin.get_topic_recovery_status(
            node=self._get_non_controller_node(), allow_redirects=False)
        assert response.status_code == requests.status_codes.codes['ok']

    @cluster(num_nodes=3)
    def test_manifest_dump(self):
        with expect_http_error(404):
            not_found_response = self.admin.get_partition_manifest(
                "test-topic", 0)

        self.rpk.create_topic("test-topic")
        self.admin.await_stable_leader("test-topic", 0)
        response = self.admin.get_partition_manifest("test-topic", 0)

        assert "last_offset" in response
        assert response["last_offset"] == 0

        assert "cloud_log_size_bytes" in response
        assert response["cloud_log_size_bytes"] == 0

        self.redpanda.set_cluster_config({"cloud_storage_enabled": False},
                                         expect_restart=True)

        with expect_http_error(400):
            not_enabled_response = self.admin.get_partition_manifest(
                "test-topic", 0)

        self.redpanda.si_settings.set_expected_damage(
            {"ntr_no_topic_manifest"})
