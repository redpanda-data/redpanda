# Copyright 2021 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool, RpkException
from rptest.util import (
    produce_until_segments,
    wait_for_segments_removal,
    firewall_blocked,
)

# Log errors expected when connectivity between redpanda and the S3
# backend is disrupted
CONNECTION_ERROR_LOGS = [
    "archival - .*Failed to create archivers",

    # e.g. archival - [fiber1] - service.cc:484 - Failed to upload 3 segments out of 4
    r"archival - .*Failed to upload \d+ segments"
]


class ShadowIndexingFirewallTest(RedpandaTest):
    log_segment_size = 1048576  # 1MB
    retention_bytes = 1024  # 1 KB

    s3_topic_name = "panda-topic"
    topics = (TopicSpec(name=s3_topic_name,
                        partition_count=1,
                        replication_factor=3), )

    def __init__(self, test_context):
        si_settings = SISettings(cloud_storage_reconciliation_interval_ms=500,
                                 cloud_storage_max_connections=5,
                                 log_segment_size=self.log_segment_size)

        super(ShadowIndexingFirewallTest,
              self).__init__(test_context=test_context,
                             si_settings=si_settings)

        self._s3_port = si_settings.cloud_storage_api_endpoint_port
        self.rpk = RpkTool(self.redpanda)

    @cluster(num_nodes=3, log_allow_list=CONNECTION_ERROR_LOGS)
    def test_consume_from_blocked_s3(self):
        produce_until_segments(redpanda=self.redpanda,
                               topic=self.s3_topic_name,
                               partition_idx=0,
                               count=5,
                               acks=-1)

        self.rpk.alter_topic_config(self.s3_topic_name,
                                    TopicSpec.PROPERTY_RETENTION_BYTES,
                                    self.retention_bytes)

        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.s3_topic_name,
                                  partition_idx=0,
                                  count=4)
        """Disconnect redpanda from S3 and try to read starting with offset 0"""
        with firewall_blocked(self.redpanda.nodes, self._s3_port):
            try:
                out = self.rpk.consume(topic=self.s3_topic_name)
            except RpkException as e:
                assert 'timed out' in e.msg
            else:
                raise RuntimeError(
                    f"RPK consume should have timed out, but ran with output: {out}"
                )
