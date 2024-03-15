# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import SISettings

from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.utils.mode_checks import skip_debug_mode


class TestEnablingTieredStorage(PreallocNodesTest):
    def __init__(self, test_context):
        super().__init__(test_context,
                         num_brokers=3,
                         node_prealloc_count=1,
                         si_settings=SISettings(test_context=test_context,
                                                fast_uploads=True))

    @property
    def producer_throughput(self):
        return 5 * (1024 * 1024) if not self.debug_mode else 1000

    @property
    def msg_count(self):
        return 20 * int(self.producer_throughput / self.msg_size)

    @property
    def msg_size(self):
        return 128

    def start_producer(self):
        self.logger.info(
            f"starting kgo-verifier producer with {self.msg_count} messages of size {self.msg_size} and throughput: {self.producer_throughput} bps"
        )
        self.producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self._topic,
            self.msg_size,
            self.msg_count,
            custom_node=self.preallocated_nodes,
            rate_limit_bps=self.producer_throughput)

        self.producer.start(clean=False)
        self.producer.wait_for_acks(
            5 * (self.producer_throughput / self.msg_size), 120, 1)

    @cluster(num_nodes=4)
    @skip_debug_mode
    def test_enabling_tiered_storage_on_old_topic(self):
        # disable cloud storage and restart cluster
        self.redpanda.set_cluster_config({"cloud_storage_enabled": False},
                                         expect_restart=True)
        # create topic without tiered storage enabled
        topic = TopicSpec(partition_count=3,
                          segment_bytes=1024 * 1024,
                          retention_bytes=5 * 1024 * 1024)

        self.client().create_topic(topic)
        self._topic = topic.name
        self.start_producer()
        rpk = RpkTool(self.redpanda)

        def _start_offset_updated():
            partitions = rpk.describe_topic(self._topic)
            return all([p.start_offset > 0 for p in partitions])

        wait_until(
            _start_offset_updated,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=
            "timed out waiting for local retention to clean up some some data")

        # enable cloud storage
        self.redpanda.set_cluster_config({"cloud_storage_enabled": True},
                                         expect_restart=True)

        self.redpanda.wait_for_manifest_uploads()
