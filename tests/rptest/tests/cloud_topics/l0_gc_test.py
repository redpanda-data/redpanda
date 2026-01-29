# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
from rptest.context.cloud_storage import CloudStorageType
from rptest.services.kgo_repeater_service import repeater_traffic
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from ducktape.tests.test import TestContext
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    SISettings,
    get_cloud_storage_type,
    CLOUD_TOPICS_CONFIG_STR,
)
from rptest.tests.redpanda_test import RedpandaTest


class CloudTopicsL0GCTest(RedpandaTest):
    def __init__(self, test_context: TestContext):
        self.test_context = test_context
        si_settings = SISettings(
            test_context=test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )
        extra_rp_conf = {
            CLOUD_TOPICS_CONFIG_STR: True,
            "cloud_topics_reconciliation_min_interval": 2000,
            "cloud_topics_reconciliation_max_interval": 2000,
            "cloud_topics_epoch_service_epoch_increment_interval": 5000,
            "cloud_topics_epoch_service_local_epoch_cache_duration": 5000,
            "cloud_topics_short_term_gc_minimum_object_age": 10000,
            "cloud_topics_short_term_gc_interval": 2000,
            "cloud_topics_short_term_gc_backoff_interval": 10000,
        }
        super(CloudTopicsL0GCTest, self).__init__(
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
            si_settings=si_settings,
        )

    def __create_topics(self, topics: list[TopicSpec]):
        rpk = RpkTool(self.redpanda)
        for spec in topics:
            rpk.create_topic(
                spec.name,
                spec.partition_count,
                spec.replication_factor,
                config={"redpanda.cloud_topic.enabled": "true"},
            )

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_l0_gc(self, cloud_storage_type: CloudStorageType):
        self.topics = [TopicSpec(partition_count=2)]
        self.__create_topics(self.topics)

        with repeater_traffic(
            context=self.test_context,
            redpanda=self.redpanda,
            topics=[spec.name for spec in self.topics],
            msg_size=1024,
            rate_limit_bps=2 * 1024 * 1024,
            workers=1,
        ) as repeater:
            repeater.await_group_ready()
            repeater.await_progress(300, timeout_sec=90)

        # TODO: we are only checking that deletes are happening here (and should
        # also be happening in parallel with the repeater's fetch/produce
        # workload), but we do want to add tests that check constraints
        def get_num_objects_deleted():
            samples = self.redpanda.metrics_sample(
                "vectorized_cloud_topics_l0_gc_objects_deleted_total"
            )
            self.logger.info(samples)
            if samples is not None and samples.samples:
                return int(sum(s.value for s in samples.samples))
            return 0

        wait_until(
            lambda: get_num_objects_deleted() > 0,
            timeout_sec=30,
            backoff_sec=5,
            retry_on_exc=True,
        )
