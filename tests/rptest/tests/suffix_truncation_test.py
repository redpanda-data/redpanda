# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import google.protobuf.duration_pb2

from connectrpc.errors import ConnectError, ConnectErrorCode
from ducktape.utils.util import wait_until
from typing import NewType

from rptest.clients.admin.v2 import Admin as AdminV2
from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    shadow_link_pb2,
    shadow_link_pb2_connect,
)
from rptest.clients.types import TopicSpec
from rptest.services.admin import (
    Admin,
    InboundDataMigration,
    InboundTopic,
    MigrationAction,
    NamespacedTopic,
    OutboundDataMigration,
)
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool, RpkPartition
from rptest.services.redpanda import SISettings
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.data_migrations_api_test import make_namespaced_topic
from rptest.utils.data_migrations import DataMigrationTestMixin
from rptest.utils.si_utils import NTP, BucketView


PartitionId = NewType("PartitionId", int)
KafkaOffset = NewType("KafkaOffset", int)


class SuffixTruncationTest(RedpandaTest, DataMigrationTestMixin):
    def __init__(self, ctx, *args, **kwargs):
        super().__init__(
            test_context=ctx,
            num_brokers=3,
            si_settings=SISettings(
                test_context=ctx,
                fast_uploads=True,
                log_segment_size=100 * 1024,
            ),
            *args,
            **kwargs,
        )
        self.admin: Admin
        self.admin_v2: AdminV2
        self.client: shadow_link_pb2_connect.ShadowLinkServiceClient
        self.topic_name = "foo"
        self.partition_count = 5
        self.my_topic = TopicSpec(
            name=self.topic_name,
            partition_count=self.partition_count,
            replication_factor=3,
        )
        self.rp_client: DefaultClient

    def setUp(self):
        super().setUp()
        self.admin = Admin(self.redpanda)
        self.admin_v2 = AdminV2(self.redpanda)
        self.client = self.admin_v2.shadow_link()
        self.rp_client = DefaultClient(self.redpanda)

    def wait_migration_disappear(self, migration_id):
        def migration_is_absent(id: int):
            return self.on_all_live_nodes(id, lambda m: m is None)

        wait_until(
            lambda: migration_is_absent(migration_id),
            timeout_sec=90,
            backoff_sec=2,
            err_msg=f"Expected migration with id {migration_id} is absent",
        )

    def get_topic_initial_revision(self, topic_name):
        anomalies = self.admin.get_cloud_storage_anomalies(
            namespace="kafka", topic=topic_name, partition=0
        )
        return anomalies["revision_id"]

    def check_topic(
        self, expected: dict[PartitionId, KafkaOffset] = {}, log: str | None = None
    ) -> list[RpkPartition]:
        res = list(RpkTool(self.redpanda).describe_topic(self.topic_name))
        dump = json.dumps(list(str(p) for p in res), indent=1)
        if log is not None:
            self.logger.warn(f"{log}: {dump}")
        for p in res:
            exp_hwm = expected.get(p.id, p.high_watermark)
            assert p.high_watermark <= exp_hwm, (
                f"{p.id=}: Expected {p.high_watermark=} <= {exp_hwm}"
            )
        return res

    def check_partition(self, ntp: NTP, num_messages: int) -> bool:
        view = BucketView(self.redpanda)
        manifest = view.get_partition_manifest(ntp)
        last_offset = BucketView.kafka_last_offset(manifest)
        return last_offset is not None and last_offset + 1 >= num_messages

    @cluster(num_nodes=4)
    def test_restore_topic(self):
        self.rp_client.create_topic(self.my_topic)

        for i in range(0, 5):
            KgoVerifierProducer.oneshot(
                context=self.test_context,
                redpanda=self.redpanda,
                topic=self.topic_name,
                msg_size=1024,
                msg_count=2048,
                key_set_cardinality=128,
                # batch_max_bytes=2048,
            )

        partition_info = [
            shadow_link_pb2.RestoreTopicPartitionInfo(
                partition_id=p.id, last_offset=p.high_watermark - 2
            )
            for p in self.check_topic(log="INITIAL")
        ]

        restore_topics = [
            shadow_link_pb2.RestoreTopic(
                name=self.topic_name, partitions=partition_info
            )
        ]

        initial_revision = self.get_topic_initial_revision(self.topic_name)

        unmount_req = shadow_link_pb2.TruncateAndRestoreRequest(
            topics=restore_topics,
            action=shadow_link_pb2.RESTORE_ACTION_UNMOUNT_TOPIC,
        )
        rsp = self.client.truncate_and_restore(unmount_req)
        self.wait_partitions_disappear([self.topic_name])
        self.wait_migration_disappear(rsp.migration_id)

        for p in partition_info:
            self.check_partition(
                ntp=NTP(ns="kafka", topic=self.topic_name, partition=p.partition_id),
                num_messages=p.last_offset,
            )

        cluster_uuid = self.admin.get_cluster_uuid(self.redpanda.nodes[0])
        source_topic_ref = f"{self.topic_name}/{cluster_uuid}/{initial_revision}"

        mount_req = shadow_link_pb2.TruncateAndRestoreRequest(
            topics=restore_topics,
            action=shadow_link_pb2.RESTORE_ACTION_MOUNT_AND_TRUNCATE_TOPIC,
        )
        rsp = self.client.truncate_and_restore(mount_req)
        self.wait_partitions_appear([self.my_topic], timeout_sec=30)
        self.wait_migration_disappear(rsp.migration_id)

        wait_until(
            lambda: len(self.check_topic()) == self.partition_count,
            timeout_sec=30,
            backoff_sec=1,
        )

        # expected max high watermark for each truncated partition
        expected = {p.partition_id: p.last_offset + 1 for p in partition_info}
        self.check_topic(expected=expected, log="REMOUNT")

        KgoVerifierProducer.oneshot(
            context=self.test_context,
            redpanda=self.redpanda,
            topic=self.topic_name,
            msg_size=1024,
            msg_count=2048,
            key_set_cardinality=128,
        )

        self.check_topic(log="FINAL")

        KgoVerifierSeqConsumer.oneshot(
            self.test_context,
            self.redpanda,
            self.topic_name,
            debug_logs=True,
            timeout_sec=30,
        )
