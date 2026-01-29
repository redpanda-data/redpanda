# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re

from ducktape.mark import matrix
from ducktape.tests.test import TestContext

from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin, InboundTopic, NamespacedTopic
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import (
    RedpandaService,
    SISettings,
    get_cloud_storage_type,
    make_redpanda_service,
)
from rptest.tests.read_replica_e2e_test import (
    create_read_replica_topic,
    hwms_are_identical,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until
from rptest.utils.si_utils import NT, BucketView, quiesce_uploads


class RemoteLabelsTest(RedpandaTest):
    """
    Tests that exercise multiple clusters sharing a single bucket.
    """

    def __init__(self, test_context: TestContext):
        self.extra_rp_conf = dict(
            cloud_storage_spillover_manifest_size=None,
            cloud_storage_topic_purge_grace_period_ms=1000,
            retention_local_target_capacity_bytes=1024,
            retention_local_trim_interval=1000,
            log_compaction_interval_ms=100,
            log_segment_ms_min=1000,
            log_segment_ms=1000,
        )
        super(RemoteLabelsTest, self).__init__(
            num_brokers=1,
            test_context=test_context,
            extra_rp_conf=self.extra_rp_conf,
            si_settings=SISettings(
                test_context,
                log_segment_size=1024,
                fast_uploads=True,
                cloud_storage_housekeeping_interval_ms=1000,
                cloud_storage_spillover_manifest_max_segments=10,
            ),
        )
        self.redpanda.set_environment(
            {"__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"}
        )

        # Set up si_settings so new clusters to reuse the same bucket.
        self.new_cluster_si_settings = SISettings(
            test_context,
            log_segment_size=1024,
            fast_uploads=True,
            cloud_storage_housekeeping_interval_ms=1000,
            cloud_storage_spillover_manifest_max_segments=10,
        )
        self.new_cluster_si_settings.bypass_bucket_creation = True
        self.new_cluster_si_settings.reset_cloud_storage_bucket(
            self.si_settings.cloud_storage_bucket
        )
        self.partition_count = 5

        self.extra_clusters: list[RedpandaService] = []

    def start_new_cluster(self) -> RedpandaService:
        new_cluster = make_redpanda_service(
            self.test_context,
            num_brokers=1,
            si_settings=self.new_cluster_si_settings,
            extra_rp_conf=self.extra_rp_conf,
        )
        new_cluster.set_environment(
            {"__REDPANDA_TEST_DISABLE_BOUNDED_PROPERTY_CHECKS": "ON"}
        )
        new_cluster.start()
        self.extra_clusters.append(new_cluster)
        return new_cluster

    def create_topic(
        self, cluster: RedpandaService, topic_name: str, partitions: int | None = None
    ) -> None:
        spec = TopicSpec(
            name=topic_name,
            partition_count=self.partition_count if partitions is None else partitions,
            replication_factor=1,
        )
        DefaultClient(cluster).create_topic(spec)

    def create_read_replica_topic(
        self, cluster: RedpandaService, topic_name: str
    ) -> None:
        rpk = RpkTool(cluster)
        conf = {
            "redpanda.remote.readreplica": self.si_settings.cloud_storage_bucket,
        }
        rpk.create_topic(topic_name, config=conf)

    def produce(
        self, cluster: RedpandaService, topic_name: str, num_records: int
    ) -> None:
        producer = KgoVerifierProducer(
            self.test_context,
            cluster,
            topic_name,
            msg_size=2056,
            msg_count=num_records,
            debug_logs=True,
            trace_logs=True,
        )
        producer.start()
        producer.wait(timeout_sec=60)
        producer.free()

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_share_bucket_delete_topic(self, cloud_storage_type) -> None:
        """
        cluster 1 creates topic_a
        cluster 2 creates topic_a
        cluster 1 deletes topic_a
        cluster 1 creates RRR to cluster 2 topic_a
        """
        topic_name = "topic-a"
        new_cluster = self.start_new_cluster()
        self.create_topic(self.redpanda, topic_name)

        # Produce some to the first cluster.
        num_records = 200
        self.produce(self.redpanda, topic_name, num_records)
        first_cluster_uuid = self.redpanda._admin.get_cluster_uuid()
        quiesce_uploads(self.redpanda, [topic_name], 30, first_cluster_uuid)

        # Produce some to the next cluster.
        new_cluster_uuid = new_cluster._admin.get_cluster_uuid()
        self.create_topic(new_cluster, topic_name)
        self.produce(new_cluster, topic_name, num_records)
        quiesce_uploads(new_cluster, [topic_name], 30, new_cluster_uuid)

        # Delete the topic on the first cluster. This shouldn't affect the
        # second cluster.
        DefaultClient(self.redpanda).delete_topic(topic_name)

        def topic_manifest_deleted():
            try:
                BucketView(self.redpanda).get_topic_manifest(
                    NT("kafka", topic_name), first_cluster_uuid
                )
            except KeyError:
                return True
            return False

        wait_until(topic_manifest_deleted, backoff_sec=1, timeout_sec=30)

        # Point the first cluster at the second cluster's data.
        create_read_replica_topic(
            self.redpanda, topic_name, self.si_settings.cloud_storage_bucket
        )

        # We should see that the clusters match, and that we can consume the
        # right number of records from the first cluster.
        def clusters_report_identical_hwms():
            return hwms_are_identical(
                self.logger,
                self.redpanda,
                new_cluster,
                topic_name,
                self.partition_count,
            )

        wait_until(clusters_report_identical_hwms, timeout_sec=30, backoff_sec=1)
        rpk = RpkTool(self.redpanda)
        out = rpk.consume(topic_name, format="%p,%o\n", n=num_records)
        out_lines = out.splitlines()
        assert len(out_lines) == num_records, (
            f"output has {len(out_lines)} lines: {out}"
        )

    @cluster(
        num_nodes=2,
        log_allow_list=[re.compile("No such file or directory.*cloud_storage_cache")],
    )
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_share_bucket_concurrent_consume(self, cloud_storage_type) -> None:
        """
        - cluster 1 creates topic_a
        - cluster 2 creates topic_a
        - cluster 1 produces to topic_a with pattern X
        - cluster 2 produces to topic_a with pattern Y
        - both clusters continue producing until there are many segments in
          cloud, and wait for local data to be reclaimed
        - both clusters verify their workloads independently
        - both clusters unmount and then mount the other clusters' topic
        - verify the swapped workloads
        """
        topic_name = "topic-a"
        c1 = self.redpanda
        c2 = self.start_new_cluster()
        self.create_topic(c1, topic_name, partitions=1)
        self.create_topic(c2, topic_name, partitions=1)

        # Produce different data to each topic.
        rpk_c1 = RpkTool(c1)
        rpk_c2 = RpkTool(c2)
        num_msgs_per_round = 100
        msg_len = 1024

        def _local_trimmed():
            def _cluster_local_trimmed(cluster: RedpandaService):
                admin = Admin(cluster)
                status = admin.get_partition_cloud_storage_status(topic_name, 0)
                return status["local_log_start_offset"] > 0

            # Once we trim local storage from both clusters, we're done.
            return _cluster_local_trimmed(c1) and _cluster_local_trimmed(c2)

        for _ in range(num_msgs_per_round):
            rpk_c1.produce(topic_name, key="1", msg="1" * msg_len)
            rpk_c2.produce(topic_name, key="2", msg="2" * msg_len)

        # Keep producing until space management kicks out the local data.
        wait_until(_local_trimmed, timeout_sec=30, backoff_sec=1)

        def wipe_cloud_cache(cluster: RedpandaService):
            cache_dir = cluster.cache_dir
            cluster.for_nodes(
                cluster.nodes, lambda n: n.account.ssh(f"rm -rf {cache_dir}/*")
            )

        # Wipe both clusters' cloud caches to ensure reads will go from S3.
        wipe_cloud_cache(c1)
        wipe_cloud_cache(c2)

        def consume_topic(cluster: RedpandaService, missing_char):
            rpk = RpkTool(cluster)

            def partition_hwms():
                return [p.high_watermark for p in rpk.describe_topic(topic_name)]

            wait_until(
                lambda: len(partition_hwms()) == 1, timeout_sec=10, backoff_sec=1
            )

            hwm = partition_hwms()[0]
            assert hwm > 0, f"Expected non-zero hwm: {hwm}"
            out = rpk.consume(topic_name, format="%k,%v\n", offset=0, n=hwm)
            assert missing_char not in out, f"Expected missing '{missing_char}': {out}"

            # Filter out empty lines and check we have as many records as the
            # HWM claims.
            rows = list(filter(None, out.split("\n")))
            assert len(rows) == hwm, f"Expected {hwm} rows: {len(rows)}, {rows}"

        consume_topic(c1, missing_char="2")
        consume_topic(c2, missing_char="1")

        # Unmount the topics.
        admin_c1 = Admin(c1)
        admin_c2 = Admin(c2)
        ns_topic = NamespacedTopic(topic_name)
        admin_c1.unmount_topics([ns_topic])
        admin_c2.unmount_topics([ns_topic])
        c1_uuid = admin_c1.get_cluster_uuid()
        c2_uuid = admin_c2.get_cluster_uuid()

        wait_until(
            lambda: len(list(rpk_c1.list_topics())) == 0, timeout_sec=10, backoff_sec=1
        )
        wait_until(
            lambda: len(list(rpk_c2.list_topics())) == 0, timeout_sec=10, backoff_sec=1
        )

        # Remount them swapped.
        admin_c1.mount_topics(
            [InboundTopic(NamespacedTopic(f"{topic_name}/{c2_uuid}"))]
        )
        admin_c2.mount_topics(
            [InboundTopic(NamespacedTopic(f"{topic_name}/{c1_uuid}"))]
        )

        wait_until(
            lambda: len(list(rpk_c1.list_topics())) == 1, timeout_sec=10, backoff_sec=1
        )
        wait_until(
            lambda: len(list(rpk_c2.list_topics())) == 1, timeout_sec=10, backoff_sec=1
        )
        consume_topic(c1, missing_char="1")
        consume_topic(c2, missing_char="2")
