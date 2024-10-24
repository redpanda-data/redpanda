# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.default import DefaultClient
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.clients.rpk import RpkTool
from rptest.services.redpanda import RedpandaService, SISettings, make_redpanda_service
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.read_replica_e2e_test import hwms_are_identical, create_read_replica_topic
from rptest.util import wait_until
from ducktape.tests.test import TestContext
from rptest.utils.si_utils import BucketView, NT, quiesce_uploads


class RemoteLabelsTest(RedpandaTest):
    """
    Tests that exercise multiple clusters sharing a single bucket.
    """
    def __init__(self, test_context: TestContext):
        extra_rp_conf = dict(cloud_storage_spillover_manifest_size=None,
                             cloud_storage_topic_purge_grace_period_ms=1000)
        super(RemoteLabelsTest, self).__init__(
            num_brokers=1,
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
            si_settings=SISettings(
                test_context,
                log_segment_size=1024,
                fast_uploads=True,
                cloud_storage_housekeeping_interval_ms=1000,
                cloud_storage_spillover_manifest_max_segments=10))

        # Set up si_settings so new clusters to reuse the same bucket.
        self.new_cluster_si_settings = SISettings(
            test_context,
            log_segment_size=1024,
            fast_uploads=True,
            cloud_storage_housekeeping_interval_ms=1000,
            cloud_storage_spillover_manifest_max_segments=10)
        self.new_cluster_si_settings.bypass_bucket_creation = True
        self.new_cluster_si_settings.reset_cloud_storage_bucket(
            self.si_settings.cloud_storage_bucket)
        self.partition_count = 5

        self.extra_clusters: list[RedpandaService] = []

    def start_new_cluster(self) -> RedpandaService:
        new_cluster = make_redpanda_service(
            self.test_context,
            num_brokers=1,
            si_settings=self.new_cluster_si_settings)
        new_cluster.start()
        self.extra_clusters.append(new_cluster)
        return new_cluster

    def create_topic(self, cluster: RedpandaService, topic_name: str) -> None:
        spec = TopicSpec(name=topic_name,
                         partition_count=self.partition_count,
                         replication_factor=1)
        DefaultClient(cluster).create_topic(spec)

    def create_read_replica_topic(self, cluster: RedpandaService,
                                  topic_name: str) -> None:
        rpk = RpkTool(cluster)
        conf = {
            'redpanda.remote.readreplica':
            self.si_settings.cloud_storage_bucket,
        }
        rpk.create_topic(topic_name, config=conf)

    def produce(self, cluster: RedpandaService, topic_name: str,
                num_records: int) -> None:
        producer = KgoVerifierProducer(self.test_context,
                                       cluster,
                                       topic_name,
                                       msg_size=2056,
                                       msg_count=num_records,
                                       debug_logs=True,
                                       trace_logs=True)
        producer.start()
        producer.wait(timeout_sec=60)
        producer.free()

    @cluster(num_nodes=3)
    def test_clusters_share_bucket(self) -> None:
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
                    NT("kafka", topic_name), first_cluster_uuid)
            except KeyError:
                return True
            return False

        wait_until(topic_manifest_deleted, backoff_sec=1, timeout_sec=30)

        # Point the first cluster at the second cluster's data.
        create_read_replica_topic(self.redpanda, topic_name,
                                  self.si_settings.cloud_storage_bucket)

        # We should see that the clusters match, and that we can consume the
        # right number of records from the first cluster.
        def clusters_report_identical_hwms():
            return hwms_are_identical(self.logger, self.redpanda, new_cluster,
                                      topic_name, self.partition_count)

        wait_until(clusters_report_identical_hwms,
                   timeout_sec=30,
                   backoff_sec=1)
        rpk = RpkTool(self.redpanda)
        out = rpk.consume(topic_name, format="%p,%o\n", n=num_records)
        out_lines = out.splitlines()
        assert len(
            out_lines
        ) == num_records, f"output has {len(out_lines)} lines: {out}"
