# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.clients.kcl import KCL
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SISettings, MetricsEndpoint, make_redpanda_service
from rptest.tests.end_to_end import EndToEndTest
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.services.admin import Admin
import time
import random


class OffsetForLeaderEpochReadReplicaTest(EndToEndTest):
    # Size of the segment in the local storage
    segment_size = 0x2000
    # Generate at least 100 terms
    num_terms = 20

    def __init__(self, test_context):
        self.extra_rp_conf = {
            'enable_leader_balancer': False,
            "log_compaction_interval_ms": 1000
        }
        super(OffsetForLeaderEpochReadReplicaTest, self).__init__(
            test_context=test_context,
            si_settings=SISettings(
                test_context,
                cloud_storage_max_connections=5,
                log_segment_size=OffsetForLeaderEpochReadReplicaTest.
                segment_size,
                cloud_storage_enable_remote_write=True,
                cloud_storage_enable_remote_read=True,
                cloud_storage_readreplica_manifest_sync_timeout_ms=500,
                cloud_storage_segment_max_upload_interval_sec=5,
                fast_uploads=True),
            extra_rp_conf=self.extra_rp_conf)

        self.rr_settings = SISettings(
            test_context,
            bypass_bucket_creation=True,
            cloud_storage_enable_remote_write=False,
            cloud_storage_max_connections=5,
            cloud_storage_readreplica_manifest_sync_timeout_ms=100,
            cloud_storage_segment_max_upload_interval_sec=5,
            cloud_storage_housekeeping_interval_ms=10)

        self.topic_name = "panda-topic"
        self.second_cluster = None
        self.epoch_offsets = {}

    def start_second_cluster(self) -> None:
        # NOTE: the RRR cluster won't have a bucket, so don't upload.
        extra_rp_conf = dict(enable_cluster_metadata_upload_loop=False)
        self.second_cluster = make_redpanda_service(
            self.test_context,
            num_brokers=3,
            si_settings=self.rr_settings,
            extra_rp_conf=extra_rp_conf)
        self.second_cluster.start(start_si=False)

    def create_source_topic(self):
        self.rpk_client().create_topic(self.topic_name,
                                       partitions=1,
                                       replicas=3)

    def create_read_replica_topic(self) -> None:
        rpk_dst_cluster = RpkTool(self.second_cluster)
        bucket_name = self.si_settings.cloud_storage_bucket

        conf = {
            'redpanda.remote.readreplica': bucket_name,
        }

        rpk_dst_cluster.create_topic(self.topic_name, config=conf)

        def has_leader():
            partitions = list(
                rpk_dst_cluster.describe_topic(self.topic_name, tolerant=True))
            for part in partitions:
                if part.leader == -1:
                    return False
            return True

        wait_until(has_leader,
                   timeout_sec=60,
                   backoff_sec=10,
                   err_msg="Failed to create a read-replica, no leadership")

    def produce(self, topic, msg_count):
        msg_size = 64
        for _ in range(0, self.num_terms):
            KgoVerifierProducer.oneshot(context=self.test_context,
                                        redpanda=self.redpanda,
                                        topic=topic,
                                        msg_size=msg_size,
                                        msg_count=100,
                                        use_transactions=False,
                                        debug_logs=True)
            self.transfer_leadership()
            time.sleep(1)

    def transfer_leadership(self):
        res = list(self.rpk_client().describe_topic(self.topic_name))
        self.epoch_offsets[res[0].leader_epoch] = res[0].high_watermark
        self.logger.info(
            f"leadership transfer, epoch {res[0]}, high watermark {res[0]}")

        admin = Admin(self.redpanda)
        leader = admin.get_partition_leader(namespace='kafka',
                                            topic=self.topic_name,
                                            partition=0)

        broker_ids = [x['node_id'] for x in admin.get_brokers()]
        transfer_to = random.choice([n for n in broker_ids if n != leader])
        admin.transfer_leadership_to(namespace="kafka",
                                     topic=self.topic_name,
                                     partition=0,
                                     target_id=transfer_to,
                                     leader_id=leader)

        admin.await_stable_leader(self.topic_name,
                                  partition=0,
                                  namespace='kafka',
                                  timeout_s=20,
                                  backoff_s=2,
                                  check=lambda node_id: node_id == transfer_to)

    def query_offset_for_leader_epoch(self, query_rrr):
        self.logger.info(
            f"query offset for leader epoch started, RRR={query_rrr}")

        kcl = KCL(self.second_cluster) if query_rrr else KCL(self.redpanda)

        for epoch, offset in self.epoch_offsets.items():

            def check():
                self.logger.info(
                    f"querying epoch {epoch} end offsets, expected to get {offset}"
                )
                epoch_end_offset = kcl.offset_for_leader_epoch(
                    topics=self.topic_name,
                    leader_epoch=epoch)[0].epoch_end_offset
                if epoch_end_offset < 0:
                    # NOT_LEADER_FOR_PARTITION error, retry
                    return False
                self.logger.info(
                    f"epoch {epoch} end_offset: {epoch_end_offset}, expected offset: {offset}"
                )
                assert epoch_end_offset == offset, f"{epoch_end_offset} vs {offset}"
                return True

            wait_until(check,
                       timeout_sec=30,
                       backoff_sec=10,
                       err_msg=f"Failed query epoch/offset {epoch}/{offset}")

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_offset_for_leader_epoch(self):
        self.start_redpanda(num_nodes=3)
        self.create_source_topic()
        self.produce(self.topic_name, 1000)
        self.query_offset_for_leader_epoch(False)
        self.start_second_cluster()
        self.create_read_replica_topic()
        self.query_offset_for_leader_epoch(True)
