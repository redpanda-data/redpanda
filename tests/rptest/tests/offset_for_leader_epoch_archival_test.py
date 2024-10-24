# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from math import fabs
from rptest.services.cluster import cluster
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.clients.kcl import KCL
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SISettings, MetricsEndpoint
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.util import (
    produce_until_segments,
    wait_for_local_storage_truncate,
    KafkaCliTools,
)


class OffsetForLeaderEpochArchivalTest(RedpandaTest):
    """
    Check offset for leader epoch handling
    """
    segment_size = 1 * (2 << 20)
    local_retention = segment_size * 2

    def _produce(self, topic, msg_cnt):
        rpk = RpkTool(self.redpanda)
        for i in range(0, msg_cnt):
            rpk.produce(topic, f"k-{i}", f"v-{i}")

    def __init__(self, test_context):
        self.extra_rp_conf = {
            'enable_leader_balancer': False,
            "log_compaction_interval_ms": 1000,
            "cloud_storage_spillover_manifest_size": None,
        }
        si_settings = SISettings(
            test_context,
            log_segment_size=OffsetForLeaderEpochArchivalTest.segment_size,
            cloud_storage_cache_size=5 *
            OffsetForLeaderEpochArchivalTest.segment_size)
        if test_context.function_name == 'test_querying_archive':
            self.extra_rp_conf[
                'cloud_storage_spillover_manifest_max_segments'] = 10
            si_settings.log_segment_size = 1024
            si_settings.fast_uploads = True,
            si_settings.cloud_storage_housekeeping_interval_ms = 10000

        super(OffsetForLeaderEpochArchivalTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=self.extra_rp_conf,
                             si_settings=si_settings)

    def _alter_topic_retention_with_retry(self, topic):
        rpk = RpkTool(self.redpanda)

        def alter_and_verify():
            try:
                rpk.alter_topic_config(
                    topic, TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES,
                    OffsetForLeaderEpochArchivalTest.local_retention)

                cfgs = rpk.describe_topic_configs(topic)
                retention = int(
                    cfgs[TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES][0])
                return retention == OffsetForLeaderEpochArchivalTest.local_retention
            except:
                return False

        wait_until(alter_and_verify, 15, 0.5)

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(remote_reads=[False, True])
    def test_querying_remote_partitions(self, remote_reads):
        topic = TopicSpec(redpanda_remote_read=True,
                          redpanda_remote_write=True)
        epoch_offsets = {}
        rpk = RpkTool(self.redpanda)
        self.client().create_topic(topic)
        remote_reads_str = 'true' if remote_reads else 'false'
        rpk.alter_topic_config(topic.name, "redpanda.remote.read",
                               remote_reads_str)
        rpk.alter_topic_config(topic.name, "redpanda.remote.write", 'true')
        desc = rpk.describe_topic_configs(topic.name)

        assert desc['redpanda.remote.read'][0] == remote_reads_str
        assert desc['redpanda.remote.write'][0] == 'true'

        def wait_for_topic():
            wait_until(lambda: len(list(rpk.describe_topic(topic.name))) > 0,
                       30,
                       backoff_sec=2)

        # restart whole cluster 6 times to trigger term rolls
        for i in range(0, 6):
            wait_for_topic()
            produce_until_segments(
                redpanda=self.redpanda,
                topic=topic.name,
                partition_idx=0,
                count=2 * i,
            )
            res = list(rpk.describe_topic(topic.name))
            epoch_offsets[res[0].leader_epoch] = res[0].high_watermark
            self.redpanda.restart_nodes(self.redpanda.nodes)

        self.logger.info(f"ledear epoch high watermarks: {epoch_offsets}")

        wait_for_topic()

        self._alter_topic_retention_with_retry(topic.name)

        wait_for_local_storage_truncate(self.redpanda,
                                        topic.name,
                                        target_bytes=self.local_retention)
        kcl = KCL(self.redpanda)

        for epoch, offset in epoch_offsets.items():
            self.logger.info(f"querying partition epoch {epoch} end offsets")
            epoch_end_offset = kcl.offset_for_leader_epoch(
                topics=topic.name, leader_epoch=epoch)[0].epoch_end_offset
            self.logger.info(
                f"epoch {epoch} end_offset: {epoch_end_offset}, expected offset: {offset}"
            )
            if remote_reads:
                assert epoch_end_offset == offset, f"{epoch_end_offset} vs {offset}"
            else:
                # Check that the returned offset isn't an invalid (-1) value,
                # even if we read from an epoch that has been truncated locally
                # and we can't read from cloud storage.
                assert epoch_end_offset != -1, f"{epoch_end_offset} vs -1"
                assert epoch_end_offset >= offset, f"{epoch_end_offset} vs {offset}"

    def num_manifests_uploaded(self):
        s = self.redpanda.metric_sum(
            metric_name=
            "redpanda_cloud_storage_spillover_manifest_uploads_total",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        self.logger.info(
            f"redpanda_cloud_storage_spillover_manifest_uploads = {s}")
        return s

    def produce(self, topic, msg_count):
        msg_size = 1024 * 256
        topic_name = topic.name
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic_name,
                                       msg_size=msg_size,
                                       msg_count=msg_count)

        producer.start()
        producer.wait()
        producer.free()

        def all_partitions_spilled():
            return self.num_manifests_uploaded() > 0

        wait_until(all_partitions_spilled, timeout_sec=180, backoff_sec=10)

    @cluster(num_nodes=4, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_querying_archive(self):
        topic = TopicSpec(redpanda_remote_read=True,
                          redpanda_remote_write=True)
        epoch_offsets = {}

        self.client().create_topic(topic)

        rpk = RpkTool(self.redpanda)

        def wait_for_topic():
            wait_until(lambda: len(list(rpk.describe_topic(topic.name))) > 0,
                       30,
                       backoff_sec=2)

        def produce_until_spillover():
            initial_uploaded = self.num_manifests_uploaded()

            def new_manifest_spilled():
                self.produce(topic, 250)
                num_spilled = self.num_manifests_uploaded()
                return num_spilled > initial_uploaded

            wait_until(new_manifest_spilled,
                       timeout_sec=120,
                       backoff_sec=2,
                       err_msg="Manifests were not created")

        for _ in range(0, 8):
            wait_for_topic()
            produce_until_spillover()
            res = list(rpk.describe_topic(topic.name))
            epoch_offsets[res[0].leader_epoch] = res[0].high_watermark
            self.redpanda.restart_nodes(self.redpanda.nodes)

        # Enable local retention for the topic to force reading from the 'archive'
        # section of the log and wait for the housekeeping to finish.
        wait_for_topic()
        rpk = RpkTool(self.redpanda)

        def topic_config_altered():
            try:
                rpk.alter_topic_config(topic.name,
                                       'retention.local.target.bytes', 0x1000)
                return True
            except:
                return False

        wait_until(topic_config_altered,
                   timeout_sec=120,
                   backoff_sec=2,
                   err_msg="Can't update topic config")

        for pix in range(0, topic.partition_count):
            wait_for_local_storage_truncate(self.redpanda,
                                            topic.name,
                                            target_bytes=0x2000,
                                            partition_idx=pix,
                                            timeout_sec=30)

        self.logger.info(f"leader epoch high watermarks: {epoch_offsets}")

        kcl = KCL(self.redpanda)

        for epoch, offset in epoch_offsets.items():
            self.logger.info(f"querying partition epoch {epoch} end offsets")
            epoch_end_offset = kcl.offset_for_leader_epoch(
                topics=topic.name, leader_epoch=epoch)[0].epoch_end_offset
            self.logger.info(
                f"epoch {epoch} end_offset: {epoch_end_offset}, expected offset: {offset}"
            )
            assert epoch_end_offset == offset, f"{epoch_end_offset} vs {offset}"
