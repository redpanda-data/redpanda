# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import json
import random
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until
from requests.exceptions import HTTPError

from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkException
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.action_injector import random_process_kills
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer, \
    KgoVerifierRandomConsumer, KgoVerifierSeqConsumer
from rptest.services.metrics_check import MetricCheck
from rptest.services.redpanda import SISettings, get_cloud_storage_type, make_redpanda_service, CHAOS_LOG_ALLOW_LIST, \
    MetricsEndpoint
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import Scale, wait_until_segments
from rptest.util import (
    produce_until_segments,
    wait_for_removal_of_n_segments,
    wait_for_local_storage_truncate,
)
from rptest.utils.mode_checks import skip_debug_mode
from rptest.utils.si_utils import nodes_report_cloud_segments, BucketView, NTP, quiesce_uploads

# This is allowed because manifest reset test disables remote.write dynamically which may race with
# ntp_archiver startup/shutdown
REST_LOG_ALLOW_LIST = [
    "Adjacent segment merging refusing to run on topic with remote.write disabled"
]


class EndToEndShadowIndexingBase(EndToEndTest):
    segment_size = 1048576  # 1 Mb
    chunk_size = segment_size * 0.75
    s3_topic_name = "panda-topic"

    num_brokers = 3

    topics = (TopicSpec(
        name=s3_topic_name,
        partition_count=1,
        replication_factor=3,
    ), )

    def __init__(self, test_context, extra_rp_conf=None, environment=None):
        super(EndToEndShadowIndexingBase,
              self).__init__(test_context=test_context)

        if environment is None:
            environment = {'__REDPANDA_TOPIC_REC_DL_CHECK_MILLIS': 5000}
        self.test_context = test_context
        self.topic = self.s3_topic_name

        conf = dict(
            enable_cluster_metadata_upload_loop=True,
            cloud_storage_cluster_metadata_upload_interval_ms=1000,
            # Tests may configure spillover manually.
            cloud_storage_spillover_manifest_size=None,
            controller_snapshot_max_age_sec=1)
        if extra_rp_conf:
            for k, v in conf.items():
                extra_rp_conf[k] = v
        else:
            extra_rp_conf = conf

        self.si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=5,
            log_segment_size=self.segment_size,  # 1MB
            fast_uploads=True,
        )
        self.s3_bucket_name = self.si_settings.cloud_storage_bucket
        self.si_settings.load_context(self.logger, test_context)
        self.scale = Scale(test_context)

        self.redpanda = make_redpanda_service(context=self.test_context,
                                              num_brokers=self.num_brokers,
                                              si_settings=self.si_settings,
                                              extra_rp_conf=extra_rp_conf,
                                              environment=environment)
        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    def setUp(self):
        assert self.redpanda
        self.redpanda.start()
        for topic in self.topics:
            self.kafka_tools.create_topic(topic)


def num_manifests_uploaded(test_self):
    s = test_self.redpanda.metric_sum(
        metric_name="redpanda_cloud_storage_spillover_manifest_uploads_total",
        metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
    test_self.logger.info(
        f"redpanda_cloud_storage_spillover_manifest_uploads = {s}")
    return s


def num_manifests_downloaded(test_self):
    s = test_self.redpanda.metric_sum(
        metric_name="redpanda_cloud_storage_spillover_manifest_downloads_total",
        metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
    test_self.logger.info(
        f"redpanda_cloud_storage_spillover_manifest_downloads = {s}")
    return s


def all_uploads_done(rpk, topic, redpanda, logger):
    topic_description = rpk.describe_topic(topic)
    partition = next(topic_description)

    hwm = partition.high_watermark

    manifest = None
    try:
        bucket = BucketView(redpanda)
        manifest = bucket.manifest_for_ntp(topic, partition.id)
    except Exception as e:
        logger.info(f"Exception thrown while retrieving the manifest: {e}")
        return False

    top_segment = max(manifest['segments'].values(),
                      key=lambda seg: seg['base_offset'])
    uploaded_raft_offset = top_segment['committed_offset']
    uploaded_kafka_offset = uploaded_raft_offset - top_segment[
        'delta_offset_end']
    logger.info(
        f"Remote HWM {uploaded_kafka_offset} (raft {uploaded_raft_offset}), local hwm {hwm}"
    )

    # -1 because uploaded offset is inclusive, hwm is exclusive
    if uploaded_kafka_offset < (hwm - 1):
        return False

    return True


class EndToEndShadowIndexingTest(EndToEndShadowIndexingBase):
    def _all_uploads_done(self):
        return all_uploads_done(self.rpk, self.topic, self.redpanda,
                                self.logger)

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_reset(self, cloud_storage_type):
        brokers = self.redpanda.started_nodes()

        msg_count_before_reset = 50 * (self.segment_size // 2056)
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=2056,
                                       msg_count=msg_count_before_reset,
                                       debug_logs=True,
                                       trace_logs=True)

        producer.start()
        producer.wait(timeout_sec=60)
        producer.free()

        wait_until(lambda: self._all_uploads_done() == True,
                   timeout_sec=60,
                   backoff_sec=5)

        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        manifest = s3_snapshot.manifest_for_ntp(self.topic, 0)

        self.rpk.alter_topic_config(self.topic, 'redpanda.remote.write',
                                    'false')
        time.sleep(1)

        # Tweak the manifest as follows: remove the last 6 segments and update
        # the last offset accordingly.
        sorted_segments = sorted(manifest['segments'].items(),
                                 key=lambda entry: entry[1]['base_offset'])

        for name, meta in sorted_segments[-6:]:
            manifest['segments'].pop(name)

        manifest['last_offset'] = sorted_segments[-7][1]['committed_offset']

        json_man = json.dumps(manifest)
        self.logger.info(f"Re-setting manifest to:{json_man}")

        self.redpanda._admin.unsafe_reset_cloud_metadata(
            self.topic, 0, manifest)

        self.rpk.alter_topic_config(self.topic, 'redpanda.remote.write',
                                    'true')

        msg_count_after_reset = 10 * (self.segment_size // 2056)
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=2056,
                                       msg_count=msg_count_after_reset,
                                       debug_logs=True,
                                       trace_logs=True)

        producer.start()
        producer.wait(timeout_sec=30)
        producer.free()

        wait_until(lambda: self._all_uploads_done() == True,
                   timeout_sec=60,
                   backoff_sec=5)

        # Enable aggresive local retention to test the cloud storage read path.
        self.rpk.alter_topic_config(self.topic, 'retention.local.target.bytes',
                                    self.segment_size * 5)

        wait_for_local_storage_truncate(self.redpanda,
                                        self.topic,
                                        target_bytes=6 * self.segment_size,
                                        partition_idx=0,
                                        timeout_sec=30)

        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          self.topic,
                                          msg_size=2056,
                                          debug_logs=True,
                                          trace_logs=True)

        consumer.start()
        consumer.wait(timeout_sec=60)

        assert consumer.consumer_status.validator.invalid_reads == 0
        assert consumer.consumer_status.validator.valid_reads >= msg_count_before_reset + msg_count_after_reset

    @cluster(num_nodes=4, log_allow_list=REST_LOG_ALLOW_LIST)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_reset_spillover(self, cloud_storage_type):
        """
        Test the unsafe_reset_metadata endpoint for situations when
        then partition manifest includes spillover entries. The test
        waits for the cloud log to stabilise and then removes the first
        entry from the spillover list from the downloaded manifest.
        That's followed by a reupload and a check that the start offset
        for the partition has been updated accordingly.
        """
        msg_size = 2056
        self.redpanda.set_cluster_config({
            "cloud_storage_housekeeping_interval_ms":
            10000,
            "cloud_storage_spillover_manifest_max_segments":
            10,
        })
        msg_per_segment = self.segment_size // msg_size
        msg_count_before_reset = 50 * msg_per_segment
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=msg_size,
                                       msg_count=msg_count_before_reset)

        producer.start()

        def all_partitions_spilled():
            return num_manifests_uploaded(self) > 0

        wait_until(all_partitions_spilled, timeout_sec=180, backoff_sec=10)

        producer.wait(timeout_sec=60)
        producer.free()

        wait_until(lambda: self._all_uploads_done(),
                   timeout_sec=60,
                   backoff_sec=5)

        class Manifests:
            def __init__(self, test_instance):
                self.test_instance = test_instance
                self.manifest = None
                self.spillover_manifests = None

            def cloud_log_stable(self) -> bool:
                s3_snapshot = BucketView(self.test_instance.redpanda,
                                         topics=self.test_instance.topics)
                self.manifest = s3_snapshot.manifest_for_ntp(
                    self.test_instance.topic, 0)
                self.spillover_manifests = s3_snapshot.get_spillover_manifests(
                    NTP("kafka", self.test_instance.topic, 0))
                if not self.spillover_manifests or len(
                        self.spillover_manifests) < 2:
                    return False
                manifest_keys = set(self.manifest['segments'].keys())
                spillover_keys = set()
                for sm in self.spillover_manifests.values():
                    for key in sm['segments'].keys():
                        spillover_keys.add(key)
                overlap = manifest_keys & spillover_keys
                if overlap:
                    self.test_instance.logger.debug(
                        f'overlap in manifest and spillovers: {overlap}')
                return not overlap

            def has_data(self) -> bool:
                return self.manifest and self.spillover_manifests

        manifests = Manifests(self)
        wait_until(
            lambda: manifests.cloud_log_stable(),
            backoff_sec=1,
            timeout_sec=120,
            err_msg='Could not find suitable manifest and spillover combination'
        )

        assert manifests.has_data(
        ), 'Manifests were not loaded from cloud storage'
        manifest = manifests.manifest
        spill_metas = manifest["spillover"]
        # Enable aggressive local retention to remove local copy of the data
        self.rpk.alter_topic_config(self.topic, 'retention.local.target.bytes',
                                    self.segment_size * 5)

        wait_for_local_storage_truncate(self.redpanda,
                                        self.topic,
                                        target_bytes=7 * self.segment_size,
                                        partition_idx=0,
                                        timeout_sec=30)

        self.rpk.alter_topic_config(self.topic, 'redpanda.remote.write',
                                    'false')
        time.sleep(1)

        # sort the list of spillover manifest metadata
        spill_metas = sorted(spill_metas, key=lambda sm: sm['base_offset'])

        self.logger.info(
            f"Removing {spill_metas[0]} from the spillover manifest list")

        manifest['spillover'] = spill_metas[1:]

        # Adjust archive fields: the start archive start offsets move forward to
        # the new first spillover manifset and the archive size decreases by
        # the size of the removed spillover manifest.
        manifest['archive_start_offset'] = manifest['spillover'][0][
            'base_offset']
        manifest['archive_clean_offset'] = manifest['spillover'][0][
            'base_offset']
        manifest['archive_start_offset_delta'] = manifest['spillover'][0][
            'delta_offset']
        manifest['archive_size_bytes'] -= spill_metas[0]['size_bytes']

        expected_new_kafka_start_offset = BucketView.kafka_start_offset(
            manifest)

        json_man = json.dumps(manifest)
        self.logger.info(f"Re-setting manifest to:{json_man}")

        self.redpanda._admin.unsafe_reset_cloud_metadata(
            self.topic, 0, manifest)

        self.rpk.alter_topic_config(self.topic, 'redpanda.remote.write',
                                    'true')

        msg_count_after_reset = 10 * msg_per_segment
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=msg_size,
                                       msg_count=msg_count_after_reset,
                                       debug_logs=True,
                                       trace_logs=True)

        producer.start()
        producer.wait(timeout_sec=30)
        producer.free()

        # wait for uploads from first
        wait_until(lambda: self._all_uploads_done(),
                   timeout_sec=60,
                   backoff_sec=5)
        rpk = RpkTool(self.redpanda)
        partitions = list(rpk.describe_topic(self.topic))
        assert partitions[0].start_offset == expected_new_kafka_start_offset \
        , f"Partition start offset must be equal to the reset start offset. \
            expected: {expected_new_kafka_start_offset} current: {partitions[0].start_offset}"

        # Read the whole partition once, consumer must not be able to consume data from removed spillover manifests
        consumer = KgoVerifierConsumerGroupConsumer(self.test_context,
                                                    self.redpanda,
                                                    self.topic,
                                                    msg_size=msg_size,
                                                    debug_logs=True,
                                                    trace_logs=True,
                                                    readers=1)

        consumer.start()
        # messages to read contains all the produced messages minus the one that were dropped
        messages_to_read = msg_count_before_reset + msg_count_after_reset - partitions[
            0].start_offset
        wait_until(
            lambda: consumer.consumer_status.validator.valid_reads >=
            messages_to_read,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=
            f"Error waiting for {messages_to_read} messages to be consumed after reset."
        )
        consumer.wait(timeout_sec=60)

        self.logger.info(
            f"finished with consumer status: {consumer.consumer_status}")

        assert consumer.consumer_status.validator.invalid_reads == 0
        # validate that messages from the manifests that were removed from the manifest are not readable
        assert consumer.consumer_status.validator.valid_reads == messages_to_read

    @cluster(
        num_nodes=4,
        log_allow_list=["Applying the cloud manifest would cause data loss"])
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_reset_from_cloud(self, cloud_storage_type):
        """
        Test the unsafe_reset_metadata_from_cloud endpoint by repeatedly
        calling it to re-set the manifest from the uploaded one while
        producing to the partition. Once the produce finishes, wait for
        all uploads to complete and do a full read of the log to bubble
        up any inconsistencies.
        """
        msg_size = 2056
        self.redpanda.set_cluster_config({
            "cloud_storage_housekeeping_interval_ms":
            10000,
            "cloud_storage_spillover_manifest_max_segments":
            10
        })

        # Set a very low local retetion to race manifest resets wih retention
        self.rpk.alter_topic_config(self.topic, 'retention.local.target.bytes',
                                    self.segment_size * 1)

        msg_per_segment = self.segment_size // msg_size
        total_messages = 250 * msg_per_segment
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=msg_size,
                                       msg_count=total_messages,
                                       rate_limit_bps=1024 * 1024 * 5)

        producer.start()

        resets_done = 0
        resets_refused = 0
        resets_failed = 0

        seconds_between_reset = 10
        next_reset = datetime.now() + timedelta(seconds=seconds_between_reset)

        # Repeatedly reset the manifest while producing to the partition
        while not producer.is_complete():
            now = datetime.now()
            if now >= next_reset:
                try:
                    self.redpanda._admin.unsafe_reset_metadata_from_cloud(
                        namespace="kafka", topic=self.topic, partition=0)
                    resets_done += 1
                except HTTPError as ex:
                    if "would cause data loss" in ex.response.text:
                        resets_refused += 1
                    else:
                        resets_failed += 1
                    self.logger.info(f"Reset from cloud failed: {ex}")
                next_reset = now + timedelta(seconds=seconds_between_reset)

            time.sleep(2)

        producer.wait(timeout_sec=120)
        producer.free()

        self.logger.info(
            f"Producer workload complete: {resets_done=}, {resets_refused=}, {resets_failed=}"
        )

        assert resets_done + resets_refused > 0, "No resets done during the test"
        assert resets_failed == 0, f"{resets_failed} resets failed during the test"

        # Wait for all uploads to complete and read the log in full.
        # This should highlight any data consistency issues.
        # Note that we are re-using the node where the producer ran,
        # which allows for validation of the consumed offests.
        quiesce_uploads(self.redpanda, [self.topic], timeout_sec=120)

        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          self.topic,
                                          debug_logs=True,
                                          trace_logs=True)

        consumer.start()
        consumer.wait(timeout_sec=120)

    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_write(self, cloud_storage_type):
        """Write at least 10 segments, set retention policy to leave only 5
        segments, wait for segments removal, consume data and run validation,
        that everything that is acked is consumed."""
        brokers = self.redpanda.started_nodes()
        index_metrics = [
            MetricCheck(self.logger,
                        self.redpanda,
                        node, [
                            'vectorized_cloud_storage_index_uploads_total',
                            'vectorized_cloud_storage_index_downloads_total',
                        ],
                        reduce=sum) for node in brokers
        ]

        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )

        # Get a snapshot of the current segments, before tightening the
        # retention policy.
        original_snapshot = self.redpanda.storage(
            all_nodes=True).segments_by_node("kafka", self.topic, 0)

        for node, node_segments in original_snapshot.items():
            assert len(
                node_segments
            ) >= 10, f"Expected at least 10 segments, but got {len(node_segments)} on {node}"

        def indices_uploaded():
            return self.redpanda.metric_sum(
                'vectorized_cloud_storage_index_uploads_total') > 0

        wait_until(indices_uploaded,
                   timeout_sec=120,
                   backoff_sec=1,
                   err_msg='No indices uploaded to cloud storage')

        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                5 * self.segment_size,
            },
        )

        wait_for_removal_of_n_segments(redpanda=self.redpanda,
                                       topic=self.topic,
                                       partition_idx=0,
                                       n=6,
                                       original_snapshot=original_snapshot)

        self.start_consumer()
        self.run_validation()

        assert any(
            im.evaluate([('vectorized_cloud_storage_index_downloads_total',
                          lambda _, cnt: cnt)]) for im in index_metrics)

        # Matches the segment or the index
        cache_expr = re.compile(
            fr'^({self.redpanda.DATA_DIR}/cloud_storage_cache/.*\.log\.\d+)[(_chunks/.*)|(.index)]?$'
        )

        # Each segment should have the corresponding index present
        for node in self.redpanda.nodes:
            index_segment_pair = defaultdict(lambda: [0, False])
            for file in self.redpanda.data_checksum(node):
                if (match := cache_expr.match(file)) and self.topic in file:
                    entry = index_segment_pair[match[1]]
                    if file.endswith('.index') or '_chunks' not in file:
                        entry[0] += 1
                    # Count chunks just once
                    elif not entry[1] and '_chunks' in file:
                        entry[1] = True
                        entry[0] += 1
            for file, (count, _) in index_segment_pair.items():
                assert count == 2, f'expected one index and one log or one set of chunks for {file}, found {count}'

    @skip_debug_mode
    @cluster(num_nodes=4)
    def test_recover(self):
        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )
        original_snapshot = self.redpanda.storage(
            all_nodes=True).segments_by_node("kafka", self.topic, 0)
        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                5 * self.segment_size,
            },
        )

        wait_for_removal_of_n_segments(redpanda=self.redpanda,
                                       topic=self.topic,
                                       partition_idx=0,
                                       n=6,
                                       original_snapshot=original_snapshot)

        # Wipe everything and restore from tiered storage.
        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)

        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    auto_assign_node_id=True,
                                    omit_seeds_on_idx_one=False)
        self.redpanda._admin.await_stable_leader("controller",
                                                 partition=0,
                                                 namespace='redpanda',
                                                 timeout_s=60,
                                                 backoff_s=2)

        rpk = RpkTool(self.redpanda)
        rpk.cluster_recovery_start(wait=True)
        wait_until(lambda: len(set(rpk.list_topics())) == 1,
                   timeout_sec=30,
                   backoff_sec=1)

    @skip_debug_mode
    @cluster(num_nodes=4)
    def test_recover_after_delete_records(self):
        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )
        original_snapshot = self.redpanda.storage(
            all_nodes=True).segments_by_node("kafka", self.topic, 0)
        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                5 * self.segment_size,
            },
        )

        wait_for_removal_of_n_segments(redpanda=self.redpanda,
                                       topic=self.topic,
                                       partition_idx=0,
                                       n=6,
                                       original_snapshot=original_snapshot)
        self.producer.stop()
        rpk = RpkTool(self.redpanda)
        new_lwm = 2
        response = rpk.trim_prefix(self.topic, new_lwm, partitions=[0])
        assert len(response) == 1
        assert response[0].topic == self.topic
        assert response[0].partition == 0
        assert response[0].error_msg == '', f"Err msg: {response[0].error_msg}"
        assert new_lwm == response[0].new_start_offset, response[
            0].new_start_offset

        def topic_info_populated():
            return len(list(rpk.describe_topic(self.topic))) == 1

        wait_until(topic_info_populated,
                   timeout_sec=60,
                   backoff_sec=1,
                   err_msg=f"topic info not available for {self.topic}")

        topics_info = list(rpk.describe_topic(self.topic))
        assert len(topics_info) == 1
        assert topics_info[0].start_offset == new_lwm, topics_info

        def manifest_has_start_override():
            s3_snapshot = BucketView(self.redpanda, topics=self.topics)
            manifest = s3_snapshot.manifest_for_ntp(self.topic, 0)
            return "start_kafka_offset" in manifest

        wait_until(manifest_has_start_override, timeout_sec=30, backoff_sec=1)

        self.redpanda.stop()
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)
        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    auto_assign_node_id=True,
                                    omit_seeds_on_idx_one=False)
        self.redpanda._admin.await_stable_leader("controller",
                                                 partition=0,
                                                 namespace='redpanda',
                                                 timeout_s=60,
                                                 backoff_s=2)

        rpk.cluster_recovery_start(wait=True)
        wait_until(lambda: len(set(rpk.list_topics())) == 1,
                   timeout_sec=30,
                   backoff_sec=1)

        wait_until(topic_info_populated,
                   timeout_sec=60,
                   backoff_sec=1,
                   err_msg=f"topic info not available for {self.topic}")
        topics_info = list(rpk.describe_topic(self.topic))
        assert len(topics_info) == 1
        assert topics_info[0].start_offset == new_lwm, topics_info


class EndToEndShadowIndexingTestCompactedTopic(EndToEndShadowIndexingBase):
    topics = (TopicSpec(
        name=EndToEndShadowIndexingBase.s3_topic_name,
        partition_count=1,
        replication_factor=3,
        cleanup_policy="compact,delete",
        segment_bytes=EndToEndShadowIndexingBase.segment_size // 2), )

    def _prime_compacted_topic(self, segment_count):
        # Set compaction interval high at first, so we can get enough segments in log
        rpk_client = RpkTool(self.redpanda)
        rpk_client.cluster_config_set("log_compaction_interval_ms",
                                      f'{1000 * 60 * 60}')

        self.start_producer(throughput=10000, repeating_keys=10)
        wait_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=segment_count,
        )

        # Force compaction every 2 seconds now that we have some data
        rpk_client = RpkTool(self.redpanda)
        rpk_client.cluster_config_set("log_compaction_interval_ms", f'{2000}')

        original_snapshot = self.redpanda.storage(
            all_nodes=True).segments_by_node("kafka", self.topic, 0)

        for node, node_segments in original_snapshot.items():
            assert len(
                node_segments
            ) >= segment_count, f"Expected at least {segment_count} segments, " \
                                f"but got {len(node_segments)} on {node}"

        self.await_num_produced(min_records=5000)
        self.logger.info(
            f"Stopping producer after writing up to offsets {self.producer.last_acked_offsets}"
        )
        self.producer.stop()

        return original_snapshot

    def _transfer_topic_leadership(self):
        admin = Admin(self.redpanda)
        cur_leader = admin.get_partition_leader(namespace='kafka',
                                                topic=self.topic,
                                                partition=0)
        broker_ids = [x['node_id'] for x in admin.get_brokers()]
        transfer_to = random.choice([n for n in broker_ids if n != cur_leader])
        assert cur_leader != transfer_to, "incorrect partition move in test"
        admin.transfer_leadership_to(namespace="kafka",
                                     topic=self.topic,
                                     partition=0,
                                     target_id=transfer_to,
                                     leader_id=cur_leader)

        admin.await_stable_leader(self.topic,
                                  partition=0,
                                  namespace='kafka',
                                  timeout_s=60,
                                  backoff_s=2,
                                  check=lambda node_id: node_id == transfer_to)

    @skip_debug_mode
    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_write(self, cloud_storage_type):
        original_snapshot = self._prime_compacted_topic(10)

        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                5 * self.segment_size,
            },
        )

        wait_for_removal_of_n_segments(redpanda=self.redpanda,
                                       topic=self.topic,
                                       partition_idx=0,
                                       n=6,
                                       original_snapshot=original_snapshot)

        self.start_consumer(verify_offsets=False)
        self.run_consumer_validation(enable_compaction=True)

        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        s3_snapshot.assert_at_least_n_uploaded_segments_compacted(
            self.topic, partition=0, revision=None, n=1)
        s3_snapshot.assert_segments_replaced(self.topic, partition=0)

    @skip_debug_mode
    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_compacting_during_leadership_transfer(self, cloud_storage_type):
        original_snapshot = self._prime_compacted_topic(10)

        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                5 * self.segment_size,
            },
        )

        # Transfer the topic to another node
        self._transfer_topic_leadership()

        # After leadership transfer has completed assert that manifest is OK
        wait_for_removal_of_n_segments(redpanda=self.redpanda,
                                       topic=self.topic,
                                       partition_idx=0,
                                       n=6,
                                       original_snapshot=original_snapshot)

        self.start_consumer(verify_offsets=False)
        self.run_consumer_validation(enable_compaction=True)

        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        s3_snapshot.assert_at_least_n_uploaded_segments_compacted(
            self.topic, partition=0, revision=None, n=1)
        s3_snapshot.assert_segments_replaced(self.topic, partition=0)


class EndToEndShadowIndexingTestWithDisruptions(EndToEndShadowIndexingBase):
    def __init__(self, test_context):
        super().__init__(test_context,
                         extra_rp_conf={
                             'default_topic_replications': self.num_brokers,
                             'cloud_storage_cache_chunk_size': self.chunk_size,
                         })

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_write_with_node_failures(self, cloud_storage_type):
        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )

        # Get a snapshot of the current segments, before tightening the
        # retention policy.
        original_snapshot = self.redpanda.storage(
            all_nodes=True).segments_by_node("kafka", self.topic, 0)

        for node, node_segments in original_snapshot.items():
            assert len(
                node_segments
            ) >= 10, f"Expected at least 10 segments, but got {len(node_segments)} on {node}"

        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                5 * self.segment_size
            },
        )

        assert self.redpanda
        with random_process_kills(self.redpanda) as ctx:
            wait_for_local_storage_truncate(redpanda=self.redpanda,
                                            topic=self.topic,
                                            target_bytes=5 * self.segment_size,
                                            partition_idx=0)

            self.start_consumer()
            self.run_validation(consumer_timeout_sec=90)
        ctx.assert_actions_triggered()


class ShadowIndexingInfiniteRetentionTest(EndToEndShadowIndexingBase):
    # Use a small segment size to speed up the test.
    small_segment_size = EndToEndShadowIndexingBase.segment_size // 4
    small_interval_ms = 100
    infinite_topic_name = f"{EndToEndShadowIndexingBase.s3_topic_name}"
    topics = (TopicSpec(name=infinite_topic_name,
                        partition_count=1,
                        replication_factor=1,
                        retention_bytes=-1,
                        retention_ms=-1,
                        segment_bytes=small_segment_size), )

    def __init__(self, test_context):
        self.num_brokers = 1
        super().__init__(
            test_context,
            extra_rp_conf={
                # Trigger housekeeping frequently to encourage segment
                # deletion.
                "cloud_storage_housekeeping_interval_ms":
                self.small_interval_ms,

                # Use small cluster-wide retention settings to
                # encourage cloud segment deletion, as we ensure
                # nothing will be deleted for an infinite-retention
                # topic.
                "delete_retention_ms": self.small_interval_ms,
                "retention_bytes": self.small_segment_size,
                'cloud_storage_cache_chunk_size': self.chunk_size,
            })

    @cluster(num_nodes=2)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_segments_not_deleted(self, cloud_storage_type):
        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=2,
        )

        # Wait for there to be some segments.
        def manifest_has_segments():
            s3_snapshot = BucketView(self.redpanda, topics=self.topics)
            manifest = s3_snapshot.manifest_for_ntp(self.infinite_topic_name,
                                                    0)
            return len(manifest.get("segments", {})) > 0

        wait_until(manifest_has_segments, timeout_sec=10, backoff_sec=1)

        # Give ample time for would-be segment deletions to occur.
        time.sleep(5)
        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        manifest = s3_snapshot.manifest_for_ntp(self.infinite_topic_name, 0)
        assert "0-1-v1.log" in manifest["segments"], manifest


class ShadowIndexingManyPartitionsTest(PreallocNodesTest):
    small_segment_size = 4096
    chunk_size = small_segment_size * 0.75
    topic_name = f"{EndToEndShadowIndexingBase.s3_topic_name}"
    topics = (TopicSpec(name=topic_name,
                        partition_count=128,
                        replication_factor=1,
                        redpanda_remote_write=True,
                        redpanda_remote_read=True,
                        retention_bytes=-1,
                        retention_ms=-1,
                        segment_bytes=small_segment_size), )

    def __init__(self, test_context):
        self.num_brokers = 1
        si_settings = SISettings(
            test_context,
            log_segment_size=self.small_segment_size,
            cloud_storage_cache_size=20 * 2**30,
            cloud_storage_segment_max_upload_interval_sec=1,
        )
        super().__init__(
            test_context,
            node_prealloc_count=1,
            extra_rp_conf={
                # Avoid segment merging so we can generate many segments
                # quickly.
                "cloud_storage_enable_segment_merging": False,
                'log_segment_size_min': 1024,
                'cloud_storage_cache_chunk_size': self.chunk_size,
                'cloud_storage_cluster_metadata_upload_interval_ms': 1000,
                'enable_cluster_metadata_upload_loop': True,
                'controller_snapshot_max_age_sec': 1,
            },
            environment={'__REDPANDA_TOPIC_REC_DL_CHECK_MILLIS': 5000},
            si_settings=si_settings,
            # These tests write many objects; set a higher scrub timeout.
            cloud_storage_scrub_timeout_s=120)
        self.kafka_tools = KafkaCliTools(self.redpanda)

    def setUp(self):
        self.redpanda.start()
        for topic in self.topics:
            self.kafka_tools.create_topic(topic)
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic,
                               TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_MS,
                               '1000')

    @skip_debug_mode
    @cluster(num_nodes=2)
    def test_many_partitions_shutdown(self):
        """
        Test that reproduces a slow shutdown when many partitions each with
        many hydrated segments get shut down.
        """
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=1024,
                                       msg_count=10 * 1000 * 1000,
                                       rate_limit_bps=256 *
                                       self.small_segment_size,
                                       custom_node=self.preallocated_nodes)
        producer.start()
        try:
            wait_until(
                lambda: nodes_report_cloud_segments(self.redpanda, 128 * 200),
                timeout_sec=300,
                backoff_sec=5)
        finally:
            producer.stop()
            producer.wait()

        seq_consumer = KgoVerifierSeqConsumer(self.test_context,
                                              self.redpanda,
                                              self.topic,
                                              0,
                                              nodes=self.preallocated_nodes)
        seq_consumer.start(clean=False)
        seq_consumer.wait()
        self.redpanda.stop_node(self.redpanda.nodes[0])

    @skip_debug_mode
    @cluster(num_nodes=2)
    def test_many_partitions_recovery(self):
        """
        Test that reproduces an OOM when doing recovery with a large dataset in
        the bucket.
        """
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=1024,
                                       msg_count=10 * 1000 * 1000,
                                       rate_limit_bps=256 *
                                       self.small_segment_size,
                                       custom_node=self.preallocated_nodes)
        producer.start()
        try:
            wait_until(
                lambda: nodes_report_cloud_segments(self.redpanda, 100 * 200),
                timeout_sec=180,
                backoff_sec=3)
        finally:
            producer.stop()
            producer.wait()

        node = self.redpanda.nodes[0]
        self.redpanda.stop()
        self.redpanda.remove_local_data(node)
        self.redpanda.restart_nodes(self.redpanda.nodes,
                                    auto_assign_node_id=True,
                                    omit_seeds_on_idx_one=False)
        self.redpanda._admin.await_stable_leader("controller",
                                                 partition=0,
                                                 namespace='redpanda',
                                                 timeout_s=60,
                                                 backoff_s=2)

        rpk = RpkTool(self.redpanda)
        rpk.cluster_recovery_start(wait=True)
        wait_until(lambda: len(set(rpk.list_topics())) == 1,
                   timeout_sec=120,
                   backoff_sec=3)


class ShadowIndexingWhileBusyTest(PreallocNodesTest):
    # With SI enabled, run common operations against a cluster
    # while the system is under load (busy).
    # This class ports Test6 from https://github.com/redpanda-data/redpanda/issues/3572
    # into ducktape.

    segment_size = 20 * 2**20
    chunk_size = segment_size // 2
    topics = [TopicSpec()]

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(test_context,
                                 log_segment_size=self.segment_size,
                                 cloud_storage_cache_size=20 * 2**30,
                                 cloud_storage_enable_remote_read=False,
                                 cloud_storage_enable_remote_write=False,
                                 fast_uploads=True)

        super(ShadowIndexingWhileBusyTest,
              self).__init__(test_context=test_context,
                             node_prealloc_count=1,
                             num_brokers=7,
                             si_settings=si_settings,
                             extra_rp_conf={
                                 'cloud_storage_cache_chunk_size':
                                 self.chunk_size,
                             })

        if not self.redpanda.dedicated_nodes:
            self.redpanda.set_extra_rp_conf(
                {'cloud_storage_max_segment_readers_per_shard': 10})

    def setUp(self):
        # Dedicated nodes refers to non-container nodes such as EC2 instances
        self.topics[
            0].partition_count = 100 if self.redpanda.dedicated_nodes else 10

        # Topic creation happens here
        super().setUp()

    @cluster(num_nodes=8,
             log_allow_list=[
                 r"failed to hydrate chunk.*Connection reset by peer",
                 r"failed to hydrate chunk.*NotFound"
             ])
    @matrix(short_retention=[False, True],
            cloud_storage_type=get_cloud_storage_type())
    @skip_debug_mode
    def test_create_or_delete_topics_while_busy(self, short_retention,
                                                cloud_storage_type):
        """
        :param short_retention: whether to run with a very short retention globally, or just
               a short target for local retention (see issue #7092)
        """
        # Remote write/read and retention set at topic level
        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(self.topic, 'redpanda.remote.write', 'true')
        rpk.alter_topic_config(self.topic, 'redpanda.remote.read', 'true')

        rpk.alter_topic_config(
            self.topic, 'retention.bytes'
            if short_retention else 'retention.local.target.bytes',
            str(self.segment_size))

        # 100k messages of size 2**18
        # is ~24GB of data. 500k messages
        # of size 2**19 is ~244GB of data.
        msg_size = 2**19 if self.redpanda.dedicated_nodes else 2**18
        msg_count = 500000 if self.redpanda.dedicated_nodes else 100000
        timeout = 600

        random_parallelism = 100 if self.redpanda.dedicated_nodes else 20

        producer = KgoVerifierProducer(self.test_context, self.redpanda,
                                       self.topic, msg_size, msg_count,
                                       self.preallocated_nodes)
        producer.start(clean=False)
        # Block until a subset of records are produced + there is an offset map
        # for the consumer to use.
        producer.wait_for_acks(msg_count // 100,
                               timeout_sec=300,
                               backoff_sec=5)
        producer.wait_for_offset_map()

        rand_consumer = KgoVerifierRandomConsumer(self.test_context,
                                                  self.redpanda, self.topic,
                                                  msg_size, 100,
                                                  random_parallelism,
                                                  self.preallocated_nodes)

        rand_consumer.start(clean=False)

        random_topics = []

        # Do random ops until the producer stops
        def create_or_delete_until_producer_fin() -> bool:
            nonlocal random_topics
            trigger = random.randint(1, 2)

            if trigger == 1:
                some_topic = TopicSpec()
                self.logger.debug(f'Create topic: {some_topic}')
                self.client().create_topic(some_topic)
                random_topics.append(some_topic)

            if trigger == 2:
                if len(random_topics) > 0:
                    random.shuffle(random_topics)
                    some_topic = random_topics.pop()
                    self.logger.debug(f'Delete topic: {some_topic}')
                    self.client().delete_topic(some_topic.name)

            return producer.is_complete()

        # The wait condition will also apply some changes
        # such as topic creation and deletion
        self.redpanda.wait_until(create_or_delete_until_producer_fin,
                                 timeout_sec=timeout,
                                 backoff_sec=0.5,
                                 err_msg='Producer did not finish')

        producer.wait()
        rand_consumer.wait()


class EndToEndSpilloverTest(RedpandaTest):
    topics = (TopicSpec(partition_count=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context):
        extra_rp_conf = dict(cloud_storage_spillover_manifest_size=None)
        super(EndToEndSpilloverTest, self).__init__(
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
            si_settings=SISettings(
                test_context,
                log_segment_size=1024,
                fast_uploads=True,
                cloud_storage_housekeeping_interval_ms=10000,
                cloud_storage_spillover_manifest_max_segments=10))

        self.msg_size = 1024 * 256
        self.msg_count = 3000

    def produce(self):
        topic_name = self.topics[0].name
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic_name,
                                       msg_size=self.msg_size,
                                       msg_count=self.msg_count)

        producer.start()
        producer.wait()
        producer.free()

        def all_partitions_spilled():
            return num_manifests_uploaded(self) > 0

        wait_until(all_partitions_spilled, timeout_sec=180, backoff_sec=10)

    def consume(self):
        topic_name = self.topics[0].name
        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          topic_name,
                                          msg_size=self.msg_size,
                                          debug_logs=True,
                                          trace_logs=True)
        consumer.start()

        consumer.wait(timeout_sec=200)

        assert consumer.consumer_status.validator.invalid_reads == 0
        assert consumer.consumer_status.validator.valid_reads >= self.msg_count

        consumer.free()

    @cluster(num_nodes=4, log_allow_list=[r"cluster.*Can't add segment"])
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_spillover(self, cloud_storage_type):

        self.logger.info("Start producer")
        self.produce()

        self.logger.info("Remove local data")
        # Enable local retention for the topic to force reading from the 'archive'
        # section of the log and wait for the housekeeping to finish.
        rpk = RpkTool(self.redpanda)
        num_partitions = self.topics[0].partition_count
        topic_name = self.topics[0].name
        rpk.alter_topic_config(topic_name, 'retention.local.target.bytes',
                               0x1000)

        for pix in range(0, num_partitions):
            wait_for_local_storage_truncate(self.redpanda,
                                            self.topic,
                                            target_bytes=0x2000,
                                            partition_idx=pix,
                                            timeout_sec=30)

        self.logger.info("Start consumer")
        self.consume()

        self.logger.info("Stop nodes")
        for node in self.redpanda.nodes:
            self.redpanda.stop_node(node, timeout=120)

        self.logger.info("Start nodes")
        for node in self.redpanda.nodes:
            self.redpanda.start_node(node, timeout=120)

        self.logger.info("Restart consumer")
        self.consume()


class EndToEndThrottlingTest(RedpandaTest):
    topics = (TopicSpec(partition_count=3,
                        cleanup_policy=TopicSpec.CLEANUP_DELETE), )

    def __init__(self, test_context):
        si_settings = SISettings(
            test_context,
            log_segment_size=10 * 1024 * 1024,
            fast_uploads=True,
        )

        super(EndToEndThrottlingTest, self).__init__(test_context=test_context,
                                                     si_settings=si_settings)

        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

        # 1 GiB of data
        self.msg_size = 1024 * 128
        self.msg_count = 8196

    def produce(self):
        topic_name = self.topics[0].name
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic_name,
                                       msg_size=self.msg_size,
                                       msg_count=self.msg_count)

        producer.start()
        producer.wait()
        producer.free()

        wait_until(self._all_uploads_done, timeout_sec=180, backoff_sec=10)

    def _all_uploads_done(self):
        return all_uploads_done(self.rpk, self.topic, self.redpanda,
                                self.logger)

    def consume(self):
        topic_name = self.topics[0].name
        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          topic_name,
                                          loop=False,
                                          debug_logs=True,
                                          trace_logs=True)

        start = time.time()
        consumer.start()
        consumer.wait(timeout_sec=400)
        consume_duration = time.time() - start

        assert consumer.consumer_status.validator.invalid_reads == 0
        assert consumer.consumer_status.validator.valid_reads >= self.msg_count

        consumer.free()

        return consume_duration

    def measure_consume_throughput(self):
        bw_measurements = []
        for iter_ix in range(2):
            # Trim cache.
            self.redpanda.for_nodes(
                self.redpanda.nodes, lambda n: self.admin.cloud_storage_trim(
                    byte_limit=0, object_limit=0, node=n))

            # Restart redpanda to make sure it gave up on all the file handles.
            self.redpanda.restart_nodes(self.redpanda.nodes)

            # Wait all topics to have leadership.
            wait_until(lambda: all(
                self.admin.get_partition_leader(
                    namespace='kafka', topic=self.topic, partition=p) != -1
                for p in range(self.topics[0].partition_count)),
                       timeout_sec=60,
                       backoff_sec=1)

            # Measure throughput.
            self.logger.info(f"Start consumer iteration {iter_ix}")
            duration = self.consume()
            bw = self.msg_count * self.msg_size / duration
            self.logger.info(
                f"Consumer took {duration} seconds. Measured throughput: {bw} bytes/sec"
            )
            bw_measurements.append(bw)

        return sum(bw_measurements) / len(bw_measurements)

    # We throttle cloud storage download bandwidth and also the I/O of the
    # cloud storage scheduling group. In debug mode, Seastar I/O throttling
    # makes the system way slower than the target bandwidth which makes it
    # hard to assert the bandwidth or test duration so we skip the test.
    @cluster(num_nodes=4)
    # @skip_debug_mode
    @matrix(cloud_storage_type=get_cloud_storage_type(
        docker_use_arbitrary=True))
    def test_throttling(self, cloud_storage_type):

        self.logger.info("Start producer")
        self.produce()

        self.logger.info("Remove local data")

        rpk = RpkTool(self.redpanda)
        num_partitions = self.topics[0].partition_count
        topic_name = self.topics[0].name
        rpk.alter_topic_config(topic_name, 'retention.local.target.bytes',
                               4096)

        for pix in range(0, num_partitions):
            wait_for_local_storage_truncate(self.redpanda,
                                            self.topic,
                                            target_bytes=8192,
                                            partition_idx=pix,
                                            timeout_sec=30)

        # A warm-up consume to make all future measurements fairer.
        # This potentially warms up object storage. Potentially some local
        # files which are not trimmed yet.
        self.consume()

        unrestricted_bw = self.measure_consume_throughput()

        # Limit bandwidth to half of the unrestricted throughput. The expected
        # throughput depends on the number of CPU cores available and the
        # number of partitions. In the best case scenario, we expect each
        # partition to land on a separate core and the throughput limit to
        # apply to each partition independently.
        per_shard_bw_limit = int(unrestricted_bw / 2 / num_partitions)
        expected_bw = per_shard_bw_limit * num_partitions * 1.1
        self.logger.info(
            f"Unrestricted bandwidth: {unrestricted_bw} bytes/sec. "
            f"Configuring per shard limit to {per_shard_bw_limit} bytes/sec. "
            f"Expecting throughput to be less than {expected_bw} bytes/sec.")

        self.redpanda.set_cluster_config(
            {'cloud_storage_max_throughput_per_shard': per_shard_bw_limit})

        restricted_bw = self.measure_consume_throughput()
        self.logger.info(f"Restricted bandwidth: {restricted_bw} bytes/sec")

        assert restricted_bw < expected_bw, f"Expected {restricted_bw=} < {expected_bw=}"


class EndToEndHydrationTimeoutTest(EndToEndShadowIndexingBase):
    segment_size = 1048576 * 32

    def setup(self):
        assert self.redpanda is not None

    def _start_redpanda(self, override_cfg_params: Optional[dict] = None):
        if override_cfg_params:
            self.redpanda.add_extra_rp_conf(override_cfg_params)
        self.redpanda.start()
        for topic in self.topics:
            self.kafka_tools.create_topic(topic)

    def _all_kafka_connections_closed(self):
        count = 0
        for n in self.redpanda.nodes:
            for family in self.redpanda.metrics(
                    n, metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS):
                for sample in family.samples:
                    if sample.name == "redpanda_rpc_active_connections" and sample.labels[
                            "redpanda_server"] == "kafka":
                        connections = int(sample.value)
                        self.logger.debug(
                            f"open connections: {connections} on node {n.account.hostname}"
                        )
                        count += connections
        return count == 0

    def workload(self):
        msg_count = 5 * (self.segment_size // 2048)
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.s3_topic_name,
                                       msg_size=2048,
                                       msg_count=msg_count)
        producer.start()
        producer.wait(timeout_sec=300)
        producer.free()

        original_snapshot = self.redpanda.storage(
            all_nodes=True).segments_by_node("kafka", self.topic, 0)

        for node, node_segments in original_snapshot.items():
            assert len(
                node_segments
            ) >= 5, f"Expected at least 10 segments, but got {len(node_segments)} on {node}"

        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                self.segment_size,
            },
        )

        wait_for_removal_of_n_segments(redpanda=self.redpanda,
                                       topic=self.topic,
                                       partition_idx=0,
                                       n=2,
                                       original_snapshot=original_snapshot)

        def segments_downloaded():
            downloads = self.redpanda.metric_sum(
                'vectorized_cloud_storage_successful_downloads_total')
            self.logger.debug(f'downloads: {downloads}')
            return downloads > 0

        assert not segments_downloaded()

        try:
            # The rpk process is killed after one second, due to the relatively larger segment
            # size, this should ensure that the client is disconnected during hydrate.
            self.rpk.consume(self.topic, timeout=1)
        except RpkException as e:
            assert 'timed out' in e.msg

        # All connections should close very quickly once wait for hydration aborts.
        wait_until(self._all_kafka_connections_closed,
                   timeout_sec=5,
                   err_msg="Kafka connections are still open")

        # Even when the consumer disconnects or times out, the download should still complete.
        # TODO assert that if we have multiple consumers and one disconnects, the others should
        #  be able to consume.
        wait_until(segments_downloaded,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="Segment downloads did not complete")

    @cluster(num_nodes=4)
    def test_hydration_completes_on_timeout(self):
        # The short hydration timeout should cause hydrate to abort early
        self._start_redpanda({
            'cloud_storage_hydration_timeout_ms': 1,
            'cloud_storage_disable_chunk_reads': True
        })
        self.workload()

    @cluster(num_nodes=4)
    def test_hydration_completes_when_consumer_killed(self):
        # Disable chunk reads so that hydration downloads full segments and takes longer.
        self._start_redpanda({'cloud_storage_disable_chunk_reads': True})
        self.workload()
