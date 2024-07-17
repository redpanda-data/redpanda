# Copyright 2021 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
import datetime
import io
import json
import os
import pprint
import random
import re
import time
from collections import defaultdict, deque
from queue import Queue
from threading import Thread
from typing import Callable, NamedTuple, Optional, Sequence

import requests
from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.archival.abs_client import ABSClient
from rptest.archival.s3_client import S3Client
from rptest.clients.default import DefaultClient
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rp_storage_tool import RpStorageTool
from rptest.clients.rpk import RpkException, RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import (FileToChecksumSize, RedpandaService,
                                      SISettings, get_cloud_storage_type)
from rptest.services.rpk_producer import RpkProducer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result
from rptest.utils.si_utils import (
    EMPTY_SEGMENT_SIZE, MISSING_DATA_ERRORS, NTP, TRANSIENT_ERRORS,
    PathMatcher, BucketView, SegmentReader, default_log_segment_size,
    get_expected_ntp_restored_size, get_on_disk_size_per_ntp, is_close_size,
    parse_s3_manifest_path, parse_s3_segment_path, quiesce_uploads,
    verify_file_layout, NTPR, gen_local_path_from_remote)

CLOUD_STORAGE_SEGMENT_MAX_UPLOAD_INTERVAL_SEC = 10


class BaseCase:
    """Base class for all test cases. The template method inside the test
    suite calls methods of this class in the following order:
    - create_initial_topics
    - generate_baseline
    - restore_redpanda
    - validate_node (per node)
    - validate_cluster
    - after_restart_validation (if restart_needed is True)
    Template method can remove all initial state from the cluster and
    restart the nodes if initial_restart_needed is true. This process is
    needed for tests that depend on revision numbers. With initial cleanup
    we can guarantee that the initial topic revisions will be the same if we
    will create them in the same order.
    The class variable 'topics' contains list of topics that need to be created
    (it's used by some validations and default create_initial_topics implementation).
    It is usually shadowed by the instance variable with the same name.
    The instance variable expected_recovered_topics contains the list of topics
    that have to be recovered. It's needed to distinguish between the recovered
    topics and topics that created to align revision ids. By default it's equal
    to topics.
    """
    topics: Sequence[TopicSpec] = []

    def __init__(self, redpanda: RedpandaService,
                 s3_client: S3Client | ABSClient | None,
                 kafka_tools: KafkaCliTools, rpk_client: RpkTool,
                 s3_bucket: str, logger, rpk_producer_maker: Callable):
        assert s3_client is not None  # precondition for type-checking purposes
        self._redpanda = redpanda
        self._kafka_tools = kafka_tools
        self._s3 = s3_client
        self._rpk = rpk_client
        self._bucket = s3_bucket
        self.logger = logger
        # list of topics that have to be recovered, subclasses can override
        self.expected_recovered_topics = self.topics
        # Allows an RPK producer to be built with predefined test context and redpanda instance
        self._rpk_producer_maker = rpk_producer_maker

    def create_initial_topics(self):
        """Create initial set of topics based on class/instance topics variable."""
        for topic in self.topics:
            self._rpk.create_topic(topic.name,
                                   topic.partition_count,
                                   topic.replication_factor,
                                   config={
                                       'redpanda.remote.write': 'true',
                                       'redpanda.remote.read': 'true'
                                   })

    def generate_baseline(self):
        """Generate initial set of data. The method should be implemented in
        derived classes."""
        pass

    def restore_redpanda(self,
                         baseline,
                         controller_checksums,
                         topics_spec: Sequence[TopicSpec] | None = None,
                         topics_overrides: dict[str, str] | None = None):
        """Run restore procedure. Default implementation runs it for every topic
        that it can find in S3."""

        topics_spec = topics_spec or self.expected_recovered_topics
        topic_manifests = list(self._get_all_topic_manifests())
        self.logger.info(f"topic_manifests: {topic_manifests}")
        for topic in topics_spec:
            for _, manifest in topic_manifests:
                if manifest['topic'] == topic.name:
                    self._restore_topic(manifest, overrides=topics_overrides)

    def validate_node(self, host, baseline, restored):
        """Validate restored node data using two sets of checksums.
        The checksums are sampled from data directory before and after recovery."""
        pass

    def validate_cluster(self, baseline, restored):
        """This method is invoked after the recovery and partition validation are
        done."""
        pass

    @property
    def second_restart_needed(self):
        """Return True if the restart is needed afer first validation steps"""
        return False

    @property
    def initial_cleanup_needed(self):
        """Return True if the nodes needs to be cleaned up before the test"""
        return False

    @property
    def verify_s3_content_after_produce(self):
        """Return True if the test will produce any data that needs to be verified"""
        return True

    def after_restart_validation(self):
        """Called after topic recovery and subsequent restart of the cluster.
        Is only invoked if 'second_restart_needed' is True.
        """
        pass

    def _validate_partition_last_offset(self):
        """Validate restored partition by comparing high watermark to
        manifests last offset value.
        We expect high_watermark to be exactly equal to last_offset because
        redpanda removes the raft_configuration batch from the log.
        The log should have only one configuration batch.
        """
        view = BucketView(redpanda=self._redpanda)

        # Prompt the view to scan the bucket and load all manifests, so that we
        # don't have to calculate the revision ID
        manifests = view.partition_manifests

        for topic in self.topics:
            for partition in self._rpk.describe_topic(topic.name):
                hw = partition.high_watermark
                ntp = NTP(ns='kafka', topic=topic.name, partition=partition.id)
                manifest = manifests[ntp]
                last_offset = manifest['last_offset']
                self.logger.info(
                    f"validating partition: {ntp}, rev: {manifest['revision']}, last offset: {last_offset}"
                )
                # Explanation: high watermark is a kafka offset equal to the last offset in the log + 1.
                # last_offset in the manifest is the last uploaded redpanda offset. Difference between
                # kafka and redpanda offsets is equal to the number of non-data records.
                assert last_offset >= hw, \
                    f"High watermark has unexpected value {hw}, last offset: {last_offset}"

    def _produce_and_verify(self, topic_spec):
        """Try to produce to the topic. The method produces data to the topic and
        checks that high watermark advanced. Wait for partition metadata to appear
        in case of a recent leader election"""

        # Utility to capture the watermark using wait_until as soon as it is not None.
        class PartitionState:
            def __init__(self, rpk, topic_name):
                self.rpk = rpk
                self.topic_name = topic_name
                self.hwm = None

            def watermark_is_present(self):
                for partition_info in self.rpk.describe_topic(self.topic_name):
                    self.hwm = partition_info.high_watermark
                return self.hwm is not None

        old_state = PartitionState(self._rpk, topic_spec.name)
        wait_until(
            lambda: old_state.watermark_is_present(),
            timeout_sec=60,
            backoff_sec=1,
            err_msg=
            f'failed to get high watermark before produce for {topic_spec}')

        self._kafka_tools.produce(topic_spec.name, 10000, 1024)

        new_state = PartitionState(self._rpk, topic_spec.name)
        wait_until(
            lambda: new_state.watermark_is_present(),
            timeout_sec=60,
            backoff_sec=1,
            err_msg=
            f'failed to get high watermark after produce for {topic_spec}')

        assert old_state.hwm != new_state.hwm, \
            f'old_hw {old_state.hwm} unexpectedly same as new_hw {new_state.hwm} ' \
            f'for topic spec: {topic_spec}'

    def _list_objects(self):
        """Return list of all topics in the bucket (only names)"""
        results = [loc.key for loc in self._s3.list_objects(self._bucket)]
        self.logger.info(f"ListObjects: {results}")
        return results

    def _get_all_topic_manifests(self):
        """Retrieves all topics stored in S3 bucket"""
        self.logger.info(f"List all objects in {self._bucket}")
        keys = [
            key for key in self._list_objects()
            if key.endswith('topic_manifest.json')
            or key.endswith('topic_manifest.bin')
        ]
        view = BucketView(self._redpanda)
        for key in keys:
            m = view.get_topic_manifest_from_path(key)
            self.logger.info(f"Topic manifest found at {key}, content:\n{m}")
            yield (key, m)

    def _restore_topic(self,
                       manifest,
                       overrides: dict[str, str] | None = None):
        """Restore individual topic. Parameter 'path' is a path to topic
        manifest, 'manifest' is a dictionary with manifest data (it's used
        to generate topic configuration), 'overrides' contains values that
        should override values from manifest or add new fields."""

        if overrides is None:
            overrides = {}
        self.logger.info(f"Restore topic called. Topic-manifest: {manifest}")
        topic = manifest['topic']
        npart = manifest['partition_count']
        nrepl = manifest['replication_factor']
        self.logger.info(
            f"Topic: {topic}, partitions count: {npart}, replication factor: {nrepl}"
        )
        # make config
        conf = {}
        mapping = [
            ("compression.type", "compression"),
            ("cleanup.policy", "cleanup_policy_bitflags"),
            ("compaction.strategy", "compaction_strategy"),
            ("message.timestamp.type", "timestamp_type"),
            ("segment.bytes", "segment_size"),
            ("retention.bytes", "retention_bytes"),
            ("retention.ms", "retention_duration"),
            ("redpanda.virtual.cluster.id", "virtual_cluster_id"),
        ]
        for cname, mname in mapping:
            val = manifest.get(mname)
            if val:
                conf[cname] = val
        conf['redpanda.remote.recovery'] = 'true'
        conf['redpanda.remote.write'] = 'true'
        conf['redpanda.remote.read'] = 'false'
        conf.update(overrides)
        self.logger.info(f"Confg: {conf}")
        self._rpk.create_topic(topic, npart, nrepl, conf)


class NoDataCase(BaseCase):
    """Restore topic that didn't have any data in S3 but existed before the recovery.
    The expected behavior is that topic will be crated but high watermark wil be set to
    zero.
    It's perfectly valid to do this because the topic might be actually empty.
    It can also be deleted from the bucket in which case we still need to create a topic
    (there is no way we will fetch the data in this case anyway).
    """

    topics = (TopicSpec(name='panda-topic',
                        partition_count=1,
                        replication_factor=3), )

    def __init__(self, redpanda, s3_client, kafka_tools, rpk_client, s3_bucket,
                 logger, rpk_producer_maker):
        super(NoDataCase,
              self).__init__(redpanda, s3_client, kafka_tools, rpk_client,
                             s3_bucket, logger, rpk_producer_maker)

    def validate_node(self, host, baseline, restored):
        """Validate restored node data using two sets of checksums.
        The checksums are sampled from data directory before and after recovery."""
        self.logger.info(f"Node: {baseline} checksums: {restored}")
        # get rid of ntp part of the path
        old_b = [os.path.basename(key) for key, _ in baseline.items()]
        assert len(old_b) == 1
        assert "0-1-v1.log" in old_b
        new_b = [os.path.basename(key) for key, _ in restored.items()]
        assert len(new_b) == 1
        assert "0-1-v1.log" in new_b

    def validate_cluster(self, baseline, restored):
        """This method is invoked after the recovery and partition validation are
        done.
        Check that high_watermark is 0 for the new partition."""
        topic_state = list(self._rpk.describe_topic("panda-topic"))
        for partition in topic_state:
            self.logger.info(
                f"partition id: {partition.id} hw: {partition.high_watermark}")
            # Verify that we don't have any data
            assert partition.high_watermark == 0

    @property
    def verify_s3_content_after_produce(self):
        return False


class EmptySegmentsCase(BaseCase):
    """Restore topic that has segments in S3, but segments have only non-data
    batches (raft configuration, raft configuration).
    """

    topics = (TopicSpec(name='panda-topic',
                        partition_count=1,
                        replication_factor=3), )

    def __init__(self, s3_client, kafka_tools, rpk_client, s3_bucket, logger,
                 rpk_producer_maker, redpanda):
        super(EmptySegmentsCase,
              self).__init__(redpanda, s3_client, kafka_tools, rpk_client,
                             s3_bucket, logger, rpk_producer_maker)

    def generate_baseline(self):
        """Restart redpanda several times to get segments with no
        data batches."""
        for node in self._redpanda.nodes:
            self._redpanda.stop_node(node)
        time.sleep(1)
        for node in self._redpanda.nodes:
            self._redpanda.start_node(node)
        time.sleep(1)
        for node in self._redpanda.nodes:
            self._redpanda.stop_node(node)
        time.sleep(1)
        for node in self._redpanda.nodes:
            self._redpanda.start_node(node)
        time.sleep(1)

    def validate_cluster(self, baseline, restored):
        """This method is invoked after the recovery and partition validation are
        done.
        Check that high_watermark is 0 for the new partition."""
        def verify():
            topic_state = list(self._rpk.describe_topic("panda-topic"))
            for partition in topic_state:
                self.logger.info(
                    f"partition id: {partition.id} hw: {partition.high_watermark}, so: {partition.start_offset}"
                )
                return partition.high_watermark == partition.start_offset
            return False

        wait_until(verify,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg='Invalid topic state')

    @property
    def second_restart_needed(self):
        """Return True if the restart is needed afer first validation steps"""
        return True

    @property
    def verify_s3_content_after_produce(self):
        return False


class MissingTopicManifest(BaseCase):
    """Check the case where the topic manifest doesn't exist.
    We can't download any data but we should create an empty topic and add an error
    message to the log.
    We need to create an empty topic because it enables the following mitigation for
    the situation when only the topic manifest is missing. The user might just delete
    the topic if the high watermark is 0 after the recovery and restore it second time.
    If the segments are missing from the bucket this won't do any harm but if only the
    topic manifest is missing it will bring the data back.
    """

    topics = []

    def __init__(self, redpanda, s3_client, kafka_tools, rpk_client, s3_bucket,
                 logger, rpk_producer_maker):
        super(MissingTopicManifest,
              self).__init__(redpanda, s3_client, kafka_tools, rpk_client,
                             s3_bucket, logger, rpk_producer_maker)

    def create_initial_topics(self):
        """Simulate missing topic manifest by not creating any topics"""
        pass

    def restore_redpanda(self, baseline, controller_checksums):
        """Run restore with fake topic manifest path"""
        topic_manifest = "d0000000/meta/kafka/panda-topic/topic_manifest.json"
        self.logger.info(f"recovering {topic_manifest} that doesn't exist")
        conf = {'recovery.source': topic_manifest}
        self._rpk.create_topic('panda-topic', 1, 3, conf)
        self.expected_recovered_topics = (TopicSpec(name='panda-topic',
                                                    partition_count=1,
                                                    replication_factor=3), )

    def validate_cluster(self, baseline, restored):
        """This method is invoked after the recovery and partition validation are
        done.
        Check that high_watermark is 0 for the new partition."""
        topic_state = list(self._rpk.describe_topic("panda-topic"))
        for partition in topic_state:
            self.logger.info(
                f"partition id: {partition.id} hw: {partition.high_watermark}")
            # Verify that we don't have any data
            assert partition.high_watermark == 0


class MissingPartition(BaseCase):
    """Restore topic with one partition missing from the bucket.

    This should restore all partitoins except the missing one.
    The missing partition should be created emtpy.
    This might happen naturally if we didn't produce any data
    to the partition (e.g. have two partitions but produced
    only one key).
    """

    topic_name = 'panda-topic'
    topics = (TopicSpec(name=topic_name,
                        partition_count=2,
                        replication_factor=3), )

    def __init__(self, redpanda, s3_client, kafka_tools, rpk_client, s3_bucket,
                 logger, rpk_producer_maker):
        self._part1_offset = 0
        self._part1_offset_delta_end = 0
        super(MissingPartition,
              self).__init__(redpanda, s3_client, kafka_tools, rpk_client,
                             s3_bucket, logger, rpk_producer_maker)

    def generate_baseline(self):
        """Produce enough data to trigger uploads to S3/minio"""
        self.original_cluster_uuid = self._redpanda._admin.get_cluster_uuid()
        for topic in self.topics:
            producer = self._rpk_producer_maker(topic=topic.name,
                                                msg_count=10000,
                                                msg_size=1024)
            producer.start()
            producer.wait()
            producer.free()

    def _delete(self, key):
        self.logger.info(f"deleting manifest file {key}")
        self._s3.delete_object(self._bucket, key, True)

    def _find_and_remove_partition_manifest(self):
        """Find and delete manifest for partition 0. This method have to poll the
        bucket to make sure that the data is deleted from Minio (deletes are eventually
        consistent in Minio S3 implementation)."""

        ntp_0 = NTP(ns='kafka', topic=self.topic_name, partition=0)
        ntp_1 = NTP(ns='kafka', topic=self.topic_name, partition=1)

        view = BucketView(self._redpanda)
        manifests = view.partition_manifests

        manifest_0 = manifests[ntp_0]
        manifest_1 = manifests[ntp_1]
        self._part1_offset = manifest_1['last_offset']
        self._part1_offset_delta_end = \
          max([seg_meta['delta_offset_end'] for _, seg_meta in manifest_1['segments'].items()])

        manifest_0_path = view.gen_manifest_path(
            ntp_0.to_ntpr(manifest_0['revision']),
            remote_label=self.original_cluster_uuid)
        self._delete(manifest_0_path)
        self.logger.info(
            f"manifest {manifest_0_path} is removed, partition-1 last offset is {self._part1_offset}"
        )

    def restore_redpanda(self, baseline, controller_checksums):
        """Run restore but first remove partition manifest from the
        bucket."""
        self._find_and_remove_partition_manifest()
        super(MissingPartition, self).restore_redpanda(baseline,
                                                       controller_checksums)

    def validate_cluster(self, baseline, restored):
        """This method is invoked after the recovery and partition validation are
        done.
        Check that high_watermark is 0 for the new partition."""
        topic_state = list(self._rpk.describe_topic("panda-topic"))
        for partition in topic_state:
            self.logger.info(
                f"partition id: {partition.id} hw: {partition.high_watermark}")
            if partition.id == 0:
                # Verify that we don't have any data since we're deleted
                # the manifest
                assert partition.high_watermark == 0
            elif partition.id == 1:
                # NOTE: high watermark is the next Kafka offset, hence the +1.
                expected_hwm = self._part1_offset + 1 - self._part1_offset_delta_end
                assert expected_hwm == partition.high_watermark, \
                    f"Unexpected high watermark {partition.high_watermark} " \
                    f"(last rp offset: {self._part1_offset}, delta_offset_end: "\
                    f"{self._part1_offset_delta_end})"
            else:
                assert False, "Unexpected partition id"


class MissingSegment(BaseCase):
    """Restore topic with missing segment in one of the partitions.
    Should work just fine and validate.
    Note: that test removes segment with base offset 0 to ensure that
    the high watermark of the partition will have expected value after
    the recovery.
    We can delete last segment in manifest (the one with highest base
    offset) but this would require more complex validation (we will have
    to download the manifest, enumerate all existing segments and find the
    one with largest committed offset).
    """

    topics = (TopicSpec(name='panda-topic',
                        partition_count=2,
                        replication_factor=3), )

    def __init__(self, redpanda, s3_client, kafka_tools, rpk_client, s3_bucket,
                 logger, rpk_producer_maker):
        self._part1_offset = 0
        self._smaller_ntp = None
        self._deleted_segment_size = None
        super(MissingSegment,
              self).__init__(redpanda, s3_client, kafka_tools, rpk_client,
                             s3_bucket, logger, rpk_producer_maker)

    def generate_baseline(self):
        """Produce enough data to trigger uploads to S3/minio"""
        for topic in self.topics:
            producer = self._rpk_producer_maker(topic=topic.name,
                                                msg_count=10000,
                                                msg_size=1024)
            producer.start()
            producer.wait()
            producer.free()

    def _delete(self, key):
        self._deleted_segment_size = self._s3.get_object_meta(
            self._bucket, key).content_length
        self.logger.info(
            f"deleting segment file {key} of size {self._deleted_segment_size}"
        )
        self._s3.delete_object(self._bucket, key, True)

    def _find_and_remove_segment(self):
        """Find and remove single segment with base offset 0."""
        for key in self._list_objects():
            if key.endswith(".log.1"):
                attr = parse_s3_segment_path(key)
                if attr.name.startswith('0'):
                    self._delete(key)
                    self._smaller_ntp = attr.ntpr.to_ntp()
                    break
        else:
            assert False, "No segments found in the bucket"

    def restore_redpanda(self, baseline, controller_checksums):
        """Run restore but remove the segment first."""
        self._find_and_remove_segment()
        super(MissingSegment, self).restore_redpanda(baseline,
                                                     controller_checksums)

    def validate_cluster(self, baseline, restored):
        """Check that the topic is writeable"""
        self.logger.info(
            f"MissingSegment.validate_cluster - baseline - {baseline}")
        self.logger.info(
            f"MissingSegment.validate_cluster - restored - {restored}")
        expected_topics = [
            topic.name for topic in self.expected_recovered_topics
        ]
        verify_file_layout(
            baseline,
            restored,
            expected_topics,
            self.logger,
            size_overrides={self._smaller_ntp: self._deleted_segment_size})
        for topic in self.topics:
            self._produce_and_verify(topic)

    @property
    def second_restart_needed(self):
        """Return True if the restart is needed afer first validation steps"""
        return True

    def after_restart_validation(self):
        """Check that we can produce to the topic after the restart"""
        self.logger.info("after restart validation")
        for topic in self.topics:
            self._produce_and_verify(topic)


class FastCheck(BaseCase):
    """This test case covers normal recovery process. It creates configured
    set of topics and runs recovery and validations."""
    def __init__(self, redpanda, s3_client, kafka_tools, rpk_client, s3_bucket,
                 logger, rpk_producer_maker, topics):
        self.topics = topics
        self.redpanda = redpanda
        super(FastCheck,
              self).__init__(redpanda, s3_client, kafka_tools, rpk_client,
                             s3_bucket, logger, rpk_producer_maker)

    def generate_baseline(self):
        """Produce enough data to trigger uploads to S3/minio"""
        for topic in self.topics:
            producer = self._rpk_producer_maker(topic=topic.name,
                                                msg_count=10000,
                                                msg_size=1024)
            producer.start()
            producer.wait()
            producer.free()

    def _collect_non_data_batch_sizes(self, expected_topics):
        """
        Parses segment file and calculates sizes of all non-data batches present in the file.
        These sizes are used when comparing S3 segment sizes with restored data, to account
        for differences caused because configuration batches are not written to restored data.
        """
        non_data_batches_per_ntp = defaultdict(lambda: 0)
        s3_snapshot = BucketView(self.redpanda, topics=expected_topics)
        segments = [
            item for item in self._s3.list_objects(self._bucket)
            if s3_snapshot.find_segment_in_manifests(item)
        ]
        for seg in segments:
            components = parse_s3_segment_path(seg.key)
            ntp = components.ntpr.to_ntp()
            segment_data = self._s3.get_object_data(self._bucket, seg.key)
            segment_size = len(segment_data)
            segment = SegmentReader(io.BytesIO(segment_data))
            # We want to skip segments where the size is lower than EMPTY_SEGMENT_SIZE,
            # since the size of these segments is not accounted for in the final comparison
            # when verifying file layout. If we accumulate size of config batch from these
            # "skipped" segments, we will mistakenly adjust the size of the baseline NTP to
            # be smaller than it should be, in the final tally.

            # e.g. for segment A of size 455 bytes (config batch + archival stm batch),
            # since it is smaller than EMPTY_SEGMENT_SIZE, it will not be used in the final
            # total for baseline calculation. But if we still add the 455 bytes here into
            # non_data_batches_per_ntp, baseline will be reduced by 455 bytes, causing an assertion
            # error that baseline is smaller than restored by 455 bytes.
            if segment_size >= EMPTY_SEGMENT_SIZE:
                for batch in segment:
                    if batch.type != 1:
                        non_data_batches_per_ntp[ntp] += batch.batch_size
                        self.logger.debug(
                            f'non data batch {batch.type} of size {batch.batch_size} is found for {ntp}, '
                            f'record count is {batch.record_count}')
        self.logger.info(
            f'non data batch sizes per ntp: '
            f'{pprint.pformat(dict(non_data_batches_per_ntp), indent=2)}')
        return non_data_batches_per_ntp

    def validate_cluster(self, baseline, restored):
        """Check that the topic is writeable"""
        self.logger.info(f"FastCheck.validate_cluster - baseline - {baseline}")
        self.logger.info(f"FastCheck.validate_cluster - restored - {restored}")
        self._validate_partition_last_offset()
        expected_topics = [
            topic.name for topic in self.expected_recovered_topics
        ]
        config_batch_sizes = self._collect_non_data_batch_sizes(
            self.expected_recovered_topics)
        verify_file_layout(baseline,
                           restored,
                           expected_topics,
                           self.logger,
                           size_overrides=config_batch_sizes)
        for topic in self.topics:
            self._produce_and_verify(topic)

    @property
    def second_restart_needed(self):
        """Return True if the restart is needed afer first validation steps"""
        return True

    def after_restart_validation(self):
        """Check that topic is writable after restart"""
        self.logger.info("after restart validation")
        for topic in self.topics:
            self._produce_and_verify(topic)


class SizeBasedRetention(BaseCase):
    """
    Restore the topic using the size base retention policy.
    The initial topic is created with default retention policy (which is time based).
    The recovered topic is created with size based retention policy.
    The test generates 20MB of data per ntp, than after the restart it recovers
    the topic with size limit set to 10MB per ntp.
    The verification takes into account individual segment size. The recovery process
    should restore at not more than 10MB but not less than 10MB - oldest segment size."""
    def __init__(self, redpanda, s3_client, kafka_tools, rpk_client, s3_bucket,
                 logger, rpk_producer_maker, topics):
        self.topics = topics
        self.max_size_bytes = 1024 * 1024 * 20
        self.restored_size_bytes = 1024 * 1024 * 10
        super(SizeBasedRetention,
              self).__init__(redpanda, s3_client, kafka_tools, rpk_client,
                             s3_bucket, logger, rpk_producer_maker)

    def generate_baseline(self):
        """Produce enough data to trigger uploads to S3/minio.
        Produce exactly 20MB of data. It will be more than 20MB on disk
        which is OK for the test."""
        for topic in self.topics:
            # Produce 20MB per partition
            message_size = 1024
            num_messages = int(topic.partition_count * self.max_size_bytes /
                               message_size)
            self._kafka_tools.produce(topic.name, num_messages, message_size)

    def restore_redpanda(self, baseline, controller_checksums):
        """Run restore procedure. Default implementation runs it for every topic
        that it can find in S3."""

        # check that we had enought data in baseline
        size_bytes_per_ntp = get_on_disk_size_per_ntp(baseline)

        for ntp, size_bytes in size_bytes_per_ntp.items():
            self.logger.info(
                f"Partition {ntp} had size {size_bytes} on disk before recovery"
            )
            # size_bytes should be larger than amount of data that we produce
            # because segments are actually contain headers
            assert is_close_size(size_bytes, self.max_size_bytes), \
                f"Not enoug bytes produced, expected {self.max_size_bytes} got {size_bytes}"

        topic_manifests = list(self._get_all_topic_manifests())
        self.logger.info(f"topic_manifests: {topic_manifests}")
        for topic in self.expected_recovered_topics:
            for _, manifest in topic_manifests:
                if manifest['topic'] == topic.name:
                    self._restore_topic(manifest, {
                        "retention.local.target.bytes":
                        self.restored_size_bytes
                    })

    def validate_cluster(self, baseline, restored):
        """Check size of every partition."""
        self.logger.info(
            f"SizeBasedRetention.validate_cluster - baseline - {baseline}")
        self.logger.info(
            f"SizeBasedRetention.validate_cluster - restored - {restored}")

        size_bytes_per_ntp = get_on_disk_size_per_ntp(restored)
        expected_restored_size_per_ntp = get_expected_ntp_restored_size(
            baseline, self.restored_size_bytes)

        for ntp, size_bytes in size_bytes_per_ntp.items():
            self.logger.info(
                f"Partition {ntp} had size {size_bytes} on disk after recovery"
            )
            assert is_close_size(size_bytes, expected_restored_size_per_ntp[ntp]), \
                f"Too much or not enough data restored, expected {self.restored_size_bytes} got {size_bytes}"

        for topic in self.topics:
            self._produce_and_verify(topic)

    @property
    def second_restart_needed(self):
        """Return True if the restart is needed afer first validation steps"""
        return True

    def after_restart_validation(self):
        self.logger.info("after restart validation")
        for topic in self.topics:
            self._produce_and_verify(topic)


class TimeBasedRetention(BaseCase):
    """Check time-based restore process.
    The test runs as follows:
    We generate 20MB of data (per partition). The first 10MB has timestamp far
    in the past, the second 10MB has current timestamps.
    Then we're running the topic recovery with time-based retention policy and
    expect to restore at least the most recent 10MB.
    Local retention implementation changed for v23.3 and computes retention 
    based on broker timestamp at write time, this means that locally all 20mb 
    will be retained. On cloud storage, retention works on data's max_timestamp 
    as before, so the oldest 10MB might be removed depending on test timing  
    """
    def __init__(self, redpanda: RedpandaService, s3_client, kafka_tools,
                 rpk_client, s3_bucket, logger, rpk_producer_maker, topics):
        assert isinstance(redpanda, RedpandaService)
        self.topics = topics
        self.max_size_bytes = 1024 * 1024 * 20
        self.min_restored_size_bytes = 1024 * 1024 * 10
        super(TimeBasedRetention,
              self).__init__(redpanda, s3_client, kafka_tools, rpk_client,
                             s3_bucket, logger, rpk_producer_maker)

    def generate_baseline(self):
        """Produce enough data to trigger uploads to S3/minio"""

        message_size = 1024

        # A timestamp way in the past
        base_ts = 1664453149000

        for topic in self.topics:
            num_messages = int(topic.partition_count * self.max_size_bytes /
                               message_size)

            # Write half of data with timestamps way in the past, we expect
            # this not to be recovered
            # Produce a run of messages with CreateTime-style timestamps, each
            # record having a timestamp 1ms greater than the last.
            producer = KgoVerifierProducer(context=self._redpanda._context,
                                           redpanda=self._redpanda,
                                           topic=topic.name,
                                           msg_size=message_size,
                                           msg_count=num_messages // 2,
                                           batch_max_bytes=message_size * 10,
                                           fake_timestamp_ms=base_ts)
            producer.start()
            producer.wait()
            producer.free()

            # Force a segment roll to align the segment start/end timestamps
            # with the records produced
            self._redpanda.restart_nodes(self._redpanda.nodes)

            # Write second half of data with up to date timestamps
            producer = KgoVerifierProducer(context=self._redpanda._context,
                                           redpanda=self._redpanda,
                                           topic=topic.name,
                                           msg_size=message_size,
                                           msg_count=num_messages // 2,
                                           batch_max_bytes=message_size * 10)
            producer.start()
            producer.wait()
            producer.free()

    def restore_redpanda(self, baseline, controller_checksums):
        """Run restore procedure. Default implementation runs it for every topic
        that it can find in S3."""

        topic_manifests = list(self._get_all_topic_manifests())
        self.logger.info(f"topic_manifests: {topic_manifests}")
        for topic in self.expected_recovered_topics:
            for _, manifest in topic_manifests:
                if manifest['topic'] == topic.name:
                    # retention - 1 hour
                    self._restore_topic(manifest,
                                        {"retention.local.target.ms": 3600000})

    def validate_cluster(self, baseline, restored):
        """Check that the topic is writeable"""
        self.logger.info(
            f"TimeBasedRetention.validate_cluster - baseline - {baseline}")
        self.logger.info(
            f"TimeBasedRetention.validate_cluster - restored - {restored}")

        size_bytes_per_ntp = get_on_disk_size_per_ntp(restored)

        for ntp, size_bytes in size_bytes_per_ntp.items():
            self.logger.info(
                f"Partition {ntp} had size {size_bytes} on disk after recovery"
            )
            assert size_bytes >= self.min_restored_size_bytes, \
                f"Not enough data restored, expected {self.min_restored_size_bytes} got {size_bytes}"

        for topic in self.topics:
            self._produce_and_verify(topic)

    @property
    def second_restart_needed(self):
        """Return True if the restart is needed afer first validation steps"""
        return True

    def after_restart_validation(self):
        self.logger.info("after restart validation")
        for topic in self.topics:
            self._produce_and_verify(topic)


class VirtualClusterIdPropertyCase(BaseCase):
    def __init__(self, redpanda, s3_client, kafka_tools, rpk_client, s3_bucket,
                 logger, rpk_producer_maker):

        super(VirtualClusterIdPropertyCase,
              self).__init__(redpanda, s3_client, kafka_tools, rpk_client,
                             s3_bucket, logger, rpk_producer_maker)

    def create_initial_topics(self):
        self._redpanda.set_cluster_config({"enable_mpx_extensions": True})
        self.topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3,
                      virtual_cluster_id="00000000000000000000")
        ]
        self.expected_recovered_topics = self.topics
        for topic in self.topics:
            self._rpk.create_topic(topic.name,
                                   topic.partition_count,
                                   topic.replication_factor,
                                   config={
                                       'redpanda.remote.write':
                                       'true',
                                       'redpanda.remote.read':
                                       'true',
                                       'redpanda.virtual.cluster.id':
                                       topic.virtual_cluster_id
                                   })

    def restore_redpanda(self, baseline, controller_checksums):
        self._redpanda.set_cluster_config({"enable_mpx_extensions": True})
        return super().restore_redpanda(baseline, controller_checksums)

    def validate_node(self, host, baseline, restored):
        pass

    def validate_cluster(self, baseline, restored):

        topic_cfg = self._rpk.describe_topic_configs("panda-topic")
        self.logger.info(f"cfg: {topic_cfg}")

    @property
    def verify_s3_content_after_produce(self):
        return False


class AdminApiBasedRestore(FastCheck):
    def __init__(self, redpanda, s3_client, kafka_tools, rpk_client: RpkTool,
                 s3_bucket, logger, rpk_producer_maker, topics, admin: Admin):
        super().__init__(redpanda, s3_client, kafka_tools, rpk_client,
                         s3_bucket, logger, rpk_producer_maker, topics)
        self.admin = admin

    def _assert_duplicate_request_is_rejected(self):
        try:
            # A duplicate request should be rejected as a recovery is already running.
            response = self.admin.initiate_topic_scan_and_recovery()
        except requests.exceptions.HTTPError as e:
            assert e.response.status_code == requests.status_codes.codes[
                'conflict'], f'request status code: {e.response.status_code}'

    def _assert_retention(self, value):
        def wait_for_topic():
            try:
                return self._kafka_tools.describe_topic_config(
                    self.topics[0].name)
            except:
                return None

        topic_config = wait_until_result(wait_for_topic, timeout_sec=60)
        assert topic_config[
            'retention.local.target.bytes'] == value, f'failed: {topic_config}'

    def _assert_temporary_retention_is_reverted(self):
        self._assert_retention('-1')

    def _assert_status(self):
        def wait_for_status():
            r = self.admin.get_topic_recovery_status()
            assert r.status_code == requests.status_codes.codes[
                'ok'], f'request status code: {r.status_code}'
            response = r.json()
            self.logger.debug(f'response {response}')
            if isinstance(response, dict):
                return False
            for entry in r.json():
                if entry['state'] != 'inactive':
                    return entry
            return False

        status = wait_until_result(wait_for_status, timeout_sec=60)
        self.logger.info(f'got status: {status}')
        assert status['request']['topic_names_pattern'] == 'none'
        assert status['request']['retention_bytes'] == -1
        assert status['request']['retention_ms'] == 500000

    def restore_redpanda(self, *_):
        payload = {'retention_ms': 500000}
        response = self.admin.initiate_topic_scan_and_recovery(payload=payload)
        assert response.status_code == requests.status_codes.codes[
            'accepted'], f'request status code: {response.status_code}'
        self._assert_duplicate_request_is_rejected()
        self._assert_status()

    def validate_cluster(self, baseline, restored):
        """Check that the topic is writeable"""
        self.logger.info(
            f"AdminApiBasedRestore.validate_cluster - baseline - {baseline}")
        self.logger.info(
            f"AdminApiBasedRestore.validate_cluster - restored - {restored}")
        self._validate_partition_last_offset()

    def after_restart_validation(self):
        super().after_restart_validation()
        self._assert_temporary_retention_is_reverted()
        items_in_bucket = self._s3.list_objects(self._bucket)

        # All temporary result files should have been removed
        for item in items_in_bucket:
            assert not item.key.startswith('recovery_state/kafka')


class ManyPartitionsCase(BaseCase):
    """
    Specific case to test recovery of a higher number of partitions.
    Ensure that the system is stable under the increased load.
    """
    def __init__(self, redpanda, s3_client, kafka_tools, rpk_client, s3_bucket,
                 logger, rpk_producer_maker, num_topics: int,
                 num_partitions_per_topic: int, check_mode: str):
        super().__init__(redpanda, s3_client, kafka_tools, rpk_client,
                         s3_bucket, logger, rpk_producer_maker)

        self.num_topics = num_topics
        self.num_partitions_per_topic = num_partitions_per_topic
        self.check_mode = check_mode
        self.topics = [
            TopicSpec(name=f"panda-topic-{i}",
                      partition_count=num_partitions_per_topic,
                      replication_factor=3) for i in range(num_topics)
        ]
        self.expected_recovered_topics = self.topics

    def generate_baseline(self):
        """Produce enough data to trigger uploads to S3/minio"""
        for topic in self.topics:
            producer = self._rpk_producer_maker(topic=topic.name,
                                                msg_count=10000,
                                                msg_size=1024)
            producer.start()
            producer.wait()
            producer.free()

        quiesce_uploads(self._redpanda, [topic.name for topic in self.topics],
                        400)

    def restore_redpanda(self, baseline, controller_checksums):
        # set check mode, start recovery of topics
        self._redpanda.set_cluster_config(
            values={
                'cloud_storage_recovery_topic_validation_mode': self.check_mode
            })
        return super().restore_redpanda(baseline, controller_checksums)


class PreventRecoveryOfDamagedPartitionsCase(BaseCase):
    """
    Damage a partition by removing some of the newest segments,
    and check that the whole partition is prevented from recovering
    recovery_check_mode dictates the operation of this test.
    """
    def __init__(self, redpanda, s3_client, kafka_tools, rpk_client, s3_bucket,
                 logger, rpk_producer_maker):
        super().__init__(redpanda, s3_client, kafka_tools, rpk_client,
                         s3_bucket, logger, rpk_producer_maker)

        # this topic will not be damaged
        self.recoverable_topic = TopicSpec(name="panda-topic-recovery-ok",
                                           partition_count=1,
                                           replication_factor=3)
        # this topic will be damaged by removing a segment
        self.unrecoverable_topic = TopicSpec(
            name="panda-topic-recovery-NOT-OK",
            partition_count=1,
            replication_factor=1)

        # base class will create the topics via this
        self.topics = [self.unrecoverable_topic, self.recoverable_topic]
        # and will verify recovery of this list
        self.expected_recovered_topics = [self.recoverable_topic]

    def generate_baseline(self):
        """Produce enough data to trigger uploads to S3/minio"""
        for topic in self.topics:
            producer = self._rpk_producer_maker(topic=topic.name,
                                                msg_count=10000,
                                                msg_size=1024)
            producer.start()
            producer.wait()
            producer.free()

        quiesce_uploads(self._redpanda, [topic.name for topic in self.topics],
                        300)

    def _delete(self, key):
        """delete a segment"""
        self._deleted_segment_size = self._s3.get_object_meta(
            self._bucket, key).content_length
        self.logger.info(
            f"deleting segment file {key} of size {self._deleted_segment_size}"
        )
        self._s3.delete_object(self._bucket, key, True)

    def _find_and_remove_newest_segments(self, topic: TopicSpec, skip: int):
        """Find and remove a segment from a partition of topic. skip is the number of newest segment to skip before selecting the victim"""

        # list all segments in cloud storage
        all_segs = {
            key: parse_s3_segment_path(key)
            for key in self._list_objects() if key.endswith(".log.1")
        }

        # filter topics for topic.name/partition
        partition = 0
        segs_for_topic_partition: dict[int, str] = {
            comps.base_offset: obj_key
            for obj_key, comps in all_segs.items()
            if comps.ntpr.topic == topic.name
            and comps.ntpr.partition == partition
        }

        # sort in descending order by base offset, skip the first #skip elements, pick the first as the victim
        victim = sorted(segs_for_topic_partition.items(),
                        reverse=True)[skip:skip + 1]
        assert len(
            victim
        ) > 0, f"no segment found for {topic.name}/{partition} at depth {skip}. total size {len(segs_for_topic_partition)}"

        self.logger.info(f"victim={victim[0]} for {topic.name}/{partition}")

        self._delete(victim[0][1])

    def validate_cluster(self, baseline, restored):
        """Check that self unrecoverable_topic is not present"""
        assert len(
            list(self._rpk.describe_topic(self.unrecoverable_topic.name))
        ) == 0, f"topic {self.unrecoverable_topic.name} is present"

    def restore_redpanda(self, baseline, controller_checksums):

        # skip 4 segments before deleting one. validation_depth=10 should fail the recovery
        self._find_and_remove_newest_segments(self.unrecoverable_topic, skip=4)

        # set a check-mode that will stops if segments are not found in remote storage
        self._redpanda.set_cluster_config(
            values={
                'cloud_storage_recovery_topic_validation_mode':
                'check_manifest_and_segment_metadata',
                'cloud_storage_recovery_topic_validation_depth': '10',
            })

        # restore recoverable topic. this should not fail
        super().restore_redpanda(baseline, controller_checksums,
                                 [self.recoverable_topic])

        # restore unrecoverable topic. this should fail
        exception = None
        try:
            super().restore_redpanda(baseline, controller_checksums,
                                     [self.unrecoverable_topic])
        except RpkException as e:
            exception = e
        assert exception is not None, f"RpkException expected for unrecoverable {self.unrecoverable_topic.name}"


class TopicRecoveryTest(RedpandaTest):
    def __init__(self, test_context: TestContext, *args, **kwargs):
        si_settings = SISettings(test_context,
                                 cloud_storage_max_connections=5,
                                 cloud_storage_segment_max_upload_interval_sec=
                                 CLOUD_STORAGE_SEGMENT_MAX_UPLOAD_INTERVAL_SEC,
                                 log_segment_size=default_log_segment_size,
                                 fast_uploads=True)

        assert si_settings.cloud_storage_bucket is not None, "Cloud storage bucket not set"
        self.s3_bucket = si_settings.cloud_storage_bucket

        dedicated_nodes = test_context.globals.get(
            RedpandaService.DEDICATED_NODE_KEY, False)
        if dedicated_nodes:
            # Open more connections when running on real servers.
            si_settings.cloud_storage_max_connections = 20

        super(TopicRecoveryTest, self).__init__(
            test_context=test_context,
            *args,
            si_settings=si_settings,
            environment={'__REDPANDA_TOPIC_REC_DL_CHECK_MILLIS': 5000},
            # ensure that only the light check of manifest existence is performed
            extra_rp_conf={
                'cloud_storage_recovery_topic_validation_mode':
                'check_manifest_existence',
            },
            **kwargs)

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self._started = True

        self.rpk = RpkTool(self.redpanda)

    def rpk_producer_maker(self,
                           topic: str,
                           msg_size: int,
                           msg_count: int,
                           acks: Optional[int] = None) -> RpkProducer:
        return RpkProducer(self.test_context,
                           self.redpanda,
                           topic=topic,
                           msg_size=msg_size,
                           msg_count=msg_count,
                           acks=acks)

    def _collect_file_checksums(self) -> dict[str, FileToChecksumSize]:
        """Get checksums of all log segments (excl. controller log) on all nodes."""

        # This is slow in scale tests, so run on nodes in parallel.
        # We can use python threading since the tasks are I/O bound.
        class NodeChecksums(NamedTuple):
            node: ClusterNode
            checksums: FileToChecksumSize

        nodes: dict[str, FileToChecksumSize] = {}
        num_nodes = len(self.redpanda.nodes)
        queue = Queue(num_nodes)
        for node in self.redpanda.nodes:
            checksummer = lambda: queue.put(
                NodeChecksums(node, self._get_data_log_segment_checksums(node))
            )
            Thread(target=checksummer, daemon=True).start()
            self.logger.debug(
                f"Started checksum thread for {node.account.hostname}..")
        for i in range(num_nodes):
            res = queue.get()
            self.logger.debug(
                f"Node: {res.node.account.hostname} checksums: {res.checksums}"
            )
            nodes[res.node.account.hostname] = res.checksums
        return nodes

    def _collect_controller_log_checksums(self):
        """Get checksums of all controller log segments on all nodes."""
        nodes = {}
        for node in self.redpanda.nodes:
            checksums = self._get_controller_log_segment_checksums(node)
            self.logger.info(
                f"Node: {node.account.hostname} checksums: {checksums}")
            nodes[node.account.hostname] = checksums
        return nodes

    def _get_data_log_segment_checksums(
            self, node: ClusterNode) -> FileToChecksumSize:
        """Get MD5 checksums of log segments with data. Controller logs are
        excluded. The paths are normalized
        (<namespace>/<topic>/<partition>_<rev>/...)."""

        # Filter out all unwanted paths
        def included(path):
            controller_log_prefix = os.path.join(RedpandaService.DATA_DIR,
                                                 "redpanda")
            log_segment_extension = ".log"
            return not path.startswith(
                controller_log_prefix) and path.endswith(log_segment_extension)

        return self._get_log_segment_checksums(node, included)

    def _get_controller_log_segment_checksums(self, node):
        """Get MD5 checksums of the controller log segments.
        The paths are normalized (<namespace>/<topic>/<partition>_<rev>/...)."""

        # Filter out all unwanted paths
        def included(path):
            controller_log_prefix = os.path.join(RedpandaService.DATA_DIR,
                                                 "redpanda")
            log_segment_extension = ".log"
            return path.startswith(controller_log_prefix) and path.endswith(
                log_segment_extension)

        return self._get_log_segment_checksums(node, included)

    def _get_log_segment_checksums(self, node: ClusterNode,
                                   include_condition) -> FileToChecksumSize:
        """Get MD5 checksums of log segments that match the condition. The paths are
        normalized (<namespace>/<topic>/<partition>_<rev>/...)."""
        checksums = self.redpanda.data_checksum(node)

        # Remove data dir from path
        def normalize_path(path):
            return os.path.relpath(path, RedpandaService.DATA_DIR)

        return {
            normalize_path(path): value
            for path, value in checksums.items() if include_condition(path)
        }

    def _stop_redpanda_nodes(self):
        """Stop all redpanda nodes"""
        assert self._started
        for node in self.redpanda.nodes:
            self.logger.info(f"Node {node.account.hostname} will be stopped")
            self.redpanda.stop_node(node)
        self._started = False

    def _start_redpanda_nodes(self):
        """Start all redpanda nodes"""
        assert not self._started
        for node in self.redpanda.nodes:
            self.logger.info(f"Starting node {node.account.hostname}")
            self.redpanda.start_node(node)
        self.redpanda.wait_for_membership(first_start=False)
        self._started = True

    def _wipe_data(self):
        """Remove all data from redpanda cluster"""
        for node in self.redpanda.nodes:
            self.logger.info(
                f"All data will be removed from node {node.account.hostname}")
            self.redpanda.remove_local_data(node)

    @staticmethod
    def _cloud_segment_key(lw_seg_meta: dict) -> str:
        """calculates the segment key in cloud storage from segment meta"""
        assert lw_seg_meta.get("sname_format") == 3
        sm = lw_seg_meta
        return (
            f"{sm['base_offset']}-{sm['committed_offset']}-{sm['size_bytes']}"
            f"-{sm['segment_term']}-v1.log.{sm['archiver_term']}")

    def _collect_replaced_segments(self, replaced: dict[NTPR, dict[str, dict]],
                                   manifest_key: str):
        data = self.cloud_storage_client.get_object_data(
            self.s3_bucket, manifest_key)
        manifest = None
        if manifest_key.endswith('.bin'):
            manifest = RpStorageTool(
                self.logger).decode_partition_manifest(data)
        elif manifest_key.endswith('.json'):
            manifest = json.loads(data)
        assert manifest is not None, f"failed to load manifest from path {manifest_key}"
        if replaced_segments := manifest.get('replaced'):
            replaced[parse_s3_manifest_path(manifest_key)] = {
                TopicRecoveryTest._cloud_segment_key(v): v
                for k, v in replaced_segments.items()
            }

    def _wait_for_data_in_s3(self,
                             expected_topics,
                             timeout=datetime.timedelta(minutes=1)):

        # For each topic/partition, wait for local storage to be truncated according
        # to retention policy.

        cloud_sizes = deque(maxlen=6)
        total_partitions = sum([t.partition_count for t in expected_topics])
        tolerance = default_log_segment_size * total_partitions
        path_matcher = PathMatcher(expected_topics)
        replaced_segments: dict[NTPR, dict[str, dict]] = {}

        def is_replaced(segment):
            parsed = parse_s3_segment_path(segment.key)
            return parsed.name in replaced_segments.get(parsed.ntpr, {})

        """Wait until all topics are uploaded to S3"""

        def verify():
            manifests = []
            topic_manifests = []
            segments = []

            # Enumerate all S3 objects, store lists of the manifests & segments
            lst = self.cloud_storage_client.list_objects(self.s3_bucket)
            for obj in lst:
                self.logger.debug(f'checking S3 object: {obj.key}')
                if path_matcher.is_partition_manifest(obj):
                    manifests.append(obj)
                    self._collect_replaced_segments(replaced_segments, obj.key)
                elif path_matcher.is_topic_manifest(obj):
                    topic_manifests.append(obj)
                elif path_matcher.is_segment(obj):
                    segments.append(obj)

            for segment in segments:
                if is_replaced(segment):
                    self.logger.debug(
                        f'removing replaced segment {segment} from segments')

            segments = [
                segment for segment in segments if not is_replaced(segment)
            ]

            if len(expected_topics) != len(topic_manifests):
                self.logger.info(
                    f"can't find enough topic_manifest.json objects, "
                    f"expected: {len(expected_topics)}, "
                    f"actual: {len(topic_manifests)}")
                return False
            if total_partitions != len(manifests):
                self.logger.info(f"can't find enough manifest objects, "
                                 f"manifest found: {len(manifests)}, "
                                 f"expected partitions: {total_partitions}, "
                                 f"{pprint.pformat(manifests, indent=2)}")
                return False
            size_on_disk = 0
            for node, files in self._collect_file_checksums().items():
                tmp_size = 0
                for path, (_, size) in files.items():
                    if path_matcher.path_matches_any_topic(
                            path) and size > EMPTY_SEGMENT_SIZE:
                        tmp_size += size
                size_on_disk = max(tmp_size, size_on_disk)

            size_in_cloud = sum(obj.content_length for obj in segments)
            self.logger.debug(
                f'segments in cloud: {pprint.pformat(segments, indent=2)}, '
                f'size in cloud: {size_in_cloud}')

            delta = size_on_disk - size_in_cloud
            # If sizes match, we can stop
            if not delta:
                return True
            cloud_sizes.append(size_in_cloud)
            # We want to make sure we're no longer uploading, and that the diff
            # is less than the tolerated limit.
            is_stable = (delta <= tolerance
                         and len(cloud_sizes) == cloud_sizes.maxlen and
                         cloud_sizes.count(size_in_cloud) == len(cloud_sizes))
            if not is_stable:
                self.logger.info(
                    f"not enough data uploaded to S3, "
                    f"uploaded {size_in_cloud} bytes, "
                    f"with {size_on_disk} bytes on disk. "
                    f"current delta: {delta} "
                    f"tracked cloud_sizes: {pprint.pformat(cloud_sizes, indent=2)}"
                )
                return False
            return True

        wait_until(
            verify,
            timeout_sec=timeout.total_seconds(),
            # Upload should happen not more than in cloud_storage_segment_max_upload_interval_sec
            backoff_sec=CLOUD_STORAGE_SEGMENT_MAX_UPLOAD_INTERVAL_SEC / 2,
            err_msg='objects not found in S3')

    def _wait_for_topic(self,
                        recovered_topics,
                        timeout=datetime.timedelta(minutes=1)):
        """This method waits until the topic is created.
        It uses rpk describe topic command. The topic is
        considered to be recovered only if the leader is
        elected.
        The method is time bound.
        """
        expected_num_leaders = sum(
            [t.partition_count for t in recovered_topics])

        def verify():
            num_leaders = 0
            try:
                for topic in recovered_topics:
                    topic_state = self.rpk.describe_topic(topic.name)
                    # Describe topics only works after leader election succeded.
                    # We can use it to wait until the recovery is completed.
                    for partition in topic_state:
                        self.logger.info(f"partition: {partition}")
                        if partition.leader in partition.replicas:
                            num_leaders += 1
            except:
                return False
            return num_leaders == expected_num_leaders

        wait_until(verify, timeout_sec=timeout.total_seconds(), backoff_sec=1)

    def do_run(self, test_case: BaseCase, upload_delay_sec=60):
        """Template method invoked by all tests."""

        if test_case.initial_cleanup_needed:
            # We need initial cleanup in some test cases
            # e.g. to make sure that the topics have the
            # same revisions
            self.logger.info("Initial cleanup")
            self._stop_redpanda_nodes()
            time.sleep(5)
            self._wipe_data()
            self._start_redpanda_nodes()

        self.logger.info("Creating topics")
        test_case.create_initial_topics()
        self._wait_for_topic(test_case.topics)

        self.logger.info("Generating test data")
        test_case.generate_baseline()

        # Give time to finish all uploads.
        # NB: sleep() in a test is usually an anti-pattern, but in this case,
        # we know this process takes time, and we can save $$ by delaying the
        # start of polling for objects in S3.
        pre_sleep = upload_delay_sec // 6
        if pre_sleep > 0:
            self.logger.info(f"Waiting {pre_sleep} sec for S3 uploads...")
            time.sleep(pre_sleep)

        if test_case.verify_s3_content_after_produce:
            self.logger.info("Wait for data in S3")
            self._wait_for_data_in_s3(
                test_case.topics,
                timeout=datetime.timedelta(seconds=upload_delay_sec))

        self._stop_redpanda_nodes()

        baseline = self._collect_file_checksums()

        self._wipe_data()

        self._start_redpanda_nodes()

        self.logger.info("Restoring topic data")
        controller_cs = self._collect_controller_log_checksums()
        test_case.restore_redpanda(baseline, controller_cs)

        self.logger.info("Waiting while topic is created")
        self._wait_for_topic(test_case.expected_recovered_topics)

        restored = self._collect_file_checksums()
        self.logger.info(f"Restored data - {restored}")

        self.logger.info("Validating restored data per node")
        for node in self.redpanda.nodes:
            host = node.account.hostname
            self.logger.info(f"Validating node {host}")
            test_case.validate_node(host, baseline[host], restored[host])

        self.logger.info("Validate all")
        test_case.validate_cluster(baseline, restored)

        if test_case.second_restart_needed:
            self._stop_redpanda_nodes()
            self._start_redpanda_nodes()
            time.sleep(20)
            test_case.after_restart_validation()

    @cluster(num_nodes=3,
             log_allow_list=MISSING_DATA_ERRORS + TRANSIENT_ERRORS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_no_data(self, cloud_storage_type):
        """If we're trying to recovery a topic which didn't have any data
        in old cluster the empty topic should be created. We should be able
        to produce to the topic."""
        test_case = NoDataCase(self.redpanda, self.cloud_storage_client,
                               self.kafka_tools, self.rpk, self.s3_bucket,
                               self.logger, self.rpk_producer_maker)
        self.do_run(test_case)

    @cluster(num_nodes=3,
             log_allow_list=MISSING_DATA_ERRORS + TRANSIENT_ERRORS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_empty_segments(self, cloud_storage_type):
        """Test case in which the segments are uploaded but they doesn't
        have any data batches but they do have configuration batches."""
        test_case = EmptySegmentsCase(self.cloud_storage_client,
                                      self.kafka_tools, self.rpk,
                                      self.s3_bucket, self.logger,
                                      self.rpk_producer_maker, self.redpanda)
        self.do_run(test_case)

    @cluster(num_nodes=3,
             log_allow_list=MISSING_DATA_ERRORS + TRANSIENT_ERRORS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_missing_topic_manifest(self, cloud_storage_type):
        """If we're trying to recovery a topic which didn't have any data
        in old cluster the empty topic should be created. We should be able
        to produce to the topic."""
        test_case = MissingTopicManifest(self.redpanda,
                                         self.cloud_storage_client,
                                         self.kafka_tools, self.rpk,
                                         self.s3_bucket, self.logger,
                                         self.rpk_producer_maker)
        self.do_run(test_case)

    @cluster(num_nodes=4,
             log_allow_list=MISSING_DATA_ERRORS + TRANSIENT_ERRORS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_missing_partition(self, cloud_storage_type):
        """Test situation when one of the partition manifests are missing.
        The partition manifest is missing if it doesn't exist in the bucket
        in the expected place (defined by revision id) or in the alternative
        locations that could be used if the partition was moved between the
        nodes.
        """
        test_case = MissingPartition(self.redpanda, self.cloud_storage_client,
                                     self.kafka_tools, self.rpk,
                                     self.s3_bucket, self.logger,
                                     self.rpk_producer_maker)
        self.do_run(test_case)

    @cluster(num_nodes=4,
             log_allow_list=MISSING_DATA_ERRORS + TRANSIENT_ERRORS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_missing_segment(self, cloud_storage_type):
        """Test the handling of the missing segment. The segment is
        missing if it's present in the manifest but deleted from the
        bucket."""
        self.redpanda.si_settings.set_expected_damage({"missing_segments"})

        test_case = MissingSegment(self.redpanda, self.cloud_storage_client,
                                   self.kafka_tools, self.rpk, self.s3_bucket,
                                   self.logger, self.rpk_producer_maker)
        self.do_run(test_case)

    @cluster(num_nodes=4, log_allow_list=TRANSIENT_ERRORS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_fast1(self, cloud_storage_type):
        """Basic recovery test. This test stresses successful recovery
        of the topic with different set of data."""
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]
        test_case = FastCheck(self.redpanda, self.cloud_storage_client,
                              self.kafka_tools, self.rpk, self.s3_bucket,
                              self.logger, self.rpk_producer_maker, topics)
        self.do_run(test_case)

    @cluster(num_nodes=4, log_allow_list=TRANSIENT_ERRORS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_fast2(self, cloud_storage_type):
        """Basic recovery test. This test stresses successful recovery
        of the topic with different set of data."""
        topics = [
            TopicSpec(name='panda-topic-1',
                      partition_count=3,
                      replication_factor=3),
            TopicSpec(name='panda-topic-2',
                      partition_count=3,
                      replication_factor=3),
        ]
        test_case = FastCheck(self.redpanda, self.cloud_storage_client,
                              self.kafka_tools, self.rpk, self.s3_bucket,
                              self.logger, self.rpk_producer_maker, topics)
        self.do_run(test_case)

    @cluster(num_nodes=4, log_allow_list=TRANSIENT_ERRORS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_fast3(self, cloud_storage_type):
        """Basic recovery test. This test stresses successful recovery
        of the topic with different set of data."""
        topics = [
            TopicSpec(name='panda-topic-1',
                      partition_count=2,
                      replication_factor=3),
            TopicSpec(name='panda-topic-2',
                      partition_count=2,
                      replication_factor=3),
            TopicSpec(name='panda-topic-3',
                      partition_count=2,
                      replication_factor=3),
        ]
        test_case = FastCheck(self.redpanda, self.cloud_storage_client,
                              self.kafka_tools, self.rpk, self.s3_bucket,
                              self.logger, self.rpk_producer_maker, topics)
        self.do_run(test_case)

    @cluster(num_nodes=3, log_allow_list=TRANSIENT_ERRORS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_size_based_retention(self, cloud_storage_type):
        """Test topic recovery with size based retention policy.
        It's tests handling of the situation when only subset of the data needs to
        be recovered due to retention."""
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]
        test_case = SizeBasedRetention(self.redpanda,
                                       self.cloud_storage_client,
                                       self.kafka_tools, self.rpk,
                                       self.s3_bucket, self.logger,
                                       self.rpk_producer_maker, topics)
        self.do_run(test_case)

    @cluster(num_nodes=4, log_allow_list=TRANSIENT_ERRORS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_time_based_retention(self, cloud_storage_type):
        """Test topic recovery with time based retention policy.
        It's tests handling of the situation when only subset of the data needs to
        be recovered due to retention. This test uses manifests with max_timestamp
        set properly."""
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]

        assert isinstance(self.redpanda, RedpandaService)
        test_case = TimeBasedRetention(self.redpanda,
                                       self.cloud_storage_client,
                                       self.kafka_tools, self.rpk,
                                       self.s3_bucket, self.logger,
                                       self.rpk_producer_maker, topics)
        self.do_run(test_case)

    @cluster(
        num_nodes=4,
        log_allow_list=TRANSIENT_ERRORS + [
            r'unexpected REST API error "" detected, code: AccessDenied, .* resource: /recovery_state/kafka/.*'
        ])
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_admin_api_recovery(self, cloud_storage_type):
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]
        test_case = AdminApiBasedRestore(self.redpanda,
                                         self.cloud_storage_client,
                                         self.kafka_tools, self.rpk,
                                         self.s3_bucket, self.logger,
                                         self.rpk_producer_maker, topics,
                                         Admin(self.redpanda))
        self.do_run(test_case)

    @cluster(num_nodes=3, log_allow_list=TRANSIENT_ERRORS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_vcluster_id(self, cloud_storage_type):

        test_case = VirtualClusterIdPropertyCase(self.redpanda,
                                                 self.cloud_storage_client,
                                                 self.kafka_tools, self.rpk,
                                                 self.s3_bucket, self.logger,
                                                 self.rpk_producer_maker)
        self.do_run(test_case)

    @cluster(num_nodes=4, log_allow_list=TRANSIENT_ERRORS)
    @matrix(cloud_storage_type=get_cloud_storage_type(),
            check_mode=[
                'check_manifest_existence',
                'check_manifest_and_segment_metadata', 'no_check'
            ])
    def test_many_partitions(self, cloud_storage_type, check_mode):
        """
        Stress the recovery checks system with a non trivial number of partitions to check
        """
        test_case = ManyPartitionsCase(self.redpanda,
                                       self.cloud_storage_client,
                                       self.kafka_tools,
                                       self.rpk,
                                       self.s3_bucket,
                                       self.logger,
                                       self.rpk_producer_maker,
                                       num_topics=5,
                                       num_partitions_per_topic=20,
                                       check_mode=check_mode)
        self.do_run(test_case, upload_delay_sec=120)

    @cluster(num_nodes=4,
             log_allow_list=TRANSIENT_ERRORS +
             ["recovery validation", "Stopping recovery of"])
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_prevent_recovery(self, cloud_storage_type):
        """
        Check that failing a check prevents recovery of the topic.
        """
        self.redpanda.si_settings.set_expected_damage({"missing_segments"})
        test_case = PreventRecoveryOfDamagedPartitionsCase(
            self.redpanda, self.cloud_storage_client, self.kafka_tools,
            self.rpk, self.s3_bucket, self.logger, self.rpk_producer_maker)
        self.do_run(test_case)
