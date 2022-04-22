# Copyright 2021 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.archival.s3_client import S3Client
from rptest.services.redpanda import RedpandaService, SISettings
from rptest.clients.rpk import RpkTool

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools

from collections import namedtuple, defaultdict
import time
import datetime
import os
import json
import uuid
import xxhash

default_log_segment_size = 1048576  # 1MB

NTP = namedtuple("NTP", ['ns', 'topic', 'partition'])

TopicManifestMetadata = namedtuple('TopicManifestMetadata',
                                   ['ntp', 'revision'])

SegmentMetadata = namedtuple(
    'SegmentMetadata',
    ['ntp', 'revision', 'base_offset', 'term', 'md5', 'size'])

SegmentPathComponents = namedtuple('SegmentPathComponents',
                                   ['ntp', 'revision', 'name'])

ManifestPathComponents = namedtuple('ManifestPathComponents',
                                    ['ntp', 'revision'])


def _parse_s3_manifest_path(path):
    """Parse S3 manifest path. Return ntp and revision.
    Sample name: 50000000/meta/kafka/panda-topic/0_19/manifest.json
    """
    items = path.split('/')
    ns = items[2]
    topic = items[3]
    part_rev = items[4].split('_')
    partition = int(part_rev[0])
    revision = int(part_rev[1])
    ntp = NTP(ns=ns, topic=topic, partition=partition)
    return ManifestPathComponents(ntp=ntp, revision=revision)


def _parse_s3_segment_path(path):
    """Parse S3 segment path. Return ntp, revision and name.
    Sample name: b525cddd/kafka/panda-topic/0_9/4109-1-v1.log
    """
    items = path.split('/')
    ns = items[1]
    topic = items[2]
    part_rev = items[3].split('_')
    partition = int(part_rev[0])
    revision = int(part_rev[1])
    fname = items[4]
    ntp = NTP(ns=ns, topic=topic, partition=partition)
    return SegmentPathComponents(ntp=ntp, revision=revision, name=fname)


def _parse_checksum_entry(path, value, ignore_rev):
    """Parse output of the '_collect_file_checksums. Interprets path as a
    normalized path
    (e.g. <ns>/<topic>/<partition>_<revision>/<baseoffset>-<term>-v1.log).
    The value should contain a pair of md5 hash and file size."""
    md5, segment_size = value
    items = path.split('/')
    ns = items[0]
    topic = items[1]
    part_rev = items[2].split('_')
    partition = int(part_rev[0])
    revision = 0 if ignore_rev else int(part_rev[1])
    fname = items[3].split('-')
    base_offset = int(fname[0])
    term = int(fname[1])
    ntp = NTP(ns=ns, topic=topic, partition=partition)
    return SegmentMetadata(ntp=ntp,
                           revision=revision,
                           base_offset=base_offset,
                           term=term,
                           md5=md5,
                           size=segment_size)


def _verify_file_layout(baseline_per_host,
                        restored_per_host,
                        expected_topics,
                        logger,
                        size_overrides={}):
    """This function checks the restored segments over the expected ones.
    It takes into account the fact that the md5 checksum as well as the
    file name of the restored segment might be different from the original
    segment. This is because we're deleting raft configuration batches
    from the segments.
    The function checks the size of the parition over the size of the original.
    The assertion is triggered only if the difference can't be explained by the
    upload lag and removal of configuration/archival-metadata batches.
    """
    def get_ntp_sizes(fdata_per_host, hosts_can_vary=True):
        """Pre-process file layout data from the cluster. Input is a dictionary
        that maps host to dict of ntps where each ntp is mapped to the list of
        segments. The result is a map from ntp to the partition size on disk.
        """
        ntps = defaultdict(int)
        for _, fdata in fdata_per_host.items():
            ntp_size = defaultdict(int)
            for path, entry in fdata.items():
                it = _parse_checksum_entry(path, entry, ignore_rev=True)
                if it.ntp.topic in expected_topics:
                    if it.size > 4096:
                        # filter out empty segments created at the end of the log
                        # which are created after recovery
                        ntp_size[it.ntp] += it.size

            for ntp, total_size in ntp_size.items():
                if ntp in ntps and not hosts_can_vary:
                    # the size of the partition should be the
                    # same on every replica in the restored
                    # cluster
                    logger.info(
                        f"checking size of the partition for {ntp}, new {total_size} vs already accounted {ntps[ntp]}"
                    )
                    assert total_size == ntps[ntp]
                else:
                    ntps[ntp] = max(total_size, ntps[ntp])
        return ntps

    restored_ntps = get_ntp_sizes(restored_per_host, hosts_can_vary=False)
    baseline_ntps = get_ntp_sizes(baseline_per_host, hosts_can_vary=True)

    logger.info(
        f"before matching\nrestored ntps: {restored_ntps}baseline ntps: {baseline_ntps}\nexpected topics: {expected_topics}"
    )

    for ntp, orig_ntp_size in baseline_ntps.items():
        # Restored ntp should be equal or less than original
        # but not by much. It can be off by one segment size.
        # Also, each segment may lose a configuration batch or two.
        if ntp in size_overrides:
            logger.info(
                f"NTP {ntp} uses size override {orig_ntp_size} - {size_overrides[ntp]}"
            )
            orig_ntp_size -= size_overrides[ntp]
        assert ntp in restored_ntps, f"NTP {ntp} is missing in the restored data"
        rest_ntp_size = restored_ntps[ntp]
        assert rest_ntp_size <= orig_ntp_size, f"NTP {ntp} the restored partition is larger {rest_ntp_size} than the original one {orig_ntp_size}."
        delta = orig_ntp_size - rest_ntp_size
        # NOTE: 1.5x is because the segments can be larger than default_log_segment_size
        assert delta <= int(
            1.5 * default_log_segment_size
        ), f"NTP {ntp} the restored partition is too small {rest_ntp_size}. The original is {orig_ntp_size} bytes which {delta} bytes larger."


def _gen_manifest_path(ntp, rev):
    x = xxhash.xxh32()
    path = f"{ntp.ns}/{ntp.topic}/{ntp.partition}_{rev}"
    x.update(path.encode('ascii'))
    hash = x.hexdigest()[0] + '0000000'
    return f"{hash}/meta/{path}/manifest.json"


def _gen_segment_path(ntp, rev, name):
    x = xxhash.xxh32()
    path = f"{ntp.ns}/{ntp.topic}/{ntp.partition}_{rev}/{name}"
    x.update(path.encode('ascii'))
    hash = x.hexdigest()
    return f"{hash}/{path}"


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
    needed for thests that depend on revision numbers. With initial cleanup
    we can guarantee that the initial topic revisions will be the same if we
    will create them in the same order.
    The class variable 'topics' contains list of topics that need to be created
    (it's used by some validations and default create_initial_topics implementation).
    It is usually shadowed by the instance variable with the same name.
    The instance variable expected_recovered_topics contains the list of topics
    that have to be recovered. It's needed to distinguish betweeen the recovered
    topics and topics that created to align revision ids. By default it's equal
    to topics.
    """
    topics = None

    def __init__(self, s3_client: S3Client, kafka_tools: KafkaCliTools,
                 rpk_client: RpkTool, s3_bucket, logger):
        self._kafka_tools = kafka_tools
        self._s3 = s3_client
        self._rpk = rpk_client
        self._bucket = s3_bucket
        self.logger = logger
        # list of topics that have to be recovered, subclasses can override
        self.expected_recovered_topics = self.topics

    def create_initial_topics(self):
        """Create initial set of topics based on class/instance topics variable."""
        for topic in self.topics or []:
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

    def restore_redpanda(self, baseline, controller_checksums):
        """Run restore procedure. Default implementation runs it for every topic
        that it can find in S3."""
        topic_manifests = list(self._get_all_topic_manifests())
        self.logger.info(f"topic_manifests: {topic_manifests}")
        for topic in self.expected_recovered_topics:
            for _, manifest in topic_manifests:
                if manifest['topic'] == topic.name:
                    self._restore_topic(manifest)

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
        revisions = {}
        for key in self._list_objects():
            if key.endswith("/manifest.json"):
                res = _parse_s3_manifest_path(key)
                rev = res.revision
                ntp = res.ntp
                revisions[ntp] = rev
        for topic in self.topics:
            for partition in self._rpk.describe_topic(topic.name):
                hw = partition.high_watermark
                ntp = NTP(ns='kafka', topic=topic.name, partition=partition.id)
                rev = revisions[ntp]
                partition_uri = _gen_manifest_path(ntp, rev)
                data = self._s3.get_object_data(self._bucket, partition_uri)
                obj = json.loads(data)
                last_offset = obj['last_offset']
                num_segments = len(obj['segments'])
                self.logger.info(
                    f"validating partition: {ntp}, rev: {rev}, last offset: {last_offset}"
                    f", num segments: {num_segments}, hw: {hw}")
                # Explanation: high watermark is a kafka offset equal to the last offset in the log + 1.
                # last_offset in the manifest is the last uploaded redpanda offset. Difference between
                # kafka and redpanda offsets is equal to the number of non-data records. There will be
                # min 1 non-data records (a configuration record) and max (1 + num_segments) records
                # (archival metadata records for each segment). Unfortunately the number of archival metadata
                # records is non-deterministic as they can end up in the last open segment which is not
                # uploaded. After bringing everything together we have the following bounds:
                assert hw <= last_offset and hw >= last_offset - num_segments, \
                    f"High watermark has unexpected value {hw}, last offset: {last_offset}"

    def _produce_and_verify(self, topic_spec):
        """Try to produce to the topic. The method produces data to the topic and
        checks that high watermark advanced."""
        old_hw = None
        new_hw = None
        for partition in self._rpk.describe_topic(topic_spec.name):
            old_hw = partition.high_watermark
        assert old_hw is not None
        self._kafka_tools.produce(topic_spec.name, 10000, 1024)
        for topic in self._rpk.describe_topic(topic_spec.name):
            new_hw = topic.high_watermark
        assert new_hw is not None
        assert old_hw != new_hw

    def _list_objects(self):
        """Return list of all topics in the bucket (only names)"""
        results = [loc.Key for loc in self._s3.list_objects(self._bucket)]
        self.logger.info(f"ListObjects: {results}")
        return results

    def _get_all_topic_manifests(self):
        """Retrieves all topics stored in S3 bucket"""
        self.logger.info(f"List all objects in {self._bucket}")
        keys = [
            key for key in self._list_objects()
            if key.endswith('topic_manifest.json')
        ]
        for key in keys:
            j = self._s3.get_object_data(self._bucket, key)
            m = json.loads(j)
            self.logger.info(f"Topic manifest found at {key}, content:\n{j}")
            yield (key, m)

    def _restore_topic(self, manifest, overrides={}):
        """Restore individual topic. Parameter 'path' is a path to topic
        manifest, 'manifest' is a dictionary with manifest data (it's used
        to generate topic configuration), 'overrides' contains values that
        should override values from manifest or add new fields."""
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
        ]
        for cname, mname in mapping:
            val = manifest.get(mname)
            if val:
                conf[cname] = val
        conf['redpanda.remote.recovery'] = 'true'
        conf['redpanda.remote.write'] = 'true'
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

    def __init__(self, s3_client, kafka_tools, rpk_client, s3_bucket, logger):
        super(NoDataCase, self).__init__(s3_client, kafka_tools, rpk_client,
                                         s3_bucket, logger)

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


class MissingTopicManifest(BaseCase):
    """Check the case where the topic manifest doesn't exist.
    We can't download any data but we should create an empty topic and add an error
    message to the log.
    We need to craete an empty topic because it emables the following mitigation for
    the situation when only the topic manifest is missing. The user might just delete
    the topic if the high watermark is 0 after the recovery and restore it second time.
    If the segments are missing from the bucket this won't do any harm but if only the
    topic manifest is missing it will bring the data back.
    """

    topics = []

    def __init__(self, s3_client, kafka_tools, rpk_client, s3_bucket, logger):
        super(MissingTopicManifest,
              self).__init__(s3_client, kafka_tools, rpk_client, s3_bucket,
                             logger)

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

    topics = (TopicSpec(name='panda-topic',
                        partition_count=2,
                        replication_factor=3), )

    def __init__(self, s3_client, kafka_tools, rpk_client, s3_bucket, logger):
        self._part1_offset = 0
        self._part1_num_segments = 0
        super(MissingPartition, self).__init__(s3_client, kafka_tools,
                                               rpk_client, s3_bucket, logger)

    def generate_baseline(self):
        """Produce enough data to trigger uploads to S3/minio"""
        for topic in self.topics:
            self._kafka_tools.produce(topic.name, 10000, 1024)

    def _delete(self, key):
        self.logger.info(f"deleting manifest file {key}")
        self._s3.delete_object(self._bucket, key, True)

    def _find_and_remove_partition_manifest(self):
        """Find and delete manifest for partition 0. This method have to poll the
        bucket to make sure that the data is deleted from Minio (deletes are eventually
        consistent in Minio S3 implementation)."""
        manifest = None
        for key in self._list_objects():
            if key.endswith("/manifest.json"):
                attr = _parse_s3_manifest_path(key)
                if attr.ntp.partition == 0:
                    manifest = key
                else:
                    assert attr.ntp.partition == 1
                    data = self._s3.get_object_data(self._bucket, key)
                    obj = json.loads(data)
                    self._part1_offset = obj['last_offset']
                    self._part1_num_segments = len(obj['segments'])
        assert manifest is not None
        self._delete(manifest)
        self.logger.info(
            f"manifest {manifest} is removed, partition-1 last offset is {self._part1_offset}"
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
                # Explanation: high watermark is a kafka offset equal to the last offset in the log + 1.
                # last_offset in the manifest is the last uploaded redpanda offset. Difference between
                # kafka and redpanda offsets is equal to the number of non-data records. There will be
                # min 1 non-data records (a configuration record) and max (1 + num_segments) records
                # (archival metadata records for each segment). Unfortunately the number of archival metadata
                # records is non-deterministic as they can end up in the last open segment which is not
                # uploaded. After bringing everything together we have the following bounds:
                min_expected_hwm = self._part1_offset - self._part1_num_segments
                max_expected_hwm = self._part1_offset
                assert partition.high_watermark >= min_expected_hwm \
                    and partition.high_watermark <= max_expected_hwm, \
                    f"Unexpected high watermark {partition.high_watermark} "\
                    f"(min expected: {min_expected_hwm}, max expected: {max_expected_hwm})"
            else:
                assert False, "Unexpected partition id"


class MissingSegment(BaseCase):
    """Restore topic with missing segment in one of the partitions.
    Should work just fine and valiate.
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

    def __init__(self, s3_client, kafka_tools, rpk_client, s3_bucket, logger):
        self._part1_offset = 0
        self._smaller_ntp = None
        super(MissingSegment, self).__init__(s3_client, kafka_tools,
                                             rpk_client, s3_bucket, logger)

    def generate_baseline(self):
        """Produce enough data to trigger uploads to S3/minio"""
        for topic in self.topics:
            self._kafka_tools.produce(topic.name, 10000, 1024)

    def _delete(self, key):
        self.logger.info(f"deleting segment file {key}")
        self._s3.delete_object(self._bucket, key, True)

    def _find_and_remove_segment(self):
        """Find and remove single segment with base offset 0."""
        for key in self._list_objects():
            if key.endswith(".log.1"):
                attr = _parse_s3_segment_path(key)
                if attr.name.startswith('0'):
                    self._delete(key)
                    self._smaller_ntp = attr.ntp
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
        self._validate_partition_last_offset()
        expected_topics = [
            topic.name for topic in self.expected_recovered_topics
        ]
        _verify_file_layout(
            baseline,
            restored,
            expected_topics,
            self.logger,
            size_overrides={self._smaller_ntp: default_log_segment_size})
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
    def __init__(self, s3_client, kafka_tools, rpk_client, s3_bucket, logger,
                 topics):
        self.topics = topics
        super(FastCheck, self).__init__(s3_client, kafka_tools, rpk_client,
                                        s3_bucket, logger)

    def generate_baseline(self):
        """Produce enough data to trigger uploads to S3/minio"""
        for topic in self.topics:
            self._kafka_tools.produce(topic.name, 10000, 1024)

    def validate_cluster(self, baseline, restored):
        """Check that the topic is writeable"""
        self.logger.info(f"FastCheck.validate_cluster - baseline - {baseline}")
        self.logger.info(f"FastCheck.validate_cluster - restored - {restored}")
        self._validate_partition_last_offset()
        expected_topics = [
            topic.name for topic in self.expected_recovered_topics
        ]
        _verify_file_layout(baseline, restored, expected_topics, self.logger)
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


def get_on_disk_size_per_ntp(chk):
    """Get number of bytes used per ntp"""
    size_bytes_per_ntp = {}
    for _, data in chk.items():
        tmp_size = defaultdict(int)
        for path, summary in data.items():
            segment = _parse_checksum_entry(path, summary, True)
            ntp = segment.ntp
            size = summary[1]
            tmp_size[ntp] += size
        for ntp, size in tmp_size.items():
            if not ntp in size_bytes_per_ntp or size_bytes_per_ntp[ntp] < size:
                size_bytes_per_ntp[ntp] = size
    return size_bytes_per_ntp


def is_close_size(actual_size, expected_size):
    """Checks if the log size is close to expected size.
    The actual size shouldn't be less than expected. Also, the difference
    between two values shouldn't be greater than the size of one segment.
    It also takes into account segment size jitter.
    """
    lower_bound = expected_size
    upper_bound = expected_size + default_log_segment_size + \
        int(default_log_segment_size * 0.2)
    return actual_size in range(lower_bound, upper_bound)


class SizeBasedRetention(BaseCase):
    """Restore the topic using the size base retention policy.
    The initial topic is created with default retention policy (which is time based).
    The recovered topic is created with size based retention policy.
    The test generates 20MB of data per ntp, than after the restart it recovers
    the topic with size limit set to 10MB per ntp.
    The verification takes into account individual segment size. The recovery process
    should restore at least 10MB but not more than 10MB + segment size."""
    def __init__(self, s3_client, kafka_tools, rpk_client, s3_bucket, logger,
                 topics):
        self.topics = topics
        self.max_size_bytes = 1024 * 1024 * 20
        self.restored_size_bytes = 1024 * 1024 * 10
        super(SizeBasedRetention, self).__init__(s3_client, kafka_tools,
                                                 rpk_client, s3_bucket, logger)

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
                    self._restore_topic(
                        manifest,
                        {"retention.bytes": self.restored_size_bytes})

    def validate_cluster(self, baseline, restored):
        """Check size of every partition."""
        self.logger.info(
            f"SizeBasedRetention.validate_cluster - baseline - {baseline}")
        self.logger.info(
            f"SizeBasedRetention.validate_cluster - restored - {restored}")

        expected_topics = [
            topic.name for topic in self.expected_recovered_topics
        ]

        size_bytes_per_ntp = get_on_disk_size_per_ntp(restored)

        for ntp, size_bytes in size_bytes_per_ntp.items():
            self.logger.info(
                f"Partition {ntp} had size {size_bytes} on disk after recovery"
            )
            assert is_close_size(size_bytes, self.restored_size_bytes), \
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
    We generate 20MB of data (per partition). Than we're downloading the partition
    manifests and patch them. For every partition we're changing the max_timestamp
    of first 10MB of segments.
    Then we're running the topic recovery with time-based retention policy and
    expect that only last 10MB will be downloaded.
    The test can be configured to shift 'max_timestamp' fields to the past or
    to remove them. The later case is needed to check the situation when we have
    a legacy manifest that doesn't have 'max_timestamp' fields.
    """
    def __init__(self, s3_client, kafka_tools, rpk_client, s3_bucket, logger,
                 topics, remove_timestamps):
        self.topics = topics
        self.max_size_bytes = 1024 * 1024 * 20
        self.restored_size_bytes = 1024 * 1024 * 10
        self.remove_timestamps = remove_timestamps
        super(TimeBasedRetention, self).__init__(s3_client, kafka_tools,
                                                 rpk_client, s3_bucket, logger)

    def generate_baseline(self):
        """Produce enough data to trigger uploads to S3/minio"""
        for topic in self.topics:
            # Produce 20MB per partition
            message_size = 1024
            num_messages = int(topic.partition_count * self.max_size_bytes /
                               message_size)
            self._kafka_tools.produce(topic.name, num_messages, message_size)

    def _patch_manifests(self):
        """Patch manifests in such way that only last restored_size_bytes
        will be added."""
        SegmentAttributes = namedtuple(
            "SegmentAttributes",
            ['base_offset', 'max_timestamp', 'size_bytes', 'name_or_path'])
        manifests = []
        for entry in self._list_objects():
            if entry.endswith("manifest.json"
                              ) and not entry.endswith("topic_manifest.json"):
                manifests.append(entry)
        self.logger.info(f"manifests to patch {manifests}")
        assert len(manifests) != 0, "No manifests found"
        for id in manifests:
            data = self._s3.get_object_data(self._bucket, id)
            self.logger.info(f"patching manifest {id}, content: {data}")
            obj = json.loads(data)
            segments = obj['segments']
            segment_attr = []
            for name_or_path, attrs in segments.items():
                max_timestamp = attrs['max_timestamp']
                size_bytes = attrs['size_bytes']
                base_offset = attrs['base_offset']
                attr = SegmentAttributes(base_offset, max_timestamp,
                                         size_bytes, name_or_path)
                segment_attr.append(attr)
            segment_attr = sorted(segment_attr, reverse=True)
            update = []
            total_size = 0
            for attr in segment_attr:
                if total_size < self.restored_size_bytes:
                    total_size += attr.size_bytes
                else:
                    ts = datetime.datetime.fromtimestamp(attr.max_timestamp /
                                                         1000)
                    ts = ts - datetime.timedelta(days=10)
                    timestamp = int(
                        (ts - datetime.datetime(1970, 1, 1)).total_seconds() *
                        1000)
                    update.append(
                        SegmentAttributes(base_offset=attr.base_offset,
                                          max_timestamp=timestamp,
                                          size_bytes=attr.size_bytes,
                                          name_or_path=attr.name_or_path))
            for upd in update:
                if self.remove_timestamps:
                    del segments[upd.name_or_path]['max_timestamp']
                else:
                    segments[
                        upd.name_or_path]['max_timestamp'] = upd.max_timestamp
            data = json.dumps(obj)
            self.logger.info(f"updated manifest {id}, content {data}")
            self._s3.put_object(self._bucket, id, data)

    def restore_redpanda(self, baseline, controller_checksums):
        """Run restore procedure. Default implementation runs it for every topic
        that it can find in S3."""

        # patch manifests
        self._patch_manifests()

        topic_manifests = list(self._get_all_topic_manifests())
        self.logger.info(f"topic_manifests: {topic_manifests}")
        for topic in self.expected_recovered_topics:
            for _, manifest in topic_manifests:
                if manifest['topic'] == topic.name:
                    # retention - 1 hour
                    self._restore_topic(manifest, {"retention.ms": 3600000})

    def validate_cluster(self, baseline, restored):
        """Check that the topic is writeable"""
        self.logger.info(
            f"TimeBasedRetention.validate_cluster - baseline - {baseline}")
        self.logger.info(
            f"TimeBasedRetention.validate_cluster - restored - {restored}")

        expected_topics = [
            topic.name for topic in self.expected_recovered_topics
        ]

        size_bytes_per_ntp = get_on_disk_size_per_ntp(restored)

        for ntp, size_bytes in size_bytes_per_ntp.items():
            self.logger.info(
                f"Partition {ntp} had size {size_bytes} on disk after recovery"
            )
            assert is_close_size(size_bytes, self.restored_size_bytes), \
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


MISSING_DATA_ERRORS = [
    "No segments found. Empty partition manifest generated",
    "Error during log recovery: cloud_storage::missing_partition_exception",
    "Failed segment download",
]

TRANSIENT_ERRORS = [
    "raft::offset_monitor::wait_aborted",
    "Upload loop error: seastar::timed_out_error"
]


class TopicRecoveryTest(RedpandaTest):
    def __init__(self, test_context):
        si_settings = SISettings(cloud_storage_reconciliation_interval_ms=50,
                                 cloud_storage_max_connections=5,
                                 log_segment_size=default_log_segment_size)

        self.s3_bucket = si_settings.cloud_storage_bucket

        dedicated_nodes = test_context.globals.get(
            RedpandaService.DEDICATED_NODE_KEY, False)
        if dedicated_nodes:
            # Open more connections when running on real servers.
            si_settings.cloud_storage_max_connections = 10

        super(TopicRecoveryTest, self).__init__(test_context=test_context,
                                                si_settings=si_settings)

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self._started = True

        self.rpk = RpkTool(self.redpanda)

    def tearDown(self):
        self.s3_client.empty_bucket(self.s3_bucket)
        super().tearDown()

    def _collect_file_checksums(self):
        """Get checksums of all log segments (excl. controller log) on all nodes."""
        nodes = {}
        for node in self.redpanda.nodes:
            checksums = self._get_data_log_segment_checksums(node)
            self.logger.info(
                f"Node: {node.account.hostname} checksums: {checksums}")
            nodes[node.account.hostname] = checksums
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

    def _get_data_log_segment_checksums(self, node):
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

    def _get_log_segment_checksums(self, node, include_condition):
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
        self._started = True

    def _wipe_data(self):
        """Remove all data from redpanda cluster"""
        for node in self.redpanda.nodes:
            self.logger.info(
                f"All data will be removed from node {node.account.hostname}")
            self.redpanda.remove_local_data(node)

    def _wait_for_data_in_s3(self,
                             expected_topics,
                             timeout=datetime.timedelta(minutes=1)):
        """Wait until all topics are uploaded to S3"""
        def verify():
            total_partitions = sum(
                [t.partition_count for t in expected_topics])
            manifests = []
            topic_manifests = []
            segments = []
            lst = self.s3_client.list_objects(self.s3_bucket)
            for obj in lst:
                if obj.Key.endswith("/manifest.json"):
                    manifests.append(obj)
                elif obj.Key.endswith("/topic_manifest.json"):
                    topic_manifests.append(obj)
                else:
                    segments.append(obj)
            if len(expected_topics) != len(topic_manifests):
                self.logger.info(
                    f"can't find enough topic_manifest.json objects, expected: {len(expected_topicsi)}, actual: {len(topic_manifest)}"
                )
                return False
            if total_partitions != len(manifests):
                self.logger.info(
                    f"can't find enough manifest.json objects, expected: {len(manifests)}, actual: {total_partitions}"
                )
                return False
            size_on_disk = 0
            for node, files in self._collect_file_checksums().items():
                tmp_size = 0
                for path, (_, size) in files.items():
                    tmp_size += size
                size_on_disk = max([tmp_size, size_on_disk])
            size_in_cloud = 0
            for obj in segments:
                size_in_cloud += obj.ContentLength
            max_delta = default_log_segment_size * 1.5 * total_partitions
            if (size_on_disk - size_in_cloud) > max_delta:
                self.logger.info(
                    f"not enough data uploaded to S3, uploaded {size_in_cloud} bytes, with {size_on_disk} bytes on disk"
                )
                return False

            return True

        wait_until(verify, timeout_sec=timeout.total_seconds(), backoff_sec=1)

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

    def do_run(self, test_case: BaseCase):
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

        # Give time to finish all uploads
        time.sleep(10)
        if test_case.verify_s3_content_after_produce:
            self.logger.info("Wait for data in S3")
            self._wait_for_data_in_s3(test_case.topics)

        self._stop_redpanda_nodes()

        baseline = self._collect_file_checksums()

        self._wipe_data()

        self._start_redpanda_nodes()

        time.sleep(5)

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
    def test_no_data(self):
        """If we're trying to recovery a topic which didn't have any data
        in old cluster the empty topic should be created. We should be able
        to produce to the topic."""
        test_case = NoDataCase(self.s3_client, self.kafka_tools, self.rpk,
                               self.s3_bucket, self.logger)
        self.do_run(test_case)

    @cluster(num_nodes=3,
             log_allow_list=MISSING_DATA_ERRORS + TRANSIENT_ERRORS)
    def test_missing_topic_manifest(self):
        """If we're trying to recovery a topic which didn't have any data
        in old cluster the empty topic should be created. We should be able
        to produce to the topic."""
        test_case = MissingTopicManifest(self.s3_client, self.kafka_tools,
                                         self.rpk, self.s3_bucket, self.logger)
        self.do_run(test_case)

    @cluster(num_nodes=3,
             log_allow_list=MISSING_DATA_ERRORS + TRANSIENT_ERRORS)
    def test_missing_partition(self):
        """Test situation when one of the partition manifests are missing.
        The partition manifest is missing if it doesn't exist in the bucket
        in the expected place (defined by revision id) or in the alternative
        locations that could be used if the partition was moved between the
        nodes.
        """
        test_case = MissingPartition(self.s3_client, self.kafka_tools,
                                     self.rpk, self.s3_bucket, self.logger)
        self.do_run(test_case)

    @cluster(num_nodes=3,
             log_allow_list=MISSING_DATA_ERRORS + TRANSIENT_ERRORS)
    def test_missing_segment(self):
        """Test the handling of the missing segment. The segment is
        missing if it's present in the manifest but deleted from the
        bucket."""
        test_case = MissingSegment(self.s3_client, self.kafka_tools, self.rpk,
                                   self.s3_bucket, self.logger)
        self.do_run(test_case)

    @cluster(num_nodes=3, log_allow_list=TRANSIENT_ERRORS)
    def test_fast1(self):
        """Basic recovery test. This test stresses successful recovery
        of the topic with different set of data."""
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]
        test_case = FastCheck(self.s3_client, self.kafka_tools, self.rpk,
                              self.s3_bucket, self.logger, topics)
        self.do_run(test_case)

    @cluster(num_nodes=3, log_allow_list=TRANSIENT_ERRORS)
    def test_fast2(self):
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
        test_case = FastCheck(self.s3_client, self.kafka_tools, self.rpk,
                              self.s3_bucket, self.logger, topics)
        self.do_run(test_case)

    @cluster(num_nodes=3, log_allow_list=TRANSIENT_ERRORS)
    def test_fast3(self):
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
        test_case = FastCheck(self.s3_client, self.kafka_tools, self.rpk,
                              self.s3_bucket, self.logger, topics)
        self.do_run(test_case)

    @cluster(num_nodes=3, log_allow_list=TRANSIENT_ERRORS)
    def test_size_based_retention(self):
        """Test topic recovery with size based retention policy.
        It's tests handling of the situation when only subset of the data needs to
        be recovered due to retention."""
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]
        test_case = SizeBasedRetention(self.s3_client, self.kafka_tools,
                                       self.rpk, self.s3_bucket, self.logger,
                                       topics)
        self.do_run(test_case)

    @cluster(num_nodes=3, log_allow_list=TRANSIENT_ERRORS)
    def test_time_based_retention(self):
        """Test topic recovery with time based retention policy.
        It's tests handling of the situation when only subset of the data needs to
        be recovered due to retention. This test uses manifests with max_timestamp
        set properly."""
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]
        test_case = TimeBasedRetention(self.s3_client, self.kafka_tools,
                                       self.rpk, self.s3_bucket, self.logger,
                                       topics, False)
        self.do_run(test_case)

    @cluster(num_nodes=3, log_allow_list=TRANSIENT_ERRORS)
    def test_time_based_retention_with_legacy_manifest(self):
        """Test topic recovery with time based retention policy.
        It's tests handling of the situation when only subset of the data needs to
        be recovered due to retention. This test uses manifests without max_timestamp
        field."""
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]
        test_case = TimeBasedRetention(self.s3_client, self.kafka_tools,
                                       self.rpk, self.s3_bucket, self.logger,
                                       topics, True)
        self.do_run(test_case)
