# Copyright 2021 Vectorized, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md

from ducktape.mark.resource import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.archival.s3_client import S3Client
from rptest.services.redpanda import RedpandaService
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


def _find_offset_matches(baseline_per_host, restored_per_host, expected_topics,
                         logger):
    """This function checks the restored segments over the expected ones.
    It takes into account the fact that the md5 checksum as well as the
    file name of the restored segment might be different from the original
    segment. This is because we're deleting raft configuration batches
    from the segments.
    Because raft configuration batch is usually the first in a log all
    base offsets of all record batches have to be updated, as well as their
    respecitve checksums. This invalidates md5 hashes stored in S3 but also
    this leads to different filenames since base offset of the whole segment
    might change.
    """
    restored_ntps = {}
    for _, fdata in restored_per_host.items():
        ntp_meta = defaultdict(list)
        for path, entry in fdata.items():
            it = _parse_checksum_entry(path, entry, ignore_rev=True)
            if it.ntp.topic in expected_topics:
                ntp_meta[it.ntp].append((it.base_offset, it.size))

        for ntp, offsets_sizes in ntp_meta.items():
            if ntp in restored_ntps:
                # the alignment of segments should be the
                # same on every replica in the restored
                # cluster
                logger.info(
                    f"checking alignment of the segments for {ntp}, alignments: {offsets_sizes} vs {restored_ntps[ntp]}"
                )
                assert sorted(offsets_sizes) == restored_ntps[ntp]
            else:
                restored_ntps[ntp] = sorted(offsets_sizes)

    logger.info(
        f"restored state before source segment removal {restored_ntps}")

    # check that we restored something
    for ntp, offsets in restored_ntps.items():
        if ntp.topic not in expected_topics:
            continue
        assert len(offsets) != 0
        if len(offsets) == 1:
            assert offsets[0] != 0

    # Assume that if we have a log segment with base offset 0 then
    # we removed raft configuration batch out of it. This means that
    # in the restored data every log segment base offset will be equal
    # to the corresponding original base offset - 1.

    logger.info(
        f"before matching restored ntps: {restored_ntps}, expected topics: {expected_topics}"
    )
    for orig_host, fdata in baseline_per_host.items():
        logger.info(f"going through baseline items from {orig_host} : {fdata}")
        for path, entry in fdata.items():
            it = _parse_checksum_entry(path, entry, ignore_rev=True)
            if it.ntp.topic not in expected_topics:
                continue
            logger.info(f"\nmatching {it} over {restored_ntps[it.ntp]}\n")
            if it.base_offset != 0:
                expected = (it.base_offset - 1, it.size)
                if expected in restored_ntps[it.ntp]:
                    # segment in the middle of the log can be matched
                    # this way
                    restored_ntps[it.ntp].remove(expected)
                    logger.info(f"fully matched {expected}")
                else:
                    # segment at the end will have different size but expected offset
                    for elem in [(off, sz) for off, sz in restored_ntps[it.ntp]
                                 if off == it.base_offset - 1]:
                        restored_ntps[it.ntp].remove(elem)
                        logger.info(f"partially matched {expected}")
                        break
                    else:
                        logger.info(f"can't match segment {it}")
            else:
                # first segment has slightly smaller size than the original since one batch
                # is removed
                for first_elem in [(off, sz)
                                   for off, sz in restored_ntps[it.ntp]
                                   if off == 0]:
                    if first_elem[1] < it.size:
                        restored_ntps[it.ntp].remove(first_elem)
                        logger.info(f"partially matched {first_elem}")
                        break
                else:
                    logger.info(f"can't match segment {it}")

    logger.info(f"after matching restored ntps: {restored_ntps}")

    # we should find a source for all items, the last segment in original
    # cluster is not uploaded (since it's not sealed) but the matching segment
    # will be created in restored partition
    logger.info(f"restored state after source segment removal {restored_ntps}")
    for ntp, items in restored_ntps.items():
        assert len(
            items
        ) == 0, f"can't match restored state, offsets {items} are mismatched"


def _find_checksum_matches(baseline_per_host, restored_per_host,
                           expected_topics, logger):
    """ Pre-process restored data per host
    restored_per_host and baseline_per_host dicts have the following schema:
    - key: host name
    - value: dictionary with the following format:
      - key: normalized segment path
      - value: tuple (md5-hash-string, file-size-bytes)
    restored_per_host has the following property:
    - every host has a different set of partitions
    - a partition has the same layout on every node

    This is because the segments are downloaded from the same source.
    This is used to verify the data in baselilne_per_host. The data
    is the same but different replicas of the same partition on
    different nodes has different layout (base offsets of segments
    are different).

    Since only leader uploads the segments we can expect that every
    restored segment can be matched with one of the replicas in
    baseline. To verify we can just eliminate all segments in
    restored set by finding their source segments in baseline.

    Also, both baseline and restored sets shuld have mismatching tail
    elements. These elements has the largest base offset on every
    partition. They're not uploaded before the shutdown and on
    restore Redpanda creates new tail segments to write new data.

    This method can only be used to validate logs that was trimmed
    from the begining. Otherwise, if the 0-1-v1.log is restored
    the raft configuration batch will be deleted from the begining
    of the log and all checksums and segment names will be different.
    """
    restored_ntps = {}
    for _, fdata in restored_per_host.items():
        rpntp = defaultdict(list)
        for path, entry in fdata.items():
            it = _parse_checksum_entry(path, entry, ignore_rev=True)
            rpntp[it.ntp].append(it)

        for ntp, items in rpntp.items():
            if ntp in restored_ntps:
                # the alignment of segments should be the
                # same on every replica in the restored
                # cluster
                assert items == restored_ntps[ntp]
            else:
                restored_ntps[ntp] = items

    logger.info(
        f"restored state before source segment removal {restored_ntps}")
    # check that we restored anything at all
    for ntp, items in restored_ntps.items():
        if ntp.topic not in expected_topics:
            continue
        assert len(items) != 0
        if len(items) == 1:
            assert items[0].base_offset != 0
    # go through every host in baseline dataset
    # try to find a leader node(s) which was a source
    # of the segment alignment in the restored cluster
    # note that every term could be a different node
    for _, fdata in baseline_per_host.items():
        for path, entry in fdata.items():
            it = _parse_checksum_entry(path, entry, ignore_rev=True)
            if it.ntp.topic not in expected_topics:
                continue
            logger.info(f"\nmatching {it} over {restored_ntps[it.ntp]}\n")
            if it in restored_ntps[it.ntp]:
                restored_ntps[it.ntp].remove(it)
            else:
                logger.info(f"can't match segment {it}")
    # we should find a source for all items but the most
    # latest ones with term = 2
    logger.info(f"restored state after source segment removal {restored_ntps}")
    for ntp, items in restored_ntps.items():
        assert len(items) == 1
        assert items[0].term == 2 or (items[0].term == 1
                                      and items[0].base_offset == 0)


def _gen_manifest_path(ntp, rev):
    x = xxhash.xxh32()
    path = f"{ntp.ns}/{ntp.topic}/{ntp.partition}_{rev}"
    x.update(path)
    hash = x.hexdigest()[0] + '0000000'
    return f"{hash}/meta/{path}/manifest.json"


def _gen_segment_path(ntp, rev, name):
    x = xxhash.xxh32()
    path = f"{ntp.ns}/{ntp.topic}/{ntp.partition}_{rev}/{name}"
    x.update(path)
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
            self._rpk.create_topic(topic.name, topic.partition_count,
                                   topic.replication_factor)

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
                self.logger.info(
                    f"validating partition: {ntp}, rev: {rev}, last offset: {last_offset}, hw: {hw}"
                )
                assert last_offset == hw, \
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
        conf['x-redpanda-recovery'] = 'true'
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
        last_offset = None
        for key in self._list_objects():
            if key.endswith("/manifest.json"):
                attr = _parse_s3_manifest_path(key)
                if attr.ntp.partition == 0:
                    manifest = key
                else:
                    data = self._s3.get_object_data(self._bucket, key)
                    obj = json.loads(data)
                    last_offset = obj['last_offset']
        assert manifest is not None
        assert last_offset is not None
        assert last_offset != 0
        self._part1_offset = last_offset
        self._delete(manifest)
        self.logger.info(
            f"manifest {manifest} is removed, partition-1 expected offset is {last_offset}"
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
                expected = self._part1_offset
                assert partition.high_watermark == expected, \
                    f"High watermark {partition.high_watermark} is not equal to last_offset {expected}"
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
            if key.endswith(".log"):
                attr = _parse_s3_segment_path(key)
                if attr.name.startswith('0'):
                    self._delete(key)
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
        _find_offset_matches(baseline, restored, expected_topics, self.logger)
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
        _find_offset_matches(baseline, restored, expected_topics, self.logger)
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


class RevShiftCheck(FastCheck):
    """Check case when restored topic has different revision related to old one.
    It's same as fast check only it created additionall dummy topic before running
    topic recovery or before creating initial topic.
    It can test three different cases:
    - new initial topic revision is equal to old topic revision (paths in the bucket 
      are the same in old and new clusters).
    - new initial revision is greater than the old one
    - new initial revision is less than the old one
    """
    def __init__(self, s3_client, kafka_tools, rpk_client, s3_bucket, logger,
                 topics, rev_shift):
        """Init test case.
        'rev_shift' is an inteded revision shift, 0 - revision should be the same
                    positive value - revision in new cluster should be greater than
                    revision in original one, negative value - revision should be
                    smaller.
        """
        assert len(topics) == 1, "Only one topic supported by this test suite"
        self._shift = rev_shift
        super(RevShiftCheck, self).__init__(s3_client, kafka_tools, rpk_client,
                                            s3_bucket, logger, topics)

    def create_initial_topics(self):
        if self._shift < 0:
            self._create_dummy_topics(1)
        return super(RevShiftCheck, self).create_initial_topics()

    def initial_cleanup_needed(self):
        return True

    def _get_remote_topic_revision(self):
        manifests = []
        for obj in self._s3.list_objects(self._bucket):
            if obj.Key.endswith("topic_manifest.json"):
                manifests.append(obj.Key)
        assert len(manifests) != 0
        max_revision = 0
        min_revision = 999999
        for key in manifests:
            data = self._s3.get_object_data(self._bucket, key)
            manifest = json.loads(data)
            rev = manifest['revision_id']
            max_revision = max(rev, max_revision)
            min_revision = min(rev, min_revision)
        return min_revision, max_revision

    def _extract_max_term(self, controller_checksums):
        max_term = 0
        self.logger.info(f"controller checksums: {controller_checksums}")
        for _, segments in controller_checksums.items():
            for path, segm in segments.items():
                meta = _parse_checksum_entry(path, segm, ignore_rev=False)
                max_term = max(max_term, meta.term)
        return max_term

    def _create_dummy_topics(self, num_topics_to_create):
        self.logger.info(f"{num_topics_to_create} topics will be created")
        for ix in range(0, num_topics_to_create, 2):
            self._rpk.create_topic(f'dummy-topic-{ix}')

    def restore_redpanda(self, baseline, controller_checksums):
        min_revision, max_revision = self._get_remote_topic_revision()
        self.logger.info(
            f"Old cluster min_revision: {min_revision}, max_revision: {max_revision}"
        )
        max_term = self._extract_max_term(controller_checksums)
        self.logger.info(
            f"New cluster initial controller log term: {max_term}")
        # Old cluster had revisions in range [min_revision, max_revision]
        # the next created topic in the new cluster will have a revision
        # number set to max_term.
        required_rev = min_revision + self._shift
        assert required_rev > max_term, \
            f"Required revision id {required_rev} is smaller than controller log term {max_term}"
        if self._shift > 0:
            self._create_dummy_topics(1)
        super(RevShiftCheck, self).restore_redpanda(baseline,
                                                    controller_checksums)


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
        _find_offset_matches(baseline, restored, expected_topics, self.logger)

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
        _find_offset_matches(baseline, restored, expected_topics, self.logger)

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


class MovedPartitionRestore(BaseCase):
    """Check situation when the partition is moved
    from one node to the other. In this case it will be asigned new revision id.
    This revision id will be larger than the topic revision id.
    This test checks this by copying or movng partition manifest and all
    segments to the location that corresponds to new revision id.
    If data is not moved the recovery algorithm have to eliminate duplicates.
    Recovery algorithm considers the manifest with the initial revision id (the
    same as the topic has) and all revisions which are larger are also candidates.

    If data is moved the recovery algorithm should be able to find the parition
    manifest anyway.    
    """
    def __init__(self, s3_client, kafka_tools, rpk_client, s3_bucket, logger,
                 topics, move_data):
        self.topics = topics
        self.total_bytes = 1024 * 1024 * 10
        self.rev_increment = 10
        self.move_data = move_data
        super(MovedPartitionRestore,
              self).__init__(s3_client, kafka_tools, rpk_client, s3_bucket,
                             logger)

    def generate_baseline(self):
        """Produce enough data to trigger uploads to S3/minio"""
        for topic in self.topics:
            message_size = 1024
            num_messages = int(topic.partition_count * self.total_bytes /
                               message_size)
            self._kafka_tools.produce(topic.name, num_messages, message_size)

    def _transfer(self, src, dst):
        """Copy or move the object in the bucket"""
        if self.move_data:
            self.logger.info(f"moving {src} to {dst}")
            self._s3.move_object(self._bucket, src, dst, True)
        else:
            self.logger.info(f"copying {src} to {dst}")
            self._s3.copy_object(self._bucket, src, dst, True)

    def _copy_or_move(self):
        """Move all segments in S3 to another paths with another revision.
        Patch manifests. Don't touch topic manifests."""
        manifests = []
        segments = []
        for entry in self._list_objects():
            self.logger.info(f"list object entry: {entry}")
            if entry.endswith("manifest.json"
                              ) and not entry.endswith("topic_manifest.json"):
                manifests.append(entry)
            elif not entry.endswith("topic_manifest.json"):
                segments.append(entry)
        self.logger.info(f"manifests to patch {manifests}")
        self.logger.info(f"segments to copy/move {segments}")
        assert len(manifests) != 0, "No manifests found in S3"
        assert len(segments) != 0, "No segment files found in S3"

        for old_path in segments:
            self.logger.info(f"parsing segment path {old_path}")
            attr = _parse_s3_segment_path(old_path)
            self.logger.info(f"segment path attributes {attr}")
            new_path = _gen_segment_path(attr.ntp,
                                         attr.revision + self.rev_increment,
                                         attr.name)
            self._transfer(old_path, new_path)

        for old_path in manifests:
            data = self._s3.get_object_data(self._bucket, old_path)
            self.logger.info(f"patching manifest {old_path}, content: {data}")
            obj = json.loads(data)
            old_rev = obj['revision']
            obj['revision'] = old_rev + self.rev_increment
            attr = _parse_s3_manifest_path(old_path)
            assert attr.revision == old_rev, f"Manifest revision {old_rev} doesn't match path {old_path}"
            new_path = _gen_manifest_path(attr.ntp,
                                          attr.revision + self.rev_increment)
            data = json.dumps(obj)
            self._transfer(old_path, new_path)
            self.logger.info(
                f"upload updated manifest to {new_path}, content {data}")
            self._s3.put_object(self._bucket, new_path, data)

    def validate_cluster(self, baseline, restored):
        """Check that the topic is writeable"""
        self.logger.info(
            f"MovedPartitionRestore.validate_cluster - baseline - {baseline}")
        self.logger.info(
            f"MovedPartitionRestore.validate_cluster - restored - {restored}")
        self._validate_partition_last_offset()
        expected_topics = [
            topic.name for topic in self.expected_recovered_topics
        ]
        _find_offset_matches(baseline, restored, expected_topics, self.logger)
        for topic in self.topics:
            self._produce_and_verify(topic)

    def restore_redpanda(self, baseline, controller_checksums):
        self._copy_or_move()
        super(MovedPartitionRestore,
              self).restore_redpanda(baseline, controller_checksums)

    @property
    def second_restart_needed(self):
        """Return True if the restart is needed afer first validation steps"""
        return True

    def after_restart_validation(self):
        self.logger.info("after restart validation")
        for topic in self.topics:
            self._produce_and_verify(topic)


class CascadingRestore(BaseCase):
    """This test checks the situation when the topic got recovered more than
    once.
    Before the recovery every partition manifests will contain only segment
    names (e.g. 10282-9-v1.log).
    Because the partition could be moved the real segment path in the bucket
    might be different for different segments. Also, there could be more than
    one partition manifest that belong to the same topic in the bucket.
    To mitigate this recovery algorithm creates an aggregate partition manifest
    that contains segment paths instead of names (e.g. 
    b525cddd/kafka/panda-topic/0_9/4109-1-v1.log). When the new manifest is
    created the recovery uses it instead of the original manifests. When
    the topic is recovered this manifests is used by archival subsystem. It
    adds new entrires to the manifest that contains the segment names.
    So when we need to run recovery for the second time the manifest might be
    different from the regullar one. It will contain both paths and names.
    This test simply patches the existing partition manifest by replacing the
    names with paths and runs the recovery process.
    """
    def __init__(self, s3_client, kafka_tools, rpk_client, s3_bucket, logger,
                 topics):
        self.topics = topics
        self.total_bytes = 1024 * 1024 * 10
        super(CascadingRestore, self).__init__(s3_client, kafka_tools,
                                               rpk_client, s3_bucket, logger)

    def generate_baseline(self):
        """Produce enough data to trigger uploads to S3/minio"""
        for topic in self.topics:
            message_size = 1024
            num_messages = int(topic.partition_count * self.total_bytes /
                               message_size)
            self._kafka_tools.produce(topic.name, num_messages, message_size)

    def _move(self, src, dst):
        self.logger.info(f"moving {src} to {dst}")
        self._s3.move_object(self._bucket, src, dst, True)

    def _relocate_oldest_segment(self):
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
                segment_attr.append((name_or_path, attrs))
            assert len(
                segment_attr) != 0, "Manifest is not expected to be empty"
            segment_attr = sorted(segment_attr)
            target_name, target_attr = segment_attr[0]
            del segments[target_name]
            # create an entry that simulates previously
            # restored entry
            ns = obj['namespace']
            topic = obj['topic']
            partition = obj['partition']
            revision = obj['revision']
            new_revision = int(revision * 2)
            ntp = NTP(ns=ns, topic=topic, partition=partition)
            new_name = _gen_segment_path(ntp, new_revision, target_name)
            old_name = _gen_segment_path(ntp, revision, target_name)
            self.logger.info(
                f"replacing entry {target_name} with {new_name} in the manifest"
            )
            segments[new_name] = target_attr
            data = json.dumps(obj)
            self.logger.info(f"updated manifest {id}, content {data}")
            self._s3.put_object(self._bucket, id, data)
            self._move(old_name, new_name)

    def validate_cluster(self, baseline, restored):
        """Check that the topic is writeable"""
        self.logger.info(
            f"CascadingRestore.validate_cluster - baseline - {baseline}")
        self.logger.info(
            f"CascadingRestore.validate_cluster - restored - {restored}")
        self._validate_partition_last_offset()
        expected_topics = [
            topic.name for topic in self.expected_recovered_topics
        ]
        _find_offset_matches(baseline, restored, expected_topics, self.logger)
        for topic in self.topics:
            self._produce_and_verify(topic)

    def restore_redpanda(self, baseline, controller_checksums):
        self._relocate_oldest_segment()
        super(CascadingRestore, self).restore_redpanda(baseline,
                                                       controller_checksums)

    @property
    def second_restart_needed(self):
        """Return True if the restart is needed afer first validation steps"""
        return True

    def after_restart_validation(self):
        self.logger.info("after restart validation")
        for topic in self.topics:
            self._produce_and_verify(topic)


class TopicRecoveryTest(RedpandaTest):
    GLOBAL_S3_BUCKET = "s3_bucket"
    GLOBAL_S3_REGION = "s3_region"
    GLOBAL_S3_ACCESS_KEY = "s3_access_key"
    GLOBAL_S3_SECRET_KEY = "s3_secret_key"

    MINIO_HOST_NAME = "minio-s3"
    MINIO_BUCKET_NAME = "panda-bucket"
    MINIO_ACCESS_KEY = "panda-user"
    MINIO_SECRET_KEY = "panda-secret"
    MINIO_REGION = "panda-region"

    def __init__(self, test_context):
        self.s3_bucket = test_context.globals.get(self.GLOBAL_S3_BUCKET, None)
        self.s3_region = test_context.globals.get(self.GLOBAL_S3_REGION, None)
        self.s3_access_key = test_context.globals.get(
            self.GLOBAL_S3_ACCESS_KEY, None)
        self.s3_secret_key = test_context.globals.get(
            self.GLOBAL_S3_SECRET_KEY, None)
        self.s3_endpoint = None
        self.real_thing = self.s3_bucket and self.s3_region and self.s3_access_key and self.s3_secret_key
        if self.real_thing:
            extra_rp_conf = dict(
                cloud_storage_enabled=True,
                cloud_storage_access_key=self.s3_access_key,
                cloud_storage_secret_key=self.s3_secret_key,
                cloud_storage_region=self.s3_region,
                cloud_storage_bucket=self.s3_bucket,
                cloud_storage_reconciliation_interval_ms=10000,
                cloud_storage_max_connections=10,
                cloud_storage_trust_file="/etc/ssl/certs/ca-certificates.crt",
                log_segment_size=default_log_segment_size)
        else:
            self.s3_bucket = f"{TopicRecoveryTest.MINIO_BUCKET_NAME}-{uuid.uuid1()}"
            self.s3_region = TopicRecoveryTest.MINIO_REGION
            self.s3_access_key = TopicRecoveryTest.MINIO_ACCESS_KEY
            self.s3_secret_key = TopicRecoveryTest.MINIO_SECRET_KEY
            extra_rp_conf = dict(
                cloud_storage_enabled=True,
                cloud_storage_access_key=TopicRecoveryTest.MINIO_ACCESS_KEY,
                cloud_storage_secret_key=TopicRecoveryTest.MINIO_SECRET_KEY,
                cloud_storage_region=TopicRecoveryTest.MINIO_REGION,
                cloud_storage_bucket=self.s3_bucket,
                cloud_storage_disable_tls=True,
                cloud_storage_api_endpoint=TopicRecoveryTest.MINIO_HOST_NAME,
                cloud_storage_api_endpoint_port=9000,
                cloud_storage_reconciliation_interval_ms=10000,
                cloud_storage_max_connections=5,
                log_segment_size=default_log_segment_size)
            self.s3_endpoint = f'http://{TopicRecoveryTest.MINIO_HOST_NAME}:9000'

        super(TopicRecoveryTest, self).__init__(test_context=test_context,
                                                extra_rp_conf=extra_rp_conf)

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self._started = True

        self.s3_client = S3Client(region=self.s3_region,
                                  access_key=self.s3_access_key,
                                  secret_key=self.s3_secret_key,
                                  endpoint=self.s3_endpoint,
                                  logger=self.logger)

        self.rpk = RpkTool(self.redpanda)

    def setUp(self):
        self.s3_client.empty_bucket(self.s3_bucket)
        self.s3_client.create_bucket(self.s3_bucket)
        super().setUp()

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
            self.redpanda.clean_node(node)

    def _wait_for_topic(self,
                        recovered_topics,
                        timeout=datetime.timedelta(minutes=1)):
        """This method waits until the topic is created.
        It uses rpk describe topic command. The topic is
        considered to be recovered only if the leader is 
        elected.
        The method is time bound.
        """
        deadline = datetime.datetime.now() + timeout
        expected_num_leaders = sum(
            [t.partition_count for t in recovered_topics])
        while True:
            num_leaders = 0
            try:
                for topic in recovered_topics:
                    topic_state = self.rpk.describe_topic(topic.name)
                    # Describe topics only works after leader election succeded.
                    # We can use it to wait until the recovery is completed.
                    for partition in topic_state:
                        self.logger.info(f"partition: {partition}")
                        if partition.leader:
                            num_leaders += 1
            except:
                pass
            if num_leaders == expected_num_leaders:
                break
            ts = datetime.datetime.now()
            assert ts < deadline
            time.sleep(1)

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

    @cluster(num_nodes=3)
    def test_no_data(self):
        """If we're trying to recovery a topic which didn't have any data
        in old cluster the empty topic should be created. We should be able
        to produce to the topic."""
        test_case = NoDataCase(self.s3_client, self.kafka_tools, self.rpk,
                               self.s3_bucket, self.logger)
        self.do_run(test_case)

    @cluster(num_nodes=3)
    def test_missing_topic_manifest(self):
        """If we're trying to recovery a topic which didn't have any data
        in old cluster the empty topic should be created. We should be able
        to produce to the topic."""
        test_case = MissingTopicManifest(self.s3_client, self.kafka_tools,
                                         self.rpk, self.s3_bucket, self.logger)
        self.do_run(test_case)

    @cluster(num_nodes=3)
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

    @cluster(num_nodes=3)
    def test_missing_segment(self):
        """Test the handling of the missing segment. The segment is
        missing if it's present in the manifest but deleted from the
        bucket."""
        test_case = MissingSegment(self.s3_client, self.kafka_tools, self.rpk,
                                   self.s3_bucket, self.logger)
        self.do_run(test_case)

    @cluster(num_nodes=3)
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

    @cluster(num_nodes=3)
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

    @cluster(num_nodes=3)
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

    @cluster(num_nodes=3)
    def test_revision_shift1(self):
        """Test handling of situations when the revision of the recovered
        topic is equal to the revision of the original."""
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]
        test_case = RevShiftCheck(self.s3_client, self.kafka_tools, self.rpk,
                                  self.s3_bucket, self.logger, topics, 0)
        self.do_run(test_case)

    @cluster(num_nodes=3)
    def test_revision_shift2(self):
        """Test handling of situations when the revision of the recovered
        topic is less then the revision of the original."""
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]
        test_case = RevShiftCheck(self.s3_client, self.kafka_tools, self.rpk,
                                  self.s3_bucket, self.logger, topics, -1)
        self.do_run(test_case)

    @cluster(num_nodes=3)
    def test_revision_shift3(self):
        """Test handling of situations when the revision of the recovered
        topic is greater then the revision of the original."""
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]
        test_case = RevShiftCheck(self.s3_client, self.kafka_tools, self.rpk,
                                  self.s3_bucket, self.logger, topics, 1)
        self.do_run(test_case)

    @cluster(num_nodes=3)
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

    @cluster(num_nodes=3)
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

    @cluster(num_nodes=3)
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

    @cluster(num_nodes=3)
    def test_restore_moved_partition1(self):
        """Test handling of the moved partition (it will have different revision id)."""
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]
        test_case = MovedPartitionRestore(self.s3_client, self.kafka_tools,
                                          self.rpk, self.s3_bucket,
                                          self.logger, topics, False)
        self.do_run(test_case)

    @cluster(num_nodes=3)
    def test_restore_moved_partition2(self):
        """Test handling of the moved partition (it will have different revision id)."""
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]
        test_case = MovedPartitionRestore(self.s3_client, self.kafka_tools,
                                          self.rpk, self.s3_bucket,
                                          self.logger, topics, True)
        self.do_run(test_case)

    @cluster(num_nodes=3)
    def test_cascading_restore(self):
        """Test handling of the situation when the topic is recovered several times in a row."""
        topics = [
            TopicSpec(name='panda-topic',
                      partition_count=1,
                      replication_factor=3)
        ]
        test_case = CascadingRestore(self.s3_client, self.kafka_tools,
                                     self.rpk, self.s3_bucket, self.logger,
                                     topics)
        self.do_run(test_case)
