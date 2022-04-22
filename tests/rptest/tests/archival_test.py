# Copyright 2021 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from rptest.clients.kafka_cat import KafkaCat
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RedpandaService, SISettings

from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.util import (
    segments_count,
    produce_until_segments,
    wait_for_segments_removal,
)

from ducktape.mark import matrix

from collections import namedtuple, defaultdict
import time
import os
import json
import traceback
import uuid
import sys
import re

NTP = namedtuple("NTP", ['ns', 'topic', 'partition', 'revision'])

# Log errors expected when connectivity between redpanda and the S3
# backend is disrupted
CONNECTION_ERROR_LOGS = [
    "archival - .*Failed to create archivers",

    # e.g. archival - [fiber1] - service.cc:484 - Failed to upload 3 segments out of 4
    r"archival - .*Failed to upload \d+ segments"
]


class ValidationError(Exception):
    pass


def validate(fn, logger, timeout_sec, backoff_sec=5):
    deadline = time.monotonic() + timeout_sec
    current = time.monotonic()
    validated = False
    while current < deadline and not validated:
        try:
            fn()
            validated = True
        except AssertionError:
            time.sleep(backoff_sec)
            current = time.monotonic()
            if current < deadline:
                e, v = sys.exc_info()[:2]
                stacktrace = traceback.format_exc()
                logger.debug(
                    f"Validation attempt failed: {e} {v} {stacktrace}")
            else:
                raise
    assert validated


SegmentMetadata = namedtuple(
    'SegmentMetadata',
    ['ntp', 'base_offset', 'term', 'normalized_path', 'md5', 'size'])

ManifestRecord = namedtuple('ManifestRecord', [
    'ntp', 'base_offset', 'term', 'normalized_path', 'md5', 'committed_offset',
    'last_offset', 'size'
])


class firewall_blocked:
    """Temporary firewall barrier that isolates set of redpanda
    nodes from the ip-address"""
    def __init__(self, nodes, blocked_port):
        self._nodes = nodes
        self._port = blocked_port

    def __enter__(self):
        """Isolate certain ips from the nodes using firewall rules"""
        cmd = []
        cmd.append(f"iptables -A INPUT -p tcp --sport {self._port} -j DROP")
        cmd.append(f"iptables -A OUTPUT -p tcp --dport {self._port} -j DROP")
        cmd = " && ".join(cmd)
        for node in self._nodes:
            node.account.ssh_output(cmd, allow_fail=False)

    def __exit__(self, type, value, traceback):
        """Remove firewall rules that isolate ips from the nodes"""
        cmd = []
        cmd.append(f"iptables -D INPUT -p tcp --sport {self._port} -j DROP")
        cmd.append(f"iptables -D OUTPUT -p tcp --dport {self._port} -j DROP")
        cmd = " && ".join(cmd)
        for node in self._nodes:
            node.account.ssh_output(cmd, allow_fail=False)


def _parse_normalized_segment_path(path, md5, segment_size):
    """Parse path like 'kafka/panda-topic/1_8/3319-1-v1.log' and
    return the components - topic: panda-topic, ns: kafka, partition: 1
    revision: 8, base offset: 3319, term: 1"""
    items = path.split('/')
    ns = items[0]
    topic = items[1]
    part_rev = items[2].split('_')
    partition = int(part_rev[0])
    revision = int(part_rev[1])
    fname = items[3].split('-')
    base_offset = int(fname[0])
    term = int(fname[1])
    ntp = NTP(ns=ns, topic=topic, partition=partition, revision=revision)
    return SegmentMetadata(ntp=ntp,
                           base_offset=base_offset,
                           term=term,
                           normalized_path=path,
                           md5=md5,
                           size=segment_size)


def _parse_manifest_segment(manifest, sname, meta, remote_set, logger):
    ns = manifest["namespace"]
    topic = manifest["topic"]
    partition = manifest["partition"]
    revision = manifest["revision"]
    last_offset = manifest["last_offset"]
    committed_offset = meta["committed_offset"]
    size_bytes = meta["size_bytes"]
    normalized_path = f"{ns}/{topic}/{partition}_{revision}/{sname}"
    md5 = None
    for r, (m, sz) in remote_set.items():
        if normalized_path == r:
            md5 = m
            if sz != size_bytes:
                logger.warning(
                    f"segment {sname} has unexpected size, size {size_bytes} expected {sz} found"
                )
    if md5 is None:
        logger.debug(f"Can't parse manifest segment {sname} over {remote_set}")
    assert not md5 is None
    sm = _parse_normalized_segment_path(normalized_path, md5, size_bytes)
    return ManifestRecord(ntp=sm.ntp,
                          base_offset=sm.base_offset,
                          term=sm.term,
                          normalized_path=normalized_path,
                          md5=md5,
                          committed_offset=committed_offset,
                          last_offset=last_offset,
                          size=size_bytes)


class ArchivalTest(RedpandaTest):
    log_segment_size = 1048576  # 1MB
    log_compaction_interval_ms = 10000

    s3_topic_name = "panda-topic"
    topics = (TopicSpec(name=s3_topic_name,
                        partition_count=1,
                        replication_factor=3), )

    def __init__(self, test_context):
        si_settings = SISettings(cloud_storage_reconciliation_interval_ms=500,
                                 cloud_storage_max_connections=5,
                                 log_segment_size=self.log_segment_size)
        self.s3_bucket_name = si_settings.cloud_storage_bucket

        extra_rp_conf = dict(
            log_compaction_interval_ms=self.log_compaction_interval_ms,
            log_segment_size=self.log_segment_size)

        if test_context.function_name == "test_timeboxed_uploads":
            si_settings.log_segment_size = 1024 * 1024 * 1024
            extra_rp_conf.update(
                cloud_storage_segment_max_upload_interval_sec=1)

        super(ArchivalTest, self).__init__(test_context=test_context,
                                           extra_rp_conf=extra_rp_conf,
                                           si_settings=si_settings)

        self._s3_port = si_settings.cloud_storage_api_endpoint_port

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)

    def setUp(self):
        super().setUp()  # topic is created here

        # enable archival for topic
        for topic in self.topics:
            self.rpk.alter_topic_config(topic.name, 'redpanda.remote.write',
                                        'true')

    def tearDown(self):
        self.s3_client.empty_bucket(self.s3_bucket_name)
        super().tearDown()

    @cluster(num_nodes=3)
    def test_write(self):
        """Simpe smoke test, write data to redpanda and check if the
        data hit the S3 storage bucket"""
        self.kafka_tools.produce(self.topic, 10000, 1024)
        validate(self._quick_verify, self.logger, 90)

    @cluster(num_nodes=3, log_allow_list=CONNECTION_ERROR_LOGS)
    def test_isolate(self):
        """Verify that our isolate/rejoin facilities actually work"""
        with firewall_blocked(self.redpanda.nodes, self._s3_port):
            self.kafka_tools.produce(self.topic, 10000, 1024)
            time.sleep(10)  # can't busy wait here

            # Topic manifest can be present in the bucket because topic is created before
            # firewall is blocked. No segments or partition manifest should be present.
            topic_manifest_id = "d0000000/meta/kafka/panda-topic/topic_manifest.json"
            objects = self.s3_client.list_objects(self.s3_bucket_name)
            keys = [x.Key for x in objects]

            assert len(keys) < 2, \
                f"Bucket should be empty or contain only {topic_manifest_id}, but contains {keys}"

            if len(keys) == 1:
                assert topic_manifest_id == keys[0], \
                    f"Bucket should be empty or contain only {topic_manifest_id}, but contains {keys[0]}"

    @cluster(num_nodes=3, log_allow_list=CONNECTION_ERROR_LOGS)
    def test_reconnect(self):
        """Disconnect redpanda from S3, write data, connect redpanda to S3
        and check that the data is uploaded"""
        with firewall_blocked(self.redpanda.nodes, self._s3_port):
            self.kafka_tools.produce(self.topic, 10000, 1024)
            time.sleep(10)  # sleep is needed because we need to make sure that
            # reconciliation loop kicked in and started uploading
            # data, otherwse we can rejoin before archival storage
            # will even try to upload new segments
        validate(self._quick_verify, self.logger, 90)

    @cluster(num_nodes=3, log_allow_list=CONNECTION_ERROR_LOGS)
    def test_one_node_reconnect(self):
        """Disconnect one redpanda node from S3, write data, connect redpanda to S3
        and check that the data is uploaded"""
        self.kafka_tools.produce(self.topic, 1000, 1024)
        leaders = list(self._get_partition_leaders().values())
        with firewall_blocked(leaders[0:1], self._s3_port):
            self.kafka_tools.produce(self.topic, 9000, 1024)
            time.sleep(10)  # sleep is needed because we need to make sure that
            # reconciliation loop kicked in and started uploading
            # data, otherwse we can rejoin before archival storage
            # will even try to upload new segments
        validate(self._quick_verify, self.logger, 90)

    @cluster(num_nodes=3, log_allow_list=CONNECTION_ERROR_LOGS)
    def test_connection_drop(self):
        """Disconnect redpanda from S3 during the active upload, restore connection
        and check that everything is uploaded"""
        self.kafka_tools.produce(self.topic, 10000, 1024)
        with firewall_blocked(self.redpanda.nodes, self._s3_port):
            time.sleep(10)  # sleep is needed because we need to make sure that
            # reconciliation loop kicked in and started uploading
            # data, otherwse we can rejoin before archival storage
            # will even try to upload new segments
        validate(self._quick_verify, self.logger, 90)

    @cluster(num_nodes=3, log_allow_list=CONNECTION_ERROR_LOGS)
    def test_connection_flicker(self):
        """Disconnect redpanda from S3 during the active upload for short period of time
        during upload and check that everything is uploaded"""
        con_enabled = True
        for _ in range(0, 20):
            # upload data in batches
            if con_enabled:
                with firewall_blocked(self.redpanda.nodes, self._s3_port):
                    self.kafka_tools.produce(self.topic, 500, 1024)
            else:
                self.kafka_tools.produce(self.topic, 500, 1024)
            con_enabled = not con_enabled
            time.sleep(1)
        time.sleep(10)
        validate(self._quick_verify, self.logger, 90)

    @cluster(num_nodes=3)
    def test_single_partition_leadership_transfer(self):
        """Start uploading data, restart leader node of the partition 0 to trigger the
        leadership transfer, continue upload, verify S3 bucket content"""
        self.kafka_tools.produce(self.topic, 5000, 1024)
        time.sleep(5)
        leaders = self._get_partition_leaders()
        node = leaders[0]
        self.redpanda.stop_node(node)
        time.sleep(1)
        self.redpanda.start_node(node)
        time.sleep(5)
        self.kafka_tools.produce(self.topic, 5000, 1024)
        validate(self._cross_node_verify, self.logger, 90)

    @cluster(num_nodes=3)
    def test_all_partitions_leadership_transfer(self):
        """Start uploading data, restart leader nodes of all partitions to trigger the
        leadership transfer, continue upload, verify S3 bucket content"""
        self.kafka_tools.produce(self.topic, 5000, 1024)
        time.sleep(5)
        leaders = self._get_partition_leaders()
        for ip, node in leaders.items():
            self.logger.debug(f"going to restart node {ip}")
            self.redpanda.stop_node(node)
            time.sleep(1)
            self.redpanda.start_node(node)
        time.sleep(5)
        self.kafka_tools.produce(self.topic, 5000, 1024)
        validate(self._cross_node_verify, self.logger, 90)

    @cluster(num_nodes=3)
    def test_timeboxed_uploads(self):
        """This test checks segment upload time limit. The feature is enabled in the
        configuration. The configuration defines maximum time interval between uploads.
        If the option is set then redpanda will start uploading a segment partially if
        configured amount of time passed since previous upload and the segment has some
        new data.
        The test sets the timeout value to 1s. Then it uploads data in batches with delays
        between the batches. The segment size is set to 1GiB. We upload 10MiB total. So
        normally, there won't be any data uploaded to Minio. But since the time limit for
        a segment is set to 1s we will see a bunch of segments in the bucket. The offsets
        of the segments won't align with the segment in the redpanda data directory. But
        their respective offset ranges should align and the sizes should make sense.
        """

        # The offsets of the segments in the Minio bucket won't necessary
        # correlate with the write bursts here. The upload depends on the
        # timeout but also on raft and current high_watermark. So we can
        # expect that the bucket won't have 9 segments with 1000 offsets.
        # The actual segments will be larger.
        for _ in range(0, 10):
            self.kafka_tools.produce(self.topic, 1000, 1024)
            time.sleep(1)
        time.sleep(5)

        def check_upload():
            # check that the upload happened
            ntps = set()
            sizes = {}

            for node in self.redpanda.nodes:
                checksums = self._get_redpanda_log_segment_checksums(node)
                self.logger.info(
                    f"Node: {node.account.hostname} checksums: {checksums}")
                lst = [
                    _parse_normalized_segment_path(path, md5, size)
                    for path, (md5, size) in checksums.items()
                ]
                lst = sorted(lst, key=lambda x: x.base_offset)
                segments = defaultdict(int)
                sz = defaultdict(int)
                for it in lst:
                    ntps.add(it.ntp)
                    sz[it.ntp] += it.size
                    segments[it.ntp] += 1
                for ntp, s in segments.items():
                    assert s != 0, f"expected to have at least one segment per partition, got {s}"
                for ntp, s in sz.items():
                    if ntp not in sizes:
                        sizes[ntp] = s

            # Download manifest for partitions
            for ntp in ntps:
                manifest = self._download_partition_manifest(ntp)
                self.logger.info(f"downloaded manifest {manifest}")
                segments = []
                for _, segment in manifest['segments'].items():
                    segments.append(segment)

                segments = sorted(segments, key=lambda s: s['base_offset'])
                self.logger.info(f"sorted segments {segments}")

                prev_committed_offset = -1
                size = 0
                for segment in segments:
                    self.logger.info(
                        f"checking {segment} prev: {prev_committed_offset}")
                    base_offset = segment['base_offset']
                    assert prev_committed_offset + 1 == base_offset, f"inconsistent segments, " +\
                        "expected base_offset: {prev_committed_offset + 1}, actual: {base_offset}"
                    prev_committed_offset = segment['committed_offset']
                    size += segment['size_bytes']
                assert sizes[ntp] >= size
                assert size > 0

        validate(check_upload, self.logger, 90)

    @cluster(num_nodes=3, log_allow_list=CONNECTION_ERROR_LOGS)
    @matrix(acks=[1, -1])
    def test_retention_archival_coordination(self, acks):
        """
        Test that only archived segments can be evicted and that eviction
        restarts once the segments have been archived.
        """
        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_BYTES: 5 * self.log_segment_size,
            },
        )

        with firewall_blocked(self.redpanda.nodes, self._s3_port):
            produce_until_segments(redpanda=self.redpanda,
                                   topic=self.topic,
                                   partition_idx=0,
                                   count=10,
                                   acks=acks)

            # Sleep some time sufficient for log eviction under normal conditions
            # and check that no segment has been evicted (because we can't upload
            # segments to the cloud storage).
            time.sleep(3 * self.log_compaction_interval_ms / 1000.0)
            counts = list(
                segments_count(self.redpanda, self.topic, partition_idx=0))
            self.logger.info(f"node segment counts: {counts}")
            assert len(counts) == len(self.redpanda.nodes)
            assert all(c >= 10 for c in counts)

        # Check that eviction restarts after we restored the connection to cloud
        # storage.
        wait_for_segments_removal(redpanda=self.redpanda,
                                  topic=self.topic,
                                  partition_idx=0,
                                  count=6)

    def _check_bucket_is_emtpy(self):
        allobj = self._list_objects()
        for obj in allobj:
            self.logger.debug(
                f"found object {obj} in bucket {self.s3_bucket_name}")
        assert len(allobj) == 0

    def _get_partition_leaders(self):
        kcat = KafkaCat(self.redpanda)
        m = kcat.metadata()
        self.logger.info(f"kcat.metadata() == {m}")
        brokers = {}
        for b in m['brokers']:
            id = b['id']
            ip = b['name']
            ip = ip[:ip.index(':')]
            for n in self.redpanda.nodes:
                n_ip = n.account.hostname
                self.logger.debug(f"matching {n_ip} over {ip}")
                if n_ip == ip:
                    brokers[id] = n
                    break
        self.logger.debug(f"found brokers {brokers}")
        assert len(brokers) == 3
        leaders = {}
        for topic in m['topics']:
            if topic['topic'] == ArchivalTest.s3_topic_name:
                for part in topic['partitions']:
                    leader_id = part['leader']
                    partition_id = part['partition']
                    leader = brokers[leader_id]
                    leaders[partition_id] = leader
        return leaders

    def _download_partition_manifest(self, ntp):
        """Find and download individual partition manifest"""
        expected = f"{ntp.ns}/{ntp.topic}/{ntp.partition}_{ntp.revision}/manifest.json"
        id = None
        objects = []
        for loc in self._list_objects():
            objects.append(loc)
            if expected in loc:
                id = loc
                break
        if id is None:
            objlist = "\n".join(objects)
            self.logger.debug(
                f"expected path {expected} is not found in the bucket, bucket content: \n{objlist}"
            )
            assert not id is None
        manifest = self.s3_client.get_object_data(self.s3_bucket_name, id)
        self.logger.info(f"manifest found: {manifest}")
        return json.loads(manifest)

    def _verify_manifest(self, ntp, manifest, remote):
        """Check that all segments that present in manifest are available
        in remote storage"""
        for sname, _ in manifest['segments'].items():
            spath = f"{ntp.ns}/{ntp.topic}/{ntp.partition}_{ntp.revision}/{sname}"
            self.logger.info(f"validating manifest path {spath}")
            assert spath in remote
        ranges = [(int(m['base_offset']), int(m['committed_offset']))
                  for _, m in manifest['segments'].items()]
        ranges = sorted(ranges, key=lambda x: x[0])
        last_offset = -1
        num_gaps = 0
        for base, committed in ranges:
            if last_offset + 1 != base:
                self.logger.debug(
                    f"gap between {last_offset} and {base} detected")
                num_gaps += 1
            last_offset = committed
        assert num_gaps == 0

    def _cross_node_verify(self):
        """Verify data on all nodes taking into account possible alignment issues
        caused by leadership transitions.
        The verification algorithm is following:
        - Download and verify partition manifest;
        - Partition manifest has all segments and metadata like committed offset
          and base offset. We can also retrieve MD5 hash of every segment;
        - Load segment metadata for every redpanda node.
        - Scan every node's metadata and match segments with manifest, on success
          remove matched segment from the partition manifest.
        The goal #1 is to remove all segments from the manifest. The goal #2 is to
        find the last segment that's supposed to be uploaded from the leader node,
        it's base offset should be equal to manifest's last offset + 1.
        The segments match if:
        - The base offset and md5 hashes are the same;
        - The committed offset of both segments are the same, md5 hashes are different,
          and base offset of the segment from manifest is larger than base offset of the
          segment from redpanda node. In this case we should also compare the data
          directly by scanning both segments.
        """
        nodes = {}
        ntps = set()

        for node in self.redpanda.nodes:
            checksums = self._get_redpanda_log_segment_checksums(node)
            self.logger.info(
                f"Node: {node.account.hostname} checksums: {checksums}")
            lst = [
                _parse_normalized_segment_path(path, md5, size)
                for path, (md5, size) in checksums.items()
            ]
            lst = sorted(lst, key=lambda x: x.base_offset)
            nodes[node.account.hostname] = lst
            for it in lst:
                ntps.add(it.ntp)

        # Download metadata from S3
        remote = self._get_redpanda_s3_checksums()

        # Download manifest for partitions
        manifests = {}
        for ntp in ntps:
            manifest = self._download_partition_manifest(ntp)
            manifests[ntp] = manifest
            self._verify_manifest(ntp, manifest, remote)

        for ntp in ntps:
            self.logger.debug(f"verifying {ntp}")
            manifest = manifests[ntp]
            segments = manifest['segments']
            manifest_segments = [
                _parse_manifest_segment(manifest, sname, meta, remote,
                                        self.logger)
                for sname, meta in segments.items()
            ]
            manifest_segments = sorted(manifest_segments,
                                       key=lambda x: x.base_offset)

            for node_key, node_segments in nodes.items():
                self.logger.debug(f"checking {ntp} on {node_key}")
                for mix, msegm in enumerate(manifest_segments):
                    if not msegm is None:
                        segments = sorted([
                            segment
                            for segment in node_segments if segment.ntp == ntp
                        ],
                                          key=lambda x: x.base_offset)
                        self.logger.debug(
                            f"checking manifest segment {msegm} over {node_key} segments {segments}"
                        )
                        found = False
                        for ix in range(0, len(segments)):
                            nsegm = segments[ix]
                            if nsegm.ntp != ntp:
                                continue
                            nsegm_co = -1 if (ix + 1) == len(segments) else (
                                segments[ix + 1].base_offset - 1)
                            self.logger.debug(
                                f"comparing {msegm.base_offset}:{msegm.committed_offset}:{msegm.md5} to {nsegm.base_offset}:{nsegm_co}:{nsegm.md5}"
                            )
                            if msegm.base_offset == nsegm.base_offset and msegm.md5 == nsegm.md5:
                                # Success
                                self.logger.info(
                                    f"found match for segment {msegm.ntp} {msegm.base_offset} on {node_key}"
                                )
                                manifest_segments[mix] = None
                                found = True
                                break
                            if msegm.committed_offset == nsegm_co and msegm.base_offset > nsegm.base_offset:
                                # Found segment with truncated head (due to leadership transition)
                                actual_hash = self._get_partial_checksum(
                                    node_key, nsegm.normalized_path,
                                    msegm.size)
                                self.logger.info(
                                    f"partial hash {actual_hash} retreived, s3 hash {msegm.md5}"
                                )
                                if actual_hash == msegm.md5:
                                    manifest_segments[mix] = None
                                    self.logger.info(
                                        f"partial match for segment {msegm.ntp} {msegm.base_offset}-"
                                        +
                                        f"{msegm.committed_offset} on {node_key}"
                                    )
                                    found = True
                                    break
                        if not found:
                            self.logger.debug(
                                f"failed to match {msegm.base_offset}:{msegm.committed_offset}"
                            )
                        else:
                            self.logger.debug(
                                f"matched {msegm.base_offset}:{msegm.committed_offset} successfully"
                            )

            # All segments should be matched and set to None
            if any(manifest_segments):
                self.logger.debug(
                    f"manifest segments that fail to validate: {manifest_segments}"
                )
            assert not any(manifest_segments)
            # Verify goal #2, the last segment on a leader node is manifest.last_offset + 1
            ntp_offsets = []
            for node_key, node_segments in nodes.items():
                offsets = [
                    segm.base_offset for segm in node_segments
                    if segm.ntp == ntp
                ]
                if offsets:
                    max_offset = max([
                        segm.base_offset for segm in node_segments
                        if segm.ntp == ntp
                    ])
                    ntp_offsets.append(max_offset)
                    self.logger.debug(
                        f"NTP {ntp} has the largest offset {max_offset} on node {node_key}"
                    )
                else:
                    self.logger.debug(
                        f"NTP {ntp} has no offsets on node {node_key}")

            last_offset = int(manifest['last_offset'])
            self.logger.debug(
                f"last offset: {last_offset}, ntp offsets: {ntp_offsets}")
            assert (last_offset + 1) in ntp_offsets

    def _list_objects(self):
        """Emulate ListObjects call by fetching the topic manifests and
        iterating through its content"""
        try:
            topic_manifest_id = "d0000000/meta/kafka/panda-topic/topic_manifest.json"
            partition_manifest_id = "d0000000/meta/kafka/panda-topic/0_9/manifest.json"
            manifest = self.s3_client.get_object_data(self.s3_bucket_name,
                                                      partition_manifest_id)
            results = [topic_manifest_id, partition_manifest_id]
            for id in manifest['segments'].keys():
                results.append(id)
            self.logger.debug(f"ListObjects(source: manifest): {results}")
        except:
            results = [
                loc.Key
                for loc in self.s3_client.list_objects(self.s3_bucket_name)
            ]
            self.logger.debug(f"ListObjects: {results}")
        return results

    def _quick_verify(self):
        """Verification algorithm that works only if no leadership
        transfer happend during the run. It works by looking up all
        segments from the remote storage in local redpanda storages.
        It's done by using md5 hashes of the nodes.
        """
        local = {}
        for node in self.redpanda.nodes:
            checksums = self._get_redpanda_log_segment_checksums(node)
            self.logger.info(
                f"Node: {node.account.hostname} checksums: {checksums}")
            for k, v in checksums.items():
                local.setdefault(k, set()).add(v)
        remote = self._get_redpanda_s3_checksums()
        self.logger.info(f"S3 checksums: {remote}")
        self.logger.info(f"Local checksums: {local}")
        assert len(local) != 0
        assert len(remote) != 0
        md5fails = 0
        lookup_fails = 0
        for path, csum in remote.items():
            self.logger.info(f"checking remote path: {path} csum: {csum}")
            if path not in local:
                self.logger.debug(
                    f"remote path {path} can't be found in any of the local storages"
                )
                lookup_fails += 1
            else:
                if len(local[path]) != 1:
                    self.logger.info(
                        f"remote segment {path} have more than one variant {local[path]}"
                    )
                if not csum in local[path]:
                    self.logger.debug(
                        f"remote md5 {csum} doesn't match any local {local[path]}"
                    )
                    md5fails += 1
        if md5fails != 0:
            self.logger.debug(
                f"Validation failed, {md5fails} remote segments doesn't match")
        if lookup_fails != 0:
            self.logger.debug(
                f"Validation failed, remote {lookup_fails} remote locations doesn't match local"
            )
        assert md5fails == 0 and lookup_fails == 0

        # Validate partitions
        # for every partition the segment with largest base offset shouldn't be
        # available in remote storage
        local_partitions = {}
        remote_partitions = {}
        for path, items in local.items():
            meta = _parse_normalized_segment_path(path, '', 0)
            local_partitions.setdefault(meta.ntp, []).append((meta, items))
        for path, items in remote.items():
            meta = _parse_normalized_segment_path(path, '', 0)
            remote_partitions.setdefault(meta.ntp, []).append((meta, items))
        self.logger.info(
            f"generated local partitions {local_partitions.keys()}")
        self.logger.info(
            f"generated remote partitions {remote_partitions.keys()}")

        # Download manifest for partitions
        manifests = {}
        for ntp in local_partitions.keys():
            manifest = self._download_partition_manifest(ntp)
            manifests[ntp] = manifest
            self._verify_manifest(ntp, manifest, remote)

        # Check that all local partition are archived
        assert len(local_partitions) == 1
        assert len(remote_partitions) == 1
        missing_partitions = 0
        for key in local_partitions.keys():
            if key not in remote_partitions:
                self.logger.debug(f"partition {key} not found in remote set")
                missing_partitions += 1
        assert missing_partitions == 0

    def _get_redpanda_log_segment_checksums(self, node):
        """Get MD5 checksums of log segments that match the topic. The paths are
        normalized (<namespace>/<topic>/<partition>_<rev>/...)."""
        checksums = self.redpanda.data_checksum(node)

        # Filter out all unwanted paths
        def included(path):
            controller_log_prefix = os.path.join(RedpandaService.DATA_DIR,
                                                 "redpanda")
            log_segment_extension = ".log"
            return not path.startswith(
                controller_log_prefix) and path.endswith(log_segment_extension)

        # Remove data dir from path
        def normalize_path(path):
            return os.path.relpath(path, RedpandaService.DATA_DIR)

        return {
            normalize_path(path): value
            for path, value in checksums.items() if included(path)
        }

    def _get_redpanda_s3_checksums(self):
        """Get MD5 checksums of log segments stored in S3 (minio). The paths are
        normalized (<namespace>/<topic>/<partition>_<rev>/...)."""
        def normalize(path):
            # strip archiver term id from the segment path
            match = re.search(r'.log(\.\d+)$', path)
            if match:
                path = path[:-len(match[1])]
            return path[9:]  # 8-character hash + /

        def included(path):
            manifest_extension = ".json"
            return not path.endswith(manifest_extension)

        objects = self.s3_client.list_objects(self.s3_bucket_name)
        self.logger.info(
            f"got {len(list(objects))} objects from bucket {self.s3_bucket_name}"
        )
        for o in objects:
            self.logger.info(f"object: {o}")

        return {
            normalize(it.Key): (it.ETag, it.ContentLength)
            for it in self.s3_client.list_objects(self.s3_bucket_name)
            if included(it.Key)
        }

    def _get_partial_checksum(self, hostname, normalized_path, tail_bytes):
        """Compute md5 checksum of the last 'tail_bytes' of the file located
        on a node."""
        node = None
        for n in self.redpanda.nodes:
            if n.account.hostname == hostname:
                node = n
        full_path = os.path.join(RedpandaService.DATA_DIR, normalized_path)
        cmd = f"tail -c {tail_bytes} {full_path} | md5sum"
        line = node.account.ssh_output(cmd)
        tokens = line.split()
        return tokens[0].decode()

    def _isolate(self, nodes, ips):
        """Isolate certain ips from the nodes using firewall rules"""
        cmd = []
        for ip in ips:
            cmd.append(f"iptables -A INPUT -s {ip} -j DROP")
            cmd.append(f"iptables -A OUTPUT -d {ip} -j DROP")
        cmd = " && ".join(cmd)
        for node in nodes:
            node.account.ssh_output(cmd, allow_fail=False)

    def _rejoin(self, nodes, ips):
        """Remove firewall rules that isolate ips from the nodes"""
        cmd = []
        for ip in ips:
            cmd.append(f"iptables -D INPUT -s {ip} -j DROP")
            cmd.append(f"iptables -D OUTPUT -d {ip} -j DROP")
        cmd = " && ".join(cmd)
        for node in nodes:
            node.account.ssh_output(cmd, allow_fail=False)

    def _host_name_to_ip_address(self, hostname):
        ip_host = self.redpanda.nodes[0].account.ssh_output(
            f'getent hosts {hostname}')
        return ip_host.split()[0].decode()

    def _get_s3_endpoint_ip(self):
        return self._host_name_to_ip_address(ArchivalTest.s3_host_name)

    def _get_rp_cluster_ips(self, nhosts=4):
        lst = []
        for ix in range(1, nhosts + 1):
            h = f"rp_n{ix}_1"
            lst.append(self._host_name_to_ip_address(h))
        return lst
