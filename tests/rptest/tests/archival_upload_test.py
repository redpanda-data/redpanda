# Copyright 2021 Vectorized, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md

from rptest.clients.kafka_cat import KafkaCat
from ducktape.mark.resource import cluster
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from ducktape.errors import DucktapeError
from rptest.tests.redpanda_test import RedpandaTest
from rptest.archival.s3_client import S3Client
from rptest.services.redpanda import RedpandaService

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools

from collections import namedtuple
import time
import os
import json
import traceback
import uuid
import sys

NTP = namedtuple("NTP", ['ns', 'topic', 'partition', 'revision'])

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
    def __init__(self, nodes, blocked_ip):
        self._nodes = nodes
        self._ip = blocked_ip

    def __enter__(self):
        """Isolate certain ips from the nodes using firewall rules"""
        cmd = []
        cmd.append(f"iptables -A INPUT -s {self._ip} -j DROP")
        cmd.append(f"iptables -A OUTPUT -d {self._ip} -j DROP")
        cmd = " && ".join(cmd)
        for node in self._nodes:
            node.account.ssh_output(cmd, allow_fail=False)

    def __exit__(self, type, value, traceback):
        """Remove firewall rules that isolate ips from the nodes"""
        cmd = []
        cmd.append(f"iptables -D INPUT -s {self._ip} -j DROP")
        cmd.append(f"iptables -D OUTPUT -d {self._ip} -j DROP")
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
    topics = tuple([
        TopicSpec(name=f'panda-topic-{ix}',
                  partition_count=10,
                  replication_factor=3) for ix in range(0, 10)
    ])

    GLOBAL_S3_BUCKET = "s3_bucket"
    GLOBAL_S3_REGION = "s3_region"
    GLOBAL_S3_ACCESS_KEY = "s3_access_key"
    GLOBAL_S3_SECRET_KEY = "s3_secret_key"

    MINIO_HOST_NAME = "minio-s3"
    MINIO_BUCKET_NAME = "panda-bucket"
    MINIO_ACCESS_KEY = "panda-user"
    MINIO_SECRET_KEY = "panda-secret"
    MINIO_REGION = "panda-region"
    MINIO_TOPIC_NAME = "panda-topic"

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
                developer_mode=True,
                disable_metrics=True,
                cloud_storage_enabled=True,
                cloud_storage_access_key=self.s3_access_key,
                cloud_storage_secret_key=self.s3_secret_key,
                cloud_storage_region=self.s3_region,
                cloud_storage_bucket=self.s3_bucket,
                cloud_storage_reconciliation_interval_ms=10000,
                cloud_storage_max_connections=10,
                cloud_storage_trust_file="/etc/ssl/certs/ca-certificates.crt",
                log_segment_size=32 * 1048576  # 32MB
            )
        else:
            bucket_name = f"{ArchivalTest.MINIO_BUCKET_NAME}-{uuid.uuid1()}"
            self.s3_bucket = bucket_name
            self.s3_region = ArchivalTest.MINIO_REGION
            self.s3_access_key = ArchivalTest.MINIO_ACCESS_KEY
            self.s3_secret_key = ArchivalTest.MINIO_SECRET_KEY
            extra_rp_conf = dict(
                developer_mode=True,
                disable_metrics=True,
                cloud_storage_enabled=True,
                cloud_storage_access_key=ArchivalTest.MINIO_ACCESS_KEY,
                cloud_storage_secret_key=ArchivalTest.MINIO_SECRET_KEY,
                cloud_storage_region=ArchivalTest.MINIO_REGION,
                cloud_storage_bucket=bucket_name,
                cloud_storage_disable_tls=True,
                cloud_storage_api_endpoint=ArchivalTest.MINIO_HOST_NAME,
                cloud_storage_api_endpoint_port=9000,
                cloud_storage_reconciliation_interval_ms=10000,
                cloud_storage_max_connections=5,
                log_segment_size=32 * 1048576  # 32MB
            )
            self.s3_endpoint = f'http://{ArchivalTest.MINIO_HOST_NAME}:9000'

        super(ArchivalTest, self).__init__(test_context=test_context,
                                           extra_rp_conf=extra_rp_conf)

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.s3_client = S3Client(region=self.s3_region,
                                  access_key=self.s3_access_key,
                                  secret_key=self.s3_secret_key,
                                  endpoint=self.s3_endpoint,
                                  logger=self.logger)

    def setUp(self):
        if not self.real_thing:
            self.s3_client.empty_bucket(self.s3_bucket)
            self.s3_client.create_bucket(self.s3_bucket)
        super().setUp()

    def tearDown(self):
        if not self.real_thing:
            self.s3_client.empty_bucket(self.s3_bucket)
        super().tearDown()

    @cluster(num_nodes=3)
    def test_write(self):
        """Simpe smoke test, write data to redpanda and check if the
        data hit the S3 storage bucket"""
        for topic in ArchivalTest.topics:
            time.sleep(5)
            self.kafka_tools.produce(topic.name, 10000, 64 * 1024)
        time.sleep(30)
        self._verify()

    def verify_remote(self):
        results = [
            loc.Key for loc in self.s3_client.list_objects(self.s3_bucket)
        ]
        self.logger.debug(f"ListObjects: {results}")

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
            return path[9:]  # 8-character hash + /

        def included(path):
            manifest_extension = ".json"
            return not path.endswith(manifest_extension)

        return {
            normalize(it.Key): (it.ETag, it.ContentLength)
            for it in self.s3_client.list_objects(self.s3_bucket)
            if included(it.Key)
        }

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
        manifest = self.s3_client.get_object_data(self.s3_bucket, id)
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

    def _verify(self):
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
                                    self.logger.info(f"partial match for segment {msegm.ntp} {msegm.base_offset}-" +\
                                                    f"{msegm.committed_offset} on {node_key}")
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
        results = [
            loc.Key for loc in self.s3_client.list_objects(self.s3_bucket)
        ]
        self.logger.debug(f"ListObjects: {results}")
        return results
