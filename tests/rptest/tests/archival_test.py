# Copyright 2021 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import os
import re
import sys
import time
import traceback
from collections import defaultdict, namedtuple
from typing import DefaultDict, Optional

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.kafka_cat import KafkaCat
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    RedpandaService,
    SISettings,
    get_cloud_storage_type,
)
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import (
    firewall_blocked,
    produce_until_segments,
    segments_count,
    wait_for_local_storage_truncate,
)
from rptest.utils.si_utils import (
    NTPR,
    BucketView,
    gen_local_path_from_remote,
    gen_segment_name_from_meta,
)

# First capture group is the log name. The last (optional) group is the archiver term to be removed.
LOG_EXPRESSION = re.compile(r"(.*\.log)(\.\d+)?$")

MANIFEST_EXTENSION = ".json"
MANIFEST_BIN_EXTENSION = ".bin"

LOG_EXTENSION = ".log"

CONTROLLER_LOG_PREFIX = os.path.join(RedpandaService.DATA_DIR, "redpanda")
INTERNAL_TOPIC_PREFIX = os.path.join(RedpandaService.DATA_DIR, "kafka_internal")

# Log errors expected when connectivity between redpanda and the S3
# backend is disrupted
CONNECTION_ERROR_LOGS = [
    "archival - .*Failed to create archivers",
    # e.g. archival - [fiber1] - service.cc:484 - Failed to upload 3 segments out of 4
    r"archival - .*Failed to upload \d+ segments",
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
                logger.debug(f"Validation attempt failed: {e} {v} {stacktrace}")
            else:
                raise
    assert validated


SegmentMetadata = namedtuple(
    "SegmentMetadata", ["ntp", "base_offset", "term", "normalized_path", "md5", "size"]
)

ManifestRecord = namedtuple(
    "ManifestRecord",
    [
        "ntp",
        "base_offset",
        "term",
        "normalized_path",
        "md5",
        "committed_offset",
        "last_offset",
        "size",
    ],
)


def _get_name_version(path):
    """Return segment size based on path"""
    items = path.split("/")
    name = items[-1]
    ndelim = name.count("-")
    if ndelim == 2:
        return "v1"
    elif ndelim == 4:
        return "v2"  # v3 is the same format
    raise ValueError(f"unexpected path format {path}")


def _parse_normalized_segment_path_v1(path, md5, segment_size):
    """Parse path like 'kafka/panda-topic/1_8/3319-1-v1.log' and
    return the components - topic: panda-topic, ns: kafka, partition: 1
    revision: 8, base offset: 3319, term: 1"""
    items = path.split("/")
    ns = items[0]
    topic = items[1]
    part_rev = items[2].split("_")
    partition = int(part_rev[0])
    revision = int(part_rev[1])
    fname = items[3].split("-")
    base_offset = int(fname[0])
    term = int(fname[1])
    ntp = NTPR(ns=ns, topic=topic, partition=partition, revision=revision)
    return SegmentMetadata(
        ntp=ntp,
        base_offset=base_offset,
        term=term,
        normalized_path=path,
        md5=md5,
        size=segment_size,
    )


def _parse_normalized_segment_path_v2_v3(path, md5, segment_size):
    """Parse path like 'kafka/panda-topic/1_8/3319-3421-2817-1-v1.log' and
    return the components - topic: panda-topic, ns: kafka, partition: 1
    revision: 8, base offset: 3319, committed offset 3421, size 2817 term: 1
    """
    items = path.split("/")
    ns = items[0]
    topic = items[1]
    part_rev = items[2].split("_")
    partition = int(part_rev[0])
    revision = int(part_rev[1])
    fname = items[3].split("-")
    base_offset = int(fname[0])
    term = int(fname[1])
    ntp = NTPR(ns=ns, topic=topic, partition=partition, revision=revision)
    return SegmentMetadata(
        ntp=ntp,
        base_offset=base_offset,
        term=term,
        normalized_path=path,
        md5=md5,
        size=segment_size,
    )


def _parse_normalized_segment_path(path, md5, segment_size):
    if _get_name_version(path) == "v1":
        return _parse_normalized_segment_path_v1(path, md5, segment_size)
    return _parse_normalized_segment_path_v2_v3(path, md5, segment_size)


def _parse_manifest_segment(manifest, sname, meta, remote_set, logger):
    ns = manifest["namespace"]
    topic = manifest["topic"]
    partition = manifest["partition"]
    revision = manifest["revision"]
    last_offset = manifest["last_offset"]
    committed_offset = meta["committed_offset"]
    size_bytes = meta["size_bytes"]
    segment_name = gen_segment_name_from_meta(meta, sname)
    normalized_path = f"{ns}/{topic}/{partition}_{revision}/{segment_name}"
    md5 = None
    for r, (m, sz) in remote_set.items():
        if normalized_path == r:
            md5 = m
            if sz != size_bytes:
                logger.warning(
                    f"segment {segment_name} has unexpected size, size {size_bytes} expected {sz} found"
                )
    if md5 is None:
        logger.debug(f"Can't parse manifest segment {segment_name} over {remote_set}")
    assert md5 is not None
    sm = _parse_normalized_segment_path(normalized_path, md5, size_bytes)
    return ManifestRecord(
        ntp=sm.ntp,
        base_offset=sm.base_offset,
        term=sm.term,
        normalized_path=normalized_path,
        md5=md5,
        committed_offset=committed_offset,
        last_offset=last_offset,
        size=size_bytes,
    )


def make_index_path(path: str) -> str:
    return f"{path}.index"


class ArchivalTest(RedpandaTest):
    log_segment_size = 1048576  # 1MB
    log_compaction_interval_ms = 10000

    s3_topic_name = "panda-topic"
    topics = (
        TopicSpec(
            name=s3_topic_name,
            partition_count=1,
            replication_factor=3,
            cleanup_policy=None,
        ),
    )

    def __init__(self, test_context):
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=5,
            log_segment_size=self.log_segment_size,
        )
        self.s3_bucket_name = si_settings.cloud_storage_bucket

        extra_rp_conf = dict(
            log_compaction_interval_ms=self.log_compaction_interval_ms,
            log_segment_size=self.log_segment_size,
        )

        if test_context.function_name == "test_timeboxed_uploads":
            si_settings.log_segment_size = 1024 * 1024 * 1024
            extra_rp_conf.update(cloud_storage_segment_max_upload_interval_sec=1)

        if test_context.function_name == "test_all_partitions_leadership_transfer":
            extra_rp_conf.update(cloud_storage_manifest_max_upload_interval_sec=30)

        super().__init__(
            test_context=test_context,
            extra_rp_conf=extra_rp_conf,
            si_settings=si_settings,
        )

        self._s3_port = self.si_settings.cloud_storage_api_endpoint_port

        self.kafka_tools = KafkaCliTools(self.redpanda)
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

    def setUp(self):
        super().setUp()  # topic is created here

        # enable archival for topic
        for topic in self.topics:
            self.rpk.alter_topic_config(topic.name, "redpanda.remote.write", "true")

    @cluster(num_nodes=3, log_allow_list=CONNECTION_ERROR_LOGS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_isolate(self, cloud_storage_type):
        """Verify that our isolate/rejoin facilities actually work"""

        with firewall_blocked(self.redpanda.nodes, self._s3_port):
            self.kafka_tools.produce(self.topic, 10000, 1024)
            time.sleep(10)  # can't busy wait here

            # Topic manifest can be present in the bucket because topic is created before
            # firewall is blocked. No segments or partition manifest should be present.
            bucket_content = BucketView(self.redpanda, topics=self.topics)

            # Any partition manifests must contain no segments
            for ntp, manifest in bucket_content.partition_manifests.items():
                assert not manifest.get("segments", []), (
                    f"Segments found in a manifest {ntp}"
                )

            # No segments must have been uploaded
            assert bucket_content.segment_objects == 0, "Data segments found"

            # All objects must belong to the topic we created (make sure we aren't searching on the wrong topic)
            if bucket_content.ignored_objects > 0:
                raise RuntimeError("Unexpected objects in bucket")

        # Firewall is unblocked, segment uploads should proceed
        def data_uploaded():
            bucket_content = BucketView(self.redpanda, topics=self.topics)
            has_segments = bucket_content.segment_objects > 0

            if not has_segments:
                self.logger.info("No segments yet")
                return False

            has_segments_in_manifest = any(
                len(m.get("segments", [])) > 0
                for m in bucket_content.partition_manifests.values()
            )
            if not has_segments_in_manifest:
                self.logger.info("No segments in any manifests yet")

            if bucket_content.ignored_objects > 0:
                # Our topic filter should have matched everything in the bucket.
                self.logger.info(f"Ignored {bucket_content.ignored_objects} objects")

            return (
                has_segments
                and has_segments_in_manifest
                and not bucket_content.ignored_objects
            )

        self.redpanda.wait_until(
            data_uploaded,
            timeout_sec=90,
            backoff_sec=5,
            err_msg="Data not uploaded after firewall unblocked",
        )

    @cluster(num_nodes=3, log_allow_list=CONNECTION_ERROR_LOGS)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_reconnect(self, cloud_storage_type):
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
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_one_node_reconnect(self, cloud_storage_type):
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
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_connection_drop(self, cloud_storage_type):
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
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_connection_flicker(self, cloud_storage_type):
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

    @cluster(num_nodes=3, log_allow_list=CONNECTION_ERROR_LOGS)
    @matrix(acks=[1, -1], cloud_storage_type=get_cloud_storage_type())
    def test_retention_archival_coordination(self, acks, cloud_storage_type):
        """
        Test that only archived segments can be evicted and that eviction
        restarts once the segments have been archived.
        """
        local_retention = 5 * self.log_segment_size
        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES: local_retention,
            },
        )

        with firewall_blocked(self.redpanda.nodes, self._s3_port):
            produce_until_segments(
                redpanda=self.redpanda,
                topic=self.topic,
                partition_idx=0,
                count=10,
                acks=acks,
            )

            # Sleep some time sufficient for log eviction under normal conditions
            # and check that no segment has been evicted (because we can't upload
            # segments to the cloud storage).
            time.sleep(3 * self.log_compaction_interval_ms / 1000.0)
            counts = list(segments_count(self.redpanda, self.topic, partition_idx=0))
            self.logger.info(f"node segment counts: {counts}")
            assert len(counts) == len(self.redpanda.nodes)
            assert all(c >= 10 for c in counts)

        # Check that eviction restarts after we restored the connection to cloud
        # storage.
        wait_for_local_storage_truncate(
            redpanda=self.redpanda, topic=self.topic, target_bytes=local_retention
        )

    def _get_partition_leaders(self):
        kcat = KafkaCat(self.redpanda)
        m = kcat.metadata()
        self.logger.info(f"kcat.metadata() == {m}")
        brokers = {}
        for b in m["brokers"]:
            id = b["id"]
            ip = b["name"]
            ip = ip[: ip.index(":")]
            for n in self.redpanda.nodes:
                n_ip = n.account.hostname
                self.logger.debug(f"matching {n_ip} over {ip}")
                if n_ip == ip:
                    brokers[id] = n
                    break
        self.logger.debug(f"found brokers {brokers}")
        assert len(brokers) == 3
        leaders = {}
        for topic in m["topics"]:
            if topic["topic"] == ArchivalTest.s3_topic_name:
                for part in topic["partitions"]:
                    leader_id = part["leader"]
                    partition_id = part["partition"]
                    leader = brokers[leader_id]
                    leaders[partition_id] = leader
        return leaders

    def _verify_manifest(self, ntp, manifest, remote):
        """Check that all segments that present in manifest are available
        in remote storage"""
        for key, meta in manifest.get("segments", {}).items():
            segment_name = gen_segment_name_from_meta(meta, key=key)
            spath = (
                f"{ntp.ns}/{ntp.topic}/{ntp.partition}_{ntp.revision}/{segment_name}"
            )
            self.logger.info(f"validating manifest path {spath}")
            assert spath in remote
        ranges = [
            (int(m["base_offset"]), int(m["committed_offset"]))
            for m in manifest.get("segments", {}).values()
        ]
        ranges = sorted(ranges, key=lambda x: x[0])
        last_offset = -1
        num_gaps = 0
        for base, committed in ranges:
            if last_offset + 1 != base:
                self.logger.debug(f"gap between {last_offset} and {base} detected")
                num_gaps += 1
            last_offset = committed
        assert num_gaps == 0

    def _quick_verify(self):
        """Verification algorithm that works only if no leadership
        transfer happened during the run. It works by looking up all
        segments from the remote storage in local redpanda storages.
        It's done by using md5 hashes of the nodes.
        """
        local = defaultdict(set)
        for node in self.redpanda.nodes:
            checksums = self._get_redpanda_log_segment_checksums(node)
            self.logger.info(f"Node: {node.account.hostname} checksums: {checksums}")
            for k, v in checksums.items():
                local[k].add(v)
        remote = self._get_redpanda_s3_checksums()
        self.logger.info(f"S3 checksums: {remote}")
        self.logger.info(f"Local checksums: {local}")
        assert len(local) != 0
        assert len(remote) != 0
        md5fails = 0
        lookup_fails = 0
        for path, csum in remote.items():
            # Skip index files, these are only present on cloud storage
            if path.endswith(".index"):
                continue

            adjusted = gen_local_path_from_remote(path)
            self.logger.info(
                f"checking remote path: {path} csum: {csum} adjusted: {adjusted}"
            )
            if adjusted not in local:
                self.logger.debug(
                    f"remote path {adjusted} can't be found in any of the local storages"
                )
                lookup_fails += 1
            else:
                if len(local[adjusted]) != 1:
                    self.logger.info(
                        f"remote segment {path} have more than one variant {local[adjusted]}"
                    )
                if csum not in local[adjusted]:
                    self.logger.debug(
                        f"remote md5 {csum} doesn't match any local {local[adjusted]}"
                    )
                    md5fails += 1

            index_expr = rf"{path}\.\d+\.index"
            assert any(re.match(index_expr, entry) for entry in remote), (
                f"expected {index_expr} to be present "
                f"for log segment {path} but missing"
            )

        if md5fails != 0:
            self.logger.debug(
                f"Validation failed, {md5fails} remote segments doesn't match"
            )
        if lookup_fails != 0:
            self.logger.debug(
                f"Validation failed, remote {lookup_fails} remote locations doesn't match local"
            )
        assert md5fails == 0 and lookup_fails == 0

        # Validate partitions
        # for every partition the segment with the largest base offset shouldn't be
        # available in remote storage
        local_partitions: DefaultDict[NTPR, list] = defaultdict(list)
        remote_partitions: DefaultDict[NTPR, list] = defaultdict(list)
        for path, items in local.items():
            meta = _parse_normalized_segment_path(path, "", 0)
            local_partitions[meta.ntp].append((meta, items))
        for path, items in remote.items():
            meta = _parse_normalized_segment_path(path, "", 0)
            remote_partitions[meta.ntp].append((meta, items))
        self.logger.info(f"generated local partitions {local_partitions.keys()}")
        self.logger.info(f"generated remote partitions {remote_partitions.keys()}")

        # Download manifest for partitions
        manifests = {}
        for ntp in local_partitions.keys():
            manifest = BucketView(self.redpanda).get_partition_manifest(ntp)
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
            return not (
                path.startswith(CONTROLLER_LOG_PREFIX)
                or path.startswith(INTERNAL_TOPIC_PREFIX)
            ) and path.endswith(LOG_EXTENSION)

        # Remove data dir from path
        def normalize_path(path):
            return os.path.relpath(path, RedpandaService.DATA_DIR)

        return {
            normalize_path(path): value
            for path, value in checksums.items()
            if included(path)
        }

    def _get_redpanda_s3_checksums(self):
        """Get MD5 checksums of log segments stored in S3 (minio). The paths are
        normalized (<namespace>/<topic>/<partition>_<rev>/...)."""

        def normalize(path):
            # strip archiver cluster UUID prefix from the segment path
            path = path[37:]
            match = LOG_EXPRESSION.match(path)
            if match:
                return match[1]
            return path

        def included(path):
            return not (
                path.endswith(MANIFEST_EXTENSION)
                or path.endswith(MANIFEST_BIN_EXTENSION)
            )

        objects = list(self.cloud_storage_client.list_objects(self.s3_bucket_name))
        self.logger.info(
            f"got {len(objects)} objects from bucket {self.s3_bucket_name}"
        )
        for o in objects:
            self.logger.info(f"object: {o}")

        return {
            normalize(it.key): (it.etag, it.content_length)
            for it in objects
            if included(it.key)
        }

    def _archiver_restart_msg_seen(self, reason: Optional[str] = None) -> bool:
        return self.redpanda.search_log_any(f".*updating archiver for {reason or ''}.*")

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_compaction_cluster_config_change(self, cloud_storage_type):
        # Note: the ducktape setup currently automatically trims trace level logs
        # from the output, so while debugging, you need to use
        # `RP_TRIM_LOGS="false"` to see the logs.
        self.admin.set_log_level(name="cluster", level="trace")

        # Verify assumptions
        assert self.topics[0].cleanup_policy is None, (
            "The compaction setting is assumed to be `delete` by default"
        )
        assert not self._archiver_restart_msg_seen(), (
            "There should be no archival restart message initially"
        )

        self.redpanda.logger.debug(
            "Change the compaction policy to trigger an archiver restart"
        )
        self.redpanda.set_cluster_config({"log_cleanup_policy": "delete,compact"})
        wait_until(
            lambda: self._archiver_restart_msg_seen(
                "cluster config change in log_cleanup_policy"
            ),
            timeout_sec=60,
            err_msg="archiver not restarted in time",
        )

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_compaction_topic_config_change(self, cloud_storage_type):
        # Note: the ducktape setup currently automatically trims trace level logs
        # from the output, so while debugging, you need to use
        # `RP_TRIM_LOGS="false"` to see the logs.
        self.admin.set_log_level(name="cluster", level="trace")

        # Verify assumptions
        assert self.topics[0].cleanup_policy is None, (
            "The compaction setting is assumed to be `delete` by default"
        )
        assert not self._archiver_restart_msg_seen(), (
            "There should be no archival restart message initially"
        )

        self.redpanda.logger.debug(
            "Change the topic config without changing the 'compactedness' and expect no archiver restart"
        )
        self.kafka_tools.alter_topic_config(
            self.topic,
            {TopicSpec.PROPERTY_CLEANUP_POLICY: TopicSpec.CLEANUP_DELETE},
        )
        time.sleep(10)
        assert not self._archiver_restart_msg_seen(), (
            "Unexpected archival restart when compacted config not changed"
        )

        self.redpanda.logger.debug(
            "Change the topic config 'compactedness' and expect an archiver restart"
        )
        self.kafka_tools.alter_topic_config(
            self.topic,
            {TopicSpec.PROPERTY_CLEANUP_POLICY: TopicSpec.CLEANUP_COMPACT},
        )
        wait_until(
            lambda: self._archiver_restart_msg_seen(
                "topic config change in compaction"
            ),
            timeout_sec=60,
            err_msg="archiver not restarted in time",
        )
