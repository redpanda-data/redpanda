# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import collections
import json
import io
import pprint
import struct
import time
from dataclasses import dataclass
from collections import defaultdict, namedtuple
from enum import Enum
from typing import Literal, Sequence, Optional, NewType, NamedTuple, Iterator

from rptest.clients.offline_log_viewer import OfflineLogViewer
import xxhash

from botocore.exceptions import ClientError
from rptest.archival.s3_client import ObjectMetadata, S3Client
from rptest.archival.abs_client import ABSClient
from rptest.clients.rp_storage_tool import RpStorageTool
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import MetricsEndpoint, RESTART_LOG_ALLOW_LIST

EMPTY_SEGMENT_SIZE = 4096

BLOCK_SIZE = 4096

default_log_segment_size = 1048576  # 1MB

DEFAULT_OFFSET = -9223372036854775808


class NT(NamedTuple):
    ns: str
    topic: str


class NTP(NamedTuple):
    ns: str
    topic: str
    partition: int

    def to_ntpr(self, revision: int) -> 'NTPR':
        return NTPR(self.ns, self.topic, self.partition, revision)

    def to_nt(self):
        return NT(self.ns, self.topic)


class NTPR(NamedTuple):
    ns: str
    topic: str
    partition: int
    revision: int

    def to_ntp(self) -> NTP:
        return NTP(self.ns, self.topic, self.partition)


@dataclass
class LogRegionSize:
    total: int = 0
    accessible: int = 0

    def __iadd__(self, other):
        self.total += other.total
        self.accessible += other.accessible
        return self


@dataclass
class CloudLogSize:
    stm: LogRegionSize
    archive: LogRegionSize

    @staticmethod
    def make_empty():
        return CloudLogSize(stm=LogRegionSize(), archive=LogRegionSize())

    def __iadd__(self, other):
        self.stm += other.stm
        self.archive += other.archive
        return self

    def accessible(self, no_archive: bool = False) -> int:
        if no_archive and self.archive.accessible > 0:
            raise RuntimeError(
                f"CloudLogSize requested to ignore archive with accessible contents: archive={self.archive}"
            )

        if no_archive:
            return self.stm.accessible
        else:
            return self.stm.accessible + self.archive.accessible

    def total(self, no_archive: bool = False) -> int:
        if no_archive and self.archive.total > 0:
            raise RuntimeError(
                f"CloudLogSize requested to ignore archive with contents: archive={self.archive}"
            )

        if no_archive:
            return self.stm.total
        else:
            return self.stm.total + self.archive.total


class ControllerSnapshotComponents(NamedTuple):
    """
    Keys that uniquely identify a controller snapshot.
    """
    cluster_uuid: str
    offset: int


class ClusterMetadataComponents(NamedTuple):
    """
    Keys that uniquely identify a cluster metadata manifest.
    """
    cluster_uuid: str
    metadata_id: int


class ClusterMetadata(NamedTuple):
    """
    Metadata associated with a single cluster.
    """
    # metadata_id => deserialized manifest
    cluster_metadata_manifests: dict[int, dict]

    # offset => snapshot size
    controller_snapshot_sizes: dict[int, int]


TopicManifestMetadata = namedtuple('TopicManifestMetadata',
                                   ['ntp', 'revision'])
SegmentMetadata = namedtuple(
    'SegmentMetadata',
    ['ntp', 'revision', 'base_offset', 'term', 'md5', 'size'])

SegmentPathComponents = namedtuple('SegmentPathComponents',
                                   ['ntpr', 'name', 'base_offset'])

MISSING_DATA_ERRORS = [
    "No segments found. Empty partition manifest generated",
    "Error during log recovery: cloud_storage::missing_partition_exception",
    "Failed segment download",
]

TRANSIENT_ERRORS = RESTART_LOG_ALLOW_LIST + [
    "raft::offset_monitor::wait_timed_out",
    "Upload loop error: seastar::timed_out_error"
]


class SegmentSummary(NamedTuple):
    ns: str
    topic: str
    partition: int
    revision: int
    epoch: int
    base_offset: int
    last_offset: int
    base_timestamp: int
    last_timestamp: int
    num_data_batches: int
    num_conf_batches: int
    num_data_records: int
    num_conf_records: int
    size_bytes: int


# NB: SegmentReader is duplicated compute_storage.py for deployment reasons. If
# making changes please adapt both.
class SegmentReader:
    HDR_FMT_RP = "<IiqbIhiqqqhii"
    HEADER_SIZE = struct.calcsize(HDR_FMT_RP)
    Header = collections.namedtuple(
        'Header', ('header_crc', 'batch_size', 'base_offset', 'type', 'crc',
                   'attrs', 'delta', 'first_ts', 'max_ts', 'producer_id',
                   'producer_epoch', 'base_seq', 'record_count'))

    def __init__(self, stream):
        self.stream = stream

    def read_batch(self):
        data = self.stream.read(self.HEADER_SIZE)
        if len(data) == self.HEADER_SIZE:
            header = self.Header(*struct.unpack(self.HDR_FMT_RP, data))
            if all(map(lambda v: v == 0, header)):
                return None
            records_size = header.batch_size - self.HEADER_SIZE
            data = self.stream.read(records_size)
            if len(data) < records_size:
                return None
            assert len(
                data
            ) == records_size, f"data len is {len(data)} but the expected records size is {records_size}"
            return header
        return None

    def __iter__(self) -> Iterator[Header]:
        while True:
            it = self.read_batch()
            if it is None:
                return
            yield it


def make_segment_summary(ntpr: NTPR, reader: SegmentReader) -> SegmentSummary:
    """Read/parse segment and produce the summary"""
    epoch: int = 0
    max_int = int(2**63 - 1)
    min_int = -1 * (max_int - 1)
    base_offset = max_int
    last_offset = min_int
    base_timestamp = max_int
    last_timestamp = min_int
    num_data_batches = 0
    num_conf_batches = 0
    num_data_records = 0
    num_conf_records = 0
    size_bytes = 0
    for header in reader:
        base_offset = min(base_offset, header.base_offset)
        last_offset = max(last_offset,
                          header.base_offset + header.record_count - 1)
        base_timestamp = min(base_timestamp, header.first_ts)
        last_timestamp = max(last_timestamp, header.max_ts)
        epoch = header.producer_epoch
        filters = [
            19,  # archival
            2,  # raft_configuration
            23,  # version_fence
            25  # prefix_truncate
        ]

        if header.type in filters:
            num_conf_batches += 1
            num_conf_records += header.record_count
        else:
            num_data_batches += 1
            num_data_records += header.record_count
        size_bytes += header.batch_size
    return SegmentSummary(ns=ntpr.ns,
                          topic=ntpr.topic,
                          revision=ntpr.revision,
                          partition=ntpr.partition,
                          epoch=epoch,
                          base_offset=base_offset,
                          last_offset=last_offset,
                          base_timestamp=base_timestamp,
                          last_timestamp=last_timestamp,
                          num_data_batches=num_data_batches,
                          num_conf_batches=num_conf_batches,
                          num_data_records=num_data_records,
                          num_conf_records=num_conf_records,
                          size_bytes=size_bytes)


def parse_s3_topic_label(path: str) -> str:
    """
    Parse S3 manifest path. Return the label, or an empty string if not
    labeled with the cluster uuid.

    Sample name: 50000000/meta/kafka/panda-topic/topic_manifest.json
        Output: ""
    Sample name: meta/kafka/panda-topic/6e94ccdc-443a-4807-b105-0bb86e8f97f7/0/topic_manifest.json
        Output: "6e94ccdc-443a-4807-b105-0bb86e8f97f7"
    """
    items = path.split('/')
    if len(items[0]) == 8 and items[0].endswith('0000000'):
        return ""
    return items[3]


def parse_s3_partition_path_label(path: str) -> str:
    """
    Parse S3 manifest path. Return the label, or an empty string if not
    labeled with the cluster uuid.

    Sample name: 50000000/meta/kafka/panda-topic/0_19/manifest.json
        Output: ""
    Sample name: 6e94ccdc-443a-4807-b105-0bb86e8f97f7/meta/kafka/panda-topic/0_18/manifest.bin
        Output: "6e94ccdc-443a-4807-b105-0bb86e8f97f7"
    Sample name: 6e94ccdc-443a-4807-b105-0bb86e8f97f7/meta/kafka/panda-topic/0_18/manifest.bin.0.21.0.20.1719867209267.1719867209268
        Output: "6e94ccdc-443a-4807-b105-0bb86e8f97f7"
    """
    items = path.split('/')
    if len(items[0]) == 8 and items[0].endswith('0000000'):
        return ""
    return items[0]


def parse_s3_manifest_path(path: str) -> NTPR:
    """Parse S3 manifest path. Return ntp and revision.
    Sample name: 50000000/meta/kafka/panda-topic/0_19/manifest.json
    Sample name: 6e94ccdc-443a-4807-b105-0bb86e8f97f7/meta/kafka/panda-topic/0_18/manifest.bin
    """
    items = path.split('/')
    ns = items[2]
    topic = items[3]
    part_rev = items[4].split('_')
    partition = int(part_rev[0])
    revision = int(part_rev[1])
    return NTPR(ns=ns, topic=topic, partition=partition, revision=revision)


def parse_cluster_metadata_manifest_path(
        path: str) -> ClusterMetadataComponents:
    """
    Parse S3 cluster metadata manifest path. Return the cluster UUID and
    metadata ID.
    Sample name: cluster_metadata/6e94ccdc-443a-4807-b105-0bb86e8f97f7/manifests/0/cluster_manifest.json
    """
    items = path.split('/')
    assert items[
        0] == "cluster_metadata", f"Invalid cluster metadata manifest path: {path}"
    cluster_uuid = items[1]
    meta_id = int(items[3])
    return ClusterMetadataComponents(cluster_uuid, meta_id)


def parse_controller_snapshot_path(path: str) -> ClusterMetadataComponents:
    """
    Parse S3 cluster controller snapshot path. Return the cluster UUID and
    metadata ID.
    Sample name: cluster_metadata/6e94ccdc-443a-4807-b105-0bb86e8f97f7/0/controller.snapshot
    """
    items = path.split('/')
    assert items[
        0] == "cluster_metadata", f"Invalid cluster metadata manifest path: {path}"
    cluster_uuid = items[1]
    meta_id = int(items[2])
    return ClusterMetadataComponents(cluster_uuid, meta_id)


def parse_s3_segment_path(path) -> SegmentPathComponents:
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
    base_offset = int(fname.split('-')[0])
    ntpr = NTPR(ns=ns, topic=topic, partition=partition, revision=revision)
    return SegmentPathComponents(ntpr=ntpr,
                                 name=fname,
                                 base_offset=base_offset)


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


def verify_file_layout(baseline_per_host,
                       restored_per_host,
                       expected_topics,
                       logger,
                       size_overrides=None):
    """This function checks the restored segments over the expected ones.
    It takes into account the fact that the md5 checksum as well as the
    file name of the restored segment might be different from the original
    segment. This is because we're deleting raft configuration batches
    from the segments.
    The function checks the size of the parition over the size of the original.
    The assertion is triggered only if the difference can't be explained by the
    upload lag and removal of configuration/archival-metadata batches.
    """

    if size_overrides is None:
        size_overrides = {}

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
                    if it.size > EMPTY_SEGMENT_SIZE:
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
                    assert total_size == ntps[ntp],\
                          f"{ntp=} new {total_size=} differs from already accounted size={ntps[ntp]}"
                else:
                    ntps[ntp] = max(total_size, ntps[ntp])
        return ntps

    restored_ntps = get_ntp_sizes(restored_per_host, hosts_can_vary=False)
    baseline_ntps = get_ntp_sizes(baseline_per_host, hosts_can_vary=True)

    logger.info(f"before matching\n"
                f"restored ntps: {restored_ntps}\n"
                f"baseline ntps: {baseline_ntps}\n"
                f"expected topics: {expected_topics}")

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
        fraction = float(max(rest_ntp_size, orig_ntp_size)) / float(
            min(rest_ntp_size, orig_ntp_size))
        assert fraction < 1.1, \
            f"NTP {ntp} the size of the restored partition is too different {rest_ntp_size} compared to the original one {orig_ntp_size}."

        delta = orig_ntp_size - rest_ntp_size
        assert delta <= BLOCK_SIZE, \
            f"NTP {ntp} the restored partition is too small {rest_ntp_size}." \
            f" The original is {orig_ntp_size} bytes which {delta} bytes larger."


def gen_topic_manifest_path(topic: NT,
                            manifest_format: Literal['json', 'bin'] = 'bin',
                            remote_label: str = "",
                            rev: int = 0):
    assert manifest_format in ['json', 'bin']
    path = f"{topic.ns}/{topic.topic}"
    if len(remote_label) == 0:
        x = xxhash.xxh32()
        x.update(path.encode('ascii'))
        hash = x.hexdigest()[0] + '0000000'
        return f"{hash}/meta/{path}/topic_manifest.{manifest_format}"
    return f"meta/{path}/{remote_label}/{rev}/topic_manifest.{manifest_format}"


def gen_topic_lifecycle_marker_path(topic: NT,
                                    rev: int,
                                    remote_label: str = ""):
    path = f"{topic.ns}/{topic.topic}"
    if len(remote_label) == 0:
        x = xxhash.xxh32()
        x.update(path.encode('ascii'))
        hash = x.hexdigest()[0] + '0000000'
        return f"{hash}/meta/{path}/{rev}_lifecycle.bin"
    return f"meta/{path}/{remote_label}/{rev}_lifecycle.bin"


def gen_segment_name_from_meta(meta: dict, key: str) -> str:
    """
    Generates segment name using the sname_format. If v2 is supplied,
    new segment name format is generated. If v1 is supplied, the key from
    manifest is used.
    :param meta: the segment meta object from manifest
    :param key: the segment key which is also the old style path for v1
    :return: adjusted path
    """
    version = meta.get('sname_format', 1)
    if version > 1:
        head = '-'.join([
            str(meta[k]) for k in ('base_offset', 'committed_offset',
                                   'size_bytes', 'segment_term')
        ])
        return f'{head}-v1.log'
    else:
        return key


def gen_local_path_from_remote(remote_path: str) -> str:
    head, tail = remote_path.rsplit('/', 1)
    tokens = tail.split('-')
    # extract base offset and term from new style path
    adjusted = f'{tokens[0]}-{tokens[-2]}-{tokens[-1]}'
    return f'{head}/{adjusted}'


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


NodeReport = NewType('NodeReport', dict[str, tuple[str, int]])
NodeSegmentsReport = NewType('NodeSegmentsReport', dict[str, NodeReport])


def get_expected_ntp_restored_size(nodes_segments_report: NodeSegmentsReport,
                                   retention_policy: int):
    """ Get expected retestored ntp disk size
    We expect that redpanda will restore max
    amount of segments with total size less
    than retention_policy
    """
    size_bytes_per_ntp = {}
    segments_sizes_per_ntp = {}

    assert len(nodes_segments_report) > 0

    expected_restored_sizes = None
    for _node, report in nodes_segments_report.items():
        tmp_partition_size = defaultdict(int)
        tmp_segments_sizes = defaultdict(dict)
        for path, summary in report.items():
            segment = _parse_checksum_entry(path, summary, True)
            ntp = segment.ntp
            size = summary[1]
            tmp_partition_size[ntp] += size
            tmp_segments_sizes[ntp][segment.base_offset] = size
        for ntp, size in tmp_partition_size.items():
            if not ntp in size_bytes_per_ntp or size_bytes_per_ntp[ntp] < size:
                size_bytes_per_ntp[ntp] = size
                segments_sizes_per_ntp[ntp] = tmp_segments_sizes[ntp]
        expected_restored_sizes = {}
        for ntp, segments in segments_sizes_per_ntp.items():
            expected_restored_sizes[ntp] = 0
            for segment in sorted(segments.keys(), reverse=True):
                if expected_restored_sizes[ntp] + segments_sizes_per_ntp[ntp][
                        segment] > retention_policy:
                    break
                expected_restored_sizes[ntp] += segments_sizes_per_ntp[ntp][
                    segment]

    assert expected_restored_sizes is not None

    return expected_restored_sizes


def nodes_report_cloud_segments(redpanda, target_segments, topic_name=None):
    """
    Returns true if the nodes in the cluster collectively report having
    above the given number of segments for a specific topic, if provided.

    Args:
        redpanda: The Redpanda instance.
        target_segments (int): The target number of segments to check for.
        topic_name (str, optional): The name of the topic for which to count the segments.
    
    NOTE: we're explicitly not checking the manifest via cloud client
    because we expect the number of items in our bucket to be quite large,
    and for associated ListObjects calls to take a long time.
    """
    try:
        metrics_endpoint = MetricsEndpoint.PUBLIC_METRICS
        if topic_name:
            num_segments = redpanda.metric_sum(
                "redpanda_cloud_storage_segments",
                metrics_endpoint=metrics_endpoint,
                topic=topic_name)
        else:
            # Fetch total number of segments without filtering by specific topic
            num_segments = redpanda.metric_sum(
                "redpanda_cloud_storage_segments",
                metrics_endpoint=metrics_endpoint)

        message = (f"Cluster metrics for topic '{topic_name}' report "
                   f"{num_segments} / {target_segments} cloud segments") \
            if topic_name else \
            f"Cluster metrics report {num_segments} / {target_segments} cloud segments"

        redpanda.logger.info(message)
    except Exception as e:
        redpanda.logger.error(f"Error fetching metrics: {e}")
        return False

    return num_segments >= target_segments


def is_close_size(actual_size, expected_size):
    """Checks if the log size is close to expected size.
    The actual size shouldn't be less than expected. Also, the difference
    between two values shouldn't be greater than the size of one segment.
    """
    lower_bound = int(expected_size * 0.95)
    upper_bound = expected_size + default_log_segment_size + \
                  int(default_log_segment_size * 0.2)
    return actual_size in range(lower_bound, upper_bound)


class PathMatcher:
    def __init__(self, expected_topics: Optional[Sequence[TopicSpec]] = None):
        self.expected_topics = expected_topics
        if self.expected_topics is not None:
            self.topic_names = {t.name for t in self.expected_topics}
            # topic_manifest can end in .json for redpanda before v24.1, and .bin for redpanda after v24.1.
            self.topic_manifest_paths = {
                manifest_key
                for t in self.topic_names
                for manifest_key in (f'/{t}/topic_manifest.json',
                                     f'/{t}/topic_manifest.bin')
            }
        else:
            self.topic_names = None
            self.topic_manifest_paths = None

    def _match_partition_manifest(self, key):
        if self.topic_names is None:
            return True
        else:
            return any(tn in key for tn in self.topic_names)

    def _match_topic_manifest(self, key):
        if self.topic_names is None:
            return True
        else:
            for t in self.topic_names:
                if not key.endswith(
                        "/topic_manifest.bin") and not key.endswith(
                            "/topic_manifest.json"):
                    continue
                if t in key:
                    return True
            return False

    def is_cluster_metadata_manifest(self, o: ObjectMetadata) -> bool:
        return o.key.endswith('/cluster_manifest.json')

    def is_controller_snapshot(self, o: ObjectMetadata) -> bool:
        return o.key.endswith('/controller.snapshot')

    def is_partition_manifest(self, o: ObjectMetadata) -> bool:
        return (o.key.endswith('/manifest.json')
                or o.key.endswith("/manifest.bin")
                ) and self._match_partition_manifest(o.key)

    def is_spillover_manifest(self, o: ObjectMetadata) -> bool:
        return "/manifest.bin." in o.key and self._match_partition_manifest(
            o.key)

    def is_topic_manifest(self, o: ObjectMetadata) -> bool:
        return self._match_topic_manifest(o.key)

    def is_segment_index(self, o: ObjectMetadata) -> bool:
        if not o.key.endswith(".index"):
            return False
        try:
            parsed = parse_s3_segment_path(o.key[0:-6])
        except Exception:
            return False
        else:
            if self.topic_names is not None:
                return parsed.ntpr.topic in self.topic_names
            else:
                return True

    def is_tx_manifest(self, o: ObjectMetadata) -> bool:
        if not o.key.endswith(".tx"):
            return False
        try:
            parsed = parse_s3_segment_path(o.key[0:-3])
        except Exception:
            return False
        else:
            if self.topic_names is not None:
                return parsed.ntpr.topic in self.topic_names
            else:
                return True

    def is_segment(self, o: ObjectMetadata) -> bool:
        try:
            if o.key.endswith(".index"):
                return False
            if o.key.endswith(".tx"):
                return False
            parsed = parse_s3_segment_path(o.key)
        except Exception:
            return False
        else:
            if self.topic_names is not None:
                return parsed.ntpr.topic in self.topic_names
            else:
                return True

    def path_matches_any_topic(self, path: str) -> bool:
        return self._match_partition_manifest(path)


def quiesce_uploads(redpanda,
                    topic_names: list[str],
                    timeout_sec,
                    target_label: Optional[str] = None):
    """
    Wait until all local data for all topics in `topic_names` has been uploaded
    to remote storage.  This function expects that no new data is being produced:
    if new data is being produced, this function will only guarantee that remote
    HWM has caught up with local HWM at the time we entered the function.

    **Important**: you must have interval uploads enabled, or this function
    will fail: it expects all data to be uploaded eventually.
    """

    last_msg = ""

    def remote_has_reached_hwm(ntp: NTP, hwm: int):
        nonlocal last_msg
        view = BucketView(redpanda)
        try:
            manifest = view.get_partition_manifest(ntp, target_label)
        except Exception as e:
            last_msg = f"Partition {ntp} doesn't have a manifest yet ({e})"
            redpanda.logger.debug(last_msg)
            return False

        remote_committed_offset = BucketView.kafka_last_offset(manifest)
        if remote_committed_offset is None:
            last_msg = f"Partition {ntp} does not have committed offset yet"
            redpanda.logger.debug(last_msg)
            return False
        else:
            ready = remote_committed_offset >= hwm - 1
            if not ready:
                last_msg = f"Partition {ntp} not yet ready ({remote_committed_offset} < {hwm-1})"
                redpanda.logger.debug(last_msg)
            return ready

    t_initial = time.time()

    rpk = RpkTool(redpanda)
    for topic_name in topic_names:
        described = rpk.describe_topic(topic_name)
        p_count = 0
        for p in described:
            p_count += 1

            ntp = NTP(ns='kafka', topic=topic_name, partition=p.id)
            hwm = p.high_watermark

            # After timeout_sec has elapsed, expect each partition to
            # be ready immediately.
            timeout = max(1, timeout_sec - (time.time() - t_initial))

            redpanda.wait_until(lambda: remote_has_reached_hwm(ntp, hwm),
                                timeout_sec=timeout,
                                backoff_sec=1,
                                err_msg=lambda: last_msg)
            redpanda.logger.debug(f"Partition {ntp} ready (reached HWM {hwm})")

        if p_count == 0:
            # We expect to be called on a topic where `rpk topic describe` returns
            # some data, otherwise we can't check that against cloud storage content
            raise RuntimeError(f"Found 0 partitions for topic '{topic_name}'")


@dataclass(order=True, frozen=True)
class SpillMeta:
    base: int
    last: int
    base_kafka: int
    last_kafka: int
    base_ts: int
    last_ts: int
    ntpr: NTPR
    path: str

    @staticmethod
    def make(ntpr: NTPR, path: str):
        base, last, base_kafka, last_kafka, base_ts, last_ts = SpillMeta._parse_path(
            ntpr, path)
        return SpillMeta(base=int(base),
                         last=int(last),
                         base_kafka=int(base_kafka),
                         last_kafka=int(last_kafka),
                         base_ts=int(base_ts),
                         last_ts=int(last_ts),
                         ntpr=ntpr,
                         path=path)

    @staticmethod
    def _parse_path(ntpr: NTPR, path: str) -> list[str]:
        """
        Extract metadata from spillover manifest path.
        Expected format is:
        {base}.{base_rp_offset}.{last_rp_offest}.{base_kafka_offset}.{last_kafka_offset}.{first_ts}.{last_ts}
        where base = {hash}/meta/{ntpr.ns}/{ntpr.topic}/{ntpr.partition}_{ntpr.revision}/manifest"
        """
        label = parse_s3_partition_path_label(path)
        base = BucketView.gen_manifest_path(ntpr, remote_label=label)
        suffix = path.removeprefix(f"{base}.")

        split = suffix.split(".")
        if len(split) != 6:
            raise RuntimeError(
                f"Invalid spillover manifest {path=} for {ntpr=}")

        return split


@dataclass
class BucketViewState:
    """
    Results of a full listing of the bucket, if listed is True, or
    partial results if listed is False
    """
    def __init__(self):
        self.listed: bool = False
        self.segment_objects: int = 0
        self.ignored_objects: int = 0
        self.tx_manifests: int = 0
        self.segment_indexes: int = 0
        self.topic_manifests: dict[str, dict[NT, dict]] = {}
        self.partition_manifests: dict[str, dict[NTP, dict]] = {}
        self.spillover_manifests: dict[str, dict[NTP, dict[SpillMeta,
                                                           dict]]] = {}
        # List of summaries for all segments. These summaries refer
        # to data in the bucket and not segments in the manifests.
        self.segment_summaries: dict[str, dict[NTP, list[SegmentSummary]]] = {}
        self.cluster_metadata: dict[str, ClusterMetadata] = {}


class ManifestFormat(Enum):
    JSON = 'json'
    BINARY = 'binary'


class LifecycleMarkerStatus(Enum):
    LIVE = 1
    PURGING = 2
    PURGED = 3


class BucketView:
    """
    A caching view onto an object storage bucket, that knows how to construct paths
    for manifests, and can also scan the bucket for aggregate segment info.

    For directly fetching a manifest or manifest-derived info for a partition,
    this will avoid doing object listings.  Access to properties that require
    object listings (like the object counts) will list the bucket and then cache
    the result.
    """
    def __init__(self,
                 redpanda,
                 topics: Optional[Sequence[TopicSpec]] = None,
                 scan_segments: bool = False):
        """
        Always construct this with a `redpanda` -- the explicit logger/bucket/client
        arguments are only here to enable the structure of topic_recovery_test.py to work,
        and an instance constructed that way will not work for all methods.

        :param redpanda: a RedpandaService, whose SISettings we will use
        :param topics: optional list of topics to filter result to
        :param scan_segments: optional flag that indicates that segments has to be parsed
        """
        self.redpanda = redpanda
        self.logger = redpanda.logger
        self.bucket = redpanda.si_settings.cloud_storage_bucket
        self.client: S3Client | ABSClient = redpanda.cloud_storage_client

        self.path_matcher = PathMatcher(topics)

        # Cache built on demand by loading revision ID from topic manifest
        self._ntp_to_revision = {}

        self._state = BucketViewState()
        self._scan_segments = scan_segments

    def reset(self):
        """
        Drop all cached state, so that subsequent calls will use fresh data
        """
        self._state = BucketViewState()

    def ntp_to_ntpr(self, ntp: NTP) -> NTPR:
        """
        Raises KeyError if the NTP is not found
        """
        try:
            revision = self._ntp_to_revision[ntp]
        except KeyError:
            # Load topic manifest to resolve revision
            topic_manifest = self.get_topic_manifest(ntp.to_nt())
            revision = topic_manifest['revision_id']
            self._ntp_to_revision[ntp] = revision

        return ntp.to_ntpr(revision)

    @property
    def segment_objects(self) -> int:
        self._ensure_listing()
        return self._state.segment_objects

    @property
    def ignored_objects(self) -> int:
        self._ensure_listing()
        return self._state.ignored_objects

    @property
    def cluster_metadata(self) -> dict[str, ClusterMetadata]:
        self._ensure_listing()
        return self._state.cluster_metadata

    @property
    def latest_cluster_metadata_manifest(self) -> dict:
        self._ensure_listing()
        latest_cluster_metadata = ClusterMetadata(dict(), dict())
        highest_meta_id = -1
        for _, meta in self.cluster_metadata.items():
            for meta_id, _ in meta.cluster_metadata_manifests.items():
                if meta_id > highest_meta_id:
                    latest_cluster_metadata = meta
                    highest_meta_id = meta_id
        if highest_meta_id == -1:
            return dict()

        highest_manifest = latest_cluster_metadata.cluster_metadata_manifests[
            highest_meta_id]
        return highest_manifest

    @property
    def partition_manifests(self) -> dict[NTP, dict]:
        self._ensure_listing()
        if len(self._state.partition_manifests) != 1:
            raise Exception(
                f"Bucket doesn't have exactly one cluster's data: {self._state.partition_manifests.keys()}"
            )
        return next(iter(self._state.partition_manifests.values()))

    @staticmethod
    def kafka_start_offset(manifest) -> Optional[int]:
        if "archive_start_offset" in manifest:
            return manifest["archive_start_offset"] - manifest[
                "archive_start_offset_delta"]

        if 'segments' not in manifest or len(manifest['segments']) == 0:
            return None

        start_model_offset = manifest['start_offset']
        for seg in manifest['segments'].values():
            if seg['base_offset'] == start_model_offset:
                # Usually, the manifest's 'start_offset' will match the 'base_offset'
                # of a 'segment' as retention normally advances the start offset to
                # another segment's base offset. This branch covers this case.
                delta = seg['delta_offset']
                return start_model_offset - delta
            elif start_model_offset == seg['committed_offset'] + 1:
                # When retention decides to remove all current segments from the cloud
                # according to the retention policy, it advances the manifest's start
                # offset to `committed_offset + 1` of the latest segment present at the time.
                # Since, there's no guarantee that new segments haven't been added in the
                # meantime, we look for a match in all segments.
                delta = seg['delta_offset_end']
                return start_model_offset - delta

        assert False, (
            "'start_offset' in manifest is inconsistent with contents of 'segments'."
            "'start_offset' should match either the 'base_offset' or 'committed_offset + 1'"
            f"of a segment in 'segments': start_offset={start_model_offset}, segments={manifest['segments']}"
        )

    @staticmethod
    def kafka_last_offset(manifest) -> Optional[int]:
        if 'segments' not in manifest or len(manifest['segments']) == 0:
            return None

        last_model_offset = manifest['last_offset']
        last_segment = max(manifest['segments'].values(),
                           key=lambda seg: seg['base_offset'])
        delta = last_segment['delta_offset_end']

        return last_model_offset - delta

    def cloud_log_sizes_sum(self) -> CloudLogSize:
        """
        Returns the cloud log size summed over all ntps.
        """
        self._do_listing()
        if len(self._state.partition_manifests) != 1:
            raise Exception(
                f"Bucket doesn't have exactly one cluster's data: {self._state.partition_manifests.keys()}"
            )

        total = CloudLogSize.make_empty()
        partition_manifests = next(
            iter(self._state.partition_manifests.values()))
        for ns, topic, partition in partition_manifests.keys():
            val = self.cloud_log_size_for_ntp(topic, partition, ns)
            self.logger.debug(f"{topic}/{partition} log_size={val}")
            total += val

        self.logger.debug(f"cloud_log_size_sum()={total}")
        return total

    def _ensure_listing(self):
        if not self._state.listed is True:
            self._do_listing()
            self._state.listed = True

    def _do_listing(self):
        for o in self.client.list_objects(self.bucket):
            self.logger.debug(f"Loading object {o.key}")
            if self.path_matcher.is_partition_manifest(o):
                ntpr = parse_s3_manifest_path(o.key)
                self._load_manifest(ntpr, o.key)
            elif self.path_matcher.is_spillover_manifest(o):
                ntpr = parse_s3_manifest_path(o.key)
                self._load_spillover_manifest(ntpr, o.key)
            elif self.path_matcher.is_segment(o):
                self.logger.debug(f"Object {o.key} is a segment")
                self._state.segment_objects += 1
                if self._scan_segments:
                    spc = parse_s3_segment_path(o.key)
                    try:
                        self._add_segment_metadata(o.key, spc)
                    except ClientError as err:
                        # The segment was listed by ListObjectV2 request
                        # and deleted by Redpanda concurrently.
                        # We don't expect this to happen with the manifests
                        # so this error is only handled in case of segments
                        if err['Error']['Code'] == 'NoSuchKey':
                            self._state.ignored_objects += 1
            elif self.path_matcher.is_topic_manifest(o):
                pass
            elif self.path_matcher.is_tx_manifest(o):
                self._state.tx_manifests += 1
            elif self.path_matcher.is_segment_index(o):
                self._state.segment_indexes += 1
            elif self.path_matcher.is_cluster_metadata_manifest(o):
                self._load_cluster_metadata_manifest(o.key)
            elif self.path_matcher.is_controller_snapshot(o):
                self._load_controller_snapshot_size(o.key)
            else:
                self._state.ignored_objects += 1
        if self._scan_segments:
            self._sort_segment_summaries()

    def _sort_segment_summaries(self):
        """Sort segment summary lists by base offset"""
        for label, summaries in self._state.segment_summaries.items():
            res = {}
            for ntp, lst in summaries.items():
                self.logger.debug(
                    f"Sorting segment summaries for {label}/{ntp}")
                res[ntp] = sorted(lst, key=lambda x: x.base_offset)
            self._state.segment_summaries[label] = res

    def _get_manifest(self, ntpr: NTPR, path: str) -> dict:
        """
        Having composed the path for a manifest, download it and return the manifest dict

        Raises KeyError if the object is not found.
        """

        # explicit path, only try loading that and fail if it fails
        if ".bin" in path:
            format = ManifestFormat.BINARY
        elif ".json" in path:
            format = ManifestFormat.JSON
        else:
            raise RuntimeError(f"Unknown manifest key format: '{path}'")

        try:
            data = self.client.get_object_data(self.bucket, path)
        except Exception as e:
            # Very generic exception handling because the storage client
            # may be one of several classes with their own exceptions
            self.logger.debug(f"Exception loading {path}: {e}")
            raise KeyError(f"Manifest for ntp {ntpr} not found")

        if format == ManifestFormat.BINARY:
            manifest = RpStorageTool(
                self.logger).decode_partition_manifest(data)
        else:
            manifest = json.loads(data)

        return manifest

    def _load_manifest(self, ntpr: NTPR, path: str) -> dict:
        manifest = self._get_manifest(ntpr, path)

        label = parse_s3_partition_path_label(path)
        if label not in self._state.partition_manifests:
            self._state.partition_manifests[label] = {}
        self._state.partition_manifests[label][ntpr.to_ntp()] = manifest

        self.logger.debug(
            f"Loaded manifest for {ntpr} at {path}: {pprint.pformat(manifest, indent=2)}"
        )

        return manifest

    def _load_spillover_manifest(self, ntpr: NTPR,
                                 path: str) -> tuple[SpillMeta, dict]:
        manifest = self._get_manifest(ntpr, path)
        ntp = ntpr.to_ntp()
        label = parse_s3_partition_path_label(path)

        if label not in self._state.spillover_manifests:
            self._state.spillover_manifests[label] = {}
        if ntp not in self._state.spillover_manifests[label]:
            self._state.spillover_manifests[label][ntp] = {}

        meta = SpillMeta.make(ntpr, path)
        self._state.spillover_manifests[label][ntp][meta] = manifest

        self.logger.debug(
            f"Loaded spillover manifest for {ntpr} at {path}: {pprint.pformat(manifest, indent=2)}"
        )

        return meta, manifest

    def _add_segment_metadata(self, path, spc: SegmentPathComponents):
        if path.endswith(".index"):
            return
        if path.endswith(".tx"):
            return
        self.logger.debug(f"Parsing segment {spc} at {path}")
        label = parse_s3_partition_path_label(path)
        ntp = spc.ntpr.to_ntp()
        if label not in self._state.segment_summaries:
            self._state.segment_summaries[label] = {}
        if ntp not in self._state.segment_summaries[label]:
            self._state.segment_summaries[label][ntp] = []
        payload = self.client.get_object_data(self.bucket, path)
        reader = SegmentReader(io.BytesIO(payload))
        summary = make_segment_summary(spc.ntpr, reader)
        self._state.segment_summaries[label][ntp].append(summary)

    def _discover_spillover_manifests(self,
                                      ntpr: NTPR,
                                      label: str = "") -> list[SpillMeta]:
        list_res = self.client.list_objects(
            bucket=self.bucket,
            prefix=BucketView.gen_manifest_path(ntpr, remote_label=label))

        def is_spillover_manifest_path(path: str) -> bool:
            return not (path.endswith(".json") or path.endswith(".bin"))

        spill_metas = []
        for manifest_obj in list_res:
            if is_spillover_manifest_path(manifest_obj.key):
                spill_metas.append(SpillMeta.make(ntpr, manifest_obj.key))

        return sorted(spill_metas)

    def _load_cluster_metadata_manifest(self, path: str) -> dict:
        try:
            data = self.client.get_object_data(self.bucket, path)
        except Exception as e:
            self.logger.debug(f"Exception loading {path}: {e}")
            raise KeyError(f"Cluster manifest at {path} failed to load")
        manifest = json.loads(data)
        self.logger.debug(
            f"Loaded cluster manifest at {path}: {pprint.pformat(manifest)}")
        cluster_uuid, meta_id = parse_cluster_metadata_manifest_path(path)
        if cluster_uuid not in self._state.cluster_metadata:
            self._state.cluster_metadata[cluster_uuid] = ClusterMetadata(
                dict(), dict())
        self._state.cluster_metadata[cluster_uuid].cluster_metadata_manifests[
            meta_id] = manifest
        return manifest

    def _load_controller_snapshot_size(self, path: str) -> int:
        try:
            meta = self.client.get_object_meta(self.bucket, path)
        except Exception as e:
            self.logger.debug(f"Exception loading {path}: {e}")
            raise KeyError(f"Cluster manifest at {path} failed to load")
        self.logger.debug(f"Loaded controller snapshot at {path}: {meta}")
        cluster_uuid, offset = parse_controller_snapshot_path(path)
        if cluster_uuid not in self._state.cluster_metadata:
            self._state.cluster_metadata[cluster_uuid] = ClusterMetadata(
                dict(), dict())
        self._state.cluster_metadata[cluster_uuid].controller_snapshot_sizes[
            offset] = meta.content_length
        return meta.content_length

    @staticmethod
    def gen_manifest_path(ntpr: NTPR,
                          extension: str = "bin",
                          remote_label: str = ""):
        path = f"{ntpr.ns}/{ntpr.topic}/{ntpr.partition}_{ntpr.revision}"
        if len(remote_label) == 0:
            x = xxhash.xxh32()
            x.update(path.encode('ascii'))
            hash = x.hexdigest()[0] + '0000000'
            return f"{hash}/meta/{path}/manifest.{extension}"
        return f"{remote_label}/meta/{path}/manifest.{extension}"

    def get_partition_manifest(self,
                               ntp: NTP | NTPR,
                               target_label: Optional[str] = None) -> dict:
        """
        Fetch a manifest, looking up revision as needed.

        If a specific remote label is not being targeted, expects there to be
        at most one matching manifest in the bucket.
        """
        ntpr = None
        if isinstance(ntp, NTPR):
            ntpr = ntp
            ntp = ntpr.to_ntp()

        matching_labels = []
        for label, pms in self._state.partition_manifests.items():
            if target_label is not None and target_label != label:
                continue
            if ntp in pms:
                matching_labels.append(label)
        if len(matching_labels) > 1:
            raise Exception(
                f"Multiple labels contain {ntp}: {matching_labels}")

        if len(matching_labels) == 1:
            return self._state.partition_manifests[matching_labels[0]][ntp]

        if not ntpr:
            ntpr = self.ntp_to_ntpr(ntp)

        # If we need to look for the partition manifest, look for topic
        # manifests first to see what cluster uuid labels to expect (note,
        # based on the naming scheme, it's easier to find topic manifests
        # without the cluster uuid in hand than it is to find partition
        # manifests)
        topic = NT(ntpr.ns, ntpr.topic)
        topic_manifest_paths = self._find_topic_manifest_paths(topic)

        for tm_path in topic_manifest_paths:
            label = parse_s3_topic_label(tm_path)
            if target_label is not None and target_label != label:
                continue
            paths = [BucketView.gen_manifest_path(ntpr, "bin", label)]
            if len(label) == 0:
                # Versions of Redpanda below 24.2 don't have labels. Farther
                # back, we also supported JSON manifests. As a crude heuristic
                # assume we may need to look for JSON if we don't have a label.
                paths.append(BucketView.gen_manifest_path(ntpr, "json", label))

            for path in paths:
                m: dict = {}
                try:
                    m = self._load_manifest(ntpr, path)
                except KeyError:
                    continue
                return m
        raise KeyError(f"Manifest for ntp {ntpr} not found")

    def get_spillover_metadata(self, ntp: NTP | NTPR) -> list[SpillMeta]:
        """
        Returns a sorted list of metadata describing each spill over manifest
        for 'ntp'. Note that the manifests themselves are not fetched.
        """
        ntpr = None
        if isinstance(ntp, NTPR):
            ntpr = ntp
            ntp = ntpr.to_ntp()

        if not ntpr:
            ntpr = self.ntp_to_ntpr(ntp)

        topic = NT(ntpr.ns, ntpr.topic)
        topic_manifest_paths = self._find_topic_manifest_paths(topic)

        for tm_path in topic_manifest_paths:
            label = parse_s3_topic_label(tm_path)
            spills = self._discover_spillover_manifests(ntpr, label)
            if len(spills) == 0:
                continue
            return spills
        return []

    def get_spillover_manifests(
            self, ntp: NTP | NTPR) -> Optional[dict[SpillMeta, dict]]:
        """
        Discovers and downloads the spillover manifests for 'ntp'. If no spillover
        manifests exist 'None' is returned. Note that the results are cached and will
        be used in subsequent calls.
        """
        ntpr = None
        if isinstance(ntp, NTPR):
            ntpr = ntp
            ntp = ntpr.to_ntp()

        matching_labels = []
        for label, pms in self._state.spillover_manifests.items():
            if ntp in pms:
                matching_labels.append(label)
        if len(matching_labels) > 1:
            raise Exception(
                f"Multiple labels contain {ntp}: {matching_labels}")

        if len(matching_labels) == 1:
            return self._state.spillover_manifests[matching_labels[0]][ntp]

        if not ntpr:
            ntpr = self.ntp_to_ntpr(ntp)

        topic = NT(ntpr.ns, ntpr.topic)
        topic_manifest_paths = self._find_topic_manifest_paths(topic)

        # If we need to look for the spillover manifests, look for topic
        # manifests first to see what cluster uuid labels to expect (note,
        # based on the naming scheme, it's easier to find topic manifests
        # without the cluster uuid in hand than it is to find partition
        # manifests)
        for tm_path in topic_manifest_paths:
            label = parse_s3_topic_label(tm_path)
            spills = self._discover_spillover_manifests(ntpr, label)
            if len(spills) == 0:
                continue
            for spill in spills:
                self._load_spillover_manifest(spill.ntpr, spill.path)
            return self._state.spillover_manifests[label][ntp]
        return None

    def _load_manifest_v1_from_data(
            self, data, manifest_format: Literal['json', 'bin']) -> dict:
        """
        Either decode a bin topic_manifest or json load a json topic_manifest.
        the result is a dict of only the fields that were serialized in v1 of topic_manifest
        """
        if manifest_format == 'bin':
            return OfflineLogViewer(self.redpanda).read_bin_topic_manifest(
                data, return_legacy_format=True)
        else:
            return json.loads(data)

    def _load_topic_manifest(self, topic: NT, path: str,
                             manifest_format: Literal['json', 'bin']):
        try:
            data = self.client.get_object_data(self.bucket, path)
        except Exception as e:
            self.logger.debug(f"Exception loading {path}: {e}")
            raise KeyError(f"Manifest for topic {topic} not found")

        manifest = self._load_manifest_v1_from_data(
            data, manifest_format=manifest_format)

        self.logger.debug(
            f"Loaded topic manifest from {path} {topic}: {pprint.pformat(manifest)}"
        )

        label = parse_s3_topic_label(path)
        if label not in self._state.topic_manifests:
            self._state.topic_manifests[label] = {}
        self._state.topic_manifests[label][topic] = manifest
        return manifest

    def get_topic_manifest_from_path(self, path: str) -> dict:
        """
        Download the object at `path` and decode it as topic_manifest.
        supports bin and json formats.
        """
        try:
            data = self.client.get_object_data(self.bucket, path)
        except Exception as e:
            self.logger.debug(f"Exception loading {path}: {e}")
            raise KeyError(f"Manifest @path={path} not found")

        manifest = self._load_manifest_v1_from_data(
            data, manifest_format='bin' if path.endswith('bin') else 'json')
        self.logger.debug(
            f"Loaded topic manifest from {path} for {manifest['topic']}: {pprint.pformat(manifest)}"
        )
        return manifest

    def _find_topic_metas(self, topic: NT) -> list:
        path = f"{topic.ns}/{topic.topic}"
        list_prefixes = []

        # First, look for newer, labeled manifests.
        list_prefixes.append(f"meta/{path}/")

        # If none, we'll fall back on legacy, hash-prefixed manifests.
        x = xxhash.xxh32()
        x.update(path.encode('ascii'))
        hash = x.hexdigest()[0] + '0000000'
        list_prefixes.append(f"{hash}/meta/{path}/")

        ret = []
        for prefix in list_prefixes:
            for obj_meta in self.client.list_objects(self.bucket,
                                                     prefix=prefix):
                ret.append(obj_meta)
        return ret

    def _find_topic_manifest_paths(self, topic: NT) -> list:
        return [
            meta.key for meta in self._find_topic_metas(topic)
            if meta.key.endswith("topic_manifest.bin")
            or meta.key.endswith("topic_manifest.json")
        ]

    def get_topic_manifest(self,
                           topic: NT,
                           target_label: Optional[str] = None) -> dict:
        """
        try to download a topic_manifest.bin for topic. if no object is found, fallback to topic_manifest.json
        """
        matching_labels = []
        for label, tms in self._state.topic_manifests.items():
            if target_label is not None and target_label != label:
                continue
            if topic in tms:
                matching_labels.append(label)
        if len(matching_labels) > 1:
            raise Exception(
                f"Multiple labels contain {topic}: {matching_labels}")

        if len(matching_labels) == 1:
            return self._state.topic_manifests[matching_labels[0]][topic]

        topic_manifest_paths = self._find_topic_manifest_paths(topic)
        for path in topic_manifest_paths:
            label = parse_s3_topic_label(path)
            if target_label is not None and target_label != label:
                continue
            format = "bin" if path.endswith(".bin") else "json"
            return self._load_topic_manifest(topic,
                                             path,
                                             manifest_format=format)

        raise KeyError(f"Topic manifest not found for {topic}")

    def get_lifecycle_marker_objects(
        self,
        topic: NT,
    ) -> list[ObjectMetadata]:
        """
        Topic manifests are identified by namespace-topic, whereas lifecycle
        markers are identified by namespace-topic-revision.

        It is convenient in tests to retrieve by NT though.
        """
        return [
            meta for meta in self._find_topic_metas(topic)
            if meta.key.endswith("lifecycle.bin")
        ]

    def get_lifecycle_marker(self, topic: NT) -> dict:
        """
        Convenience: when we expect only one lifecycle marker for an NT (i.e. there
        are not multiple revisions).  Return exactly one, or assert
        """
        objects = self.get_lifecycle_marker_objects(topic)
        if len(objects) != 1:
            raise RuntimeError(
                f"Expected exactly 1 lifecycle marker for {topic}, found {len(objects)}"
            )

        key = objects[0].key
        body = self.client.get_object_data(self.bucket, key)
        decoded = RpStorageTool(self.logger).decode_lifecycle_marker(body)
        self.logger.debug(
            f"Decoded lifecycle marker for {topic}: {json.dumps(decoded,indent=2)}"
        )
        return decoded

    def find_segment_in_manifests(self, o: ObjectMetadata) -> Optional[dict]:
        """
        Checks that given object is a segment, and is a part of one of the test partition manifests
        with a matching archiver term. If that's the case, the metadata associated with the segment
        is returned.
        """
        try:
            if not self.path_matcher.is_segment(o):
                return None

            segment_path = parse_s3_segment_path(o.key)
            partition_manifest = self.get_partition_manifest(segment_path.ntpr)
            if not partition_manifest:
                self.logger.warn(f'no manifest found for {segment_path.ntpr}')
                return None

            segments_in_manifest = partition_manifest['segments']

            # Filename for segment contains the archiver term, eg:
            # 4886-1-v1.log.2 -> 4886-1-v1.log and 2
            base_name, archiver_term = segment_path.name.rsplit('.', 1)

            # New segment path format is base-committed-size-term-v1.log
            base_name_tokens = base_name.split('-')
            manifest_key = '-'.join([base_name_tokens[i] for i in (0, -2)])
            manifest_key = f'{manifest_key}-v1.log'
            segment_entry = segments_in_manifest.get(manifest_key)
            if not segment_entry:
                self.logger.warn(
                    f'no entry found for segment path {manifest_key} '
                    f'in manifest: {pprint.pformat(segments_in_manifest, indent=2)}'
                )
                return None

            # Archiver term should match the value in partition manifest
            manifest_archiver_term = str(segment_entry['archiver_term'])
            if archiver_term == manifest_archiver_term:
                return segment_entry

            self.logger.warn(
                f'{segment_path} has archiver term {archiver_term} '
                f'which does not match manifest term {manifest_archiver_term}')
            return None
        except Exception as e:
            self.logger.info(f'error {e} while checking if {o} is a segment')
            return None

    def is_ntp_in_manifest(self,
                           topic: str,
                           partition: int,
                           ns: str = "kafka") -> bool:
        """
        Whether a manifest is present for this NTP
        """
        ntp = NTP(ns, topic, partition)
        try:
            self.get_partition_manifest(ntp)
        except KeyError:
            return False
        else:
            return True

    def manifest_for_ntpr(self,
                          topic: str,
                          partition: int,
                          revision: int,
                          ns: str = 'kafka') -> dict:
        ntpr = NTPR(ns, topic, partition, revision)
        return self.get_partition_manifest(ntpr)

    def manifest_for_ntp(self,
                         topic: str,
                         partition: int,
                         ns: str = 'kafka') -> dict:
        ntp = NTP(ns, topic, partition)
        return self.get_partition_manifest(ntp)

    def cloud_log_segment_count_for_ntp(self,
                                        topic: str,
                                        partition: int,
                                        ns: str = 'kafka') -> int:
        manifest = self.manifest_for_ntp(topic, partition, ns)
        if 'segments' not in manifest:
            return 0

        return len(manifest['segments'])

    def stm_region_size_for_ntp(self,
                                topic: str,
                                partition: int,
                                ns: str = "kafka") -> LogRegionSize:
        size = LogRegionSize()

        try:
            manifest = self.manifest_for_ntp(topic, partition, ns)
        except KeyError:
            return size

        if 'segments' not in manifest or len(manifest['segments']) == 0:
            return size

        so = manifest['start_offset']
        for seg in manifest['segments'].values():
            size.total += seg['size_bytes']
            if seg['base_offset'] >= so:
                size.accessible += seg['size_bytes']

        return size

    def archive_size_for_ntp(self,
                             topic: str,
                             partition: int,
                             ns: str = "kafka") -> LogRegionSize:
        size = LogRegionSize()

        try:
            manifest = self.manifest_for_ntp(topic, partition, ns)
        except KeyError:
            return size

        if "archive_clean_offset" not in manifest or "archive_start_offset" not in manifest:
            return size

        archive_clean_offset = manifest["archive_clean_offset"]
        archive_start_offset = manifest["archive_start_offset"]

        spills = self.get_spillover_manifests(NTP(ns, topic, partition))
        if spills is None:
            return size

        for meta, spill in spills.items():
            for seg in spill["segments"].values():
                if seg["base_offset"] >= archive_clean_offset:
                    size.total += seg["size_bytes"]
                if seg["base_offset"] >= archive_start_offset:
                    size.accessible += seg["size_bytes"]

        return size

    def cloud_log_size_for_ntp(self,
                               topic: str,
                               partition: int,
                               ns: str = 'kafka') -> CloudLogSize:
        return CloudLogSize(
            stm=self.stm_region_size_for_ntp(topic, partition, ns),
            archive=self.archive_size_for_ntp(topic, partition, ns))

    def assert_at_least_n_uploaded_segments_compacted(self,
                                                      topic: str,
                                                      partition: int,
                                                      revision: Optional[int],
                                                      n=1):
        if revision:
            manifest_data = self.manifest_for_ntpr(topic, partition, revision)
        else:
            manifest_data = self.manifest_for_ntp(topic, partition)
        segments = manifest_data['segments']
        compacted_segments = len(
            [meta for meta in segments.values() if meta['is_compacted']])
        assert compacted_segments >= n, f"Could not find {n} compacted segments, " \
                                        f"total uploaded: {len(segments)}, " \
                                        f"total compacted: {compacted_segments}"

    def assert_segments_replaced(self, topic: str, partition: int):
        manifest_data = self.manifest_for_ntp(topic, partition)
        assert len(manifest_data.get(
            'replaced',
            [])) > 0, f"No replaced segments after compacted segments uploaded"

    def assert_segments_deleted(self, topic: str, partition: int):
        manifest = self.manifest_for_ntp(topic, partition)
        assert manifest.get('start_offset', 0) > 0

        first_segment = min(manifest['segments'].values(),
                            key=lambda seg: seg['base_offset'])
        assert first_segment['base_offset'] > 0

    def segment_summaries(self, ntp: NTP) -> list[SegmentSummary]:
        self._ensure_listing()
        matching_labels = []
        for label, summaries in self._state.segment_summaries.items():
            if ntp in summaries:
                matching_labels.append(label)
        if len(matching_labels) > 1:
            raise Exception(
                f"Multiple labels contain {ntp}: {matching_labels}")

        if len(matching_labels) == 1:
            return self._state.segment_summaries[matching_labels[0]][ntp]

        return []

    def is_archive_cleanup_complete(self, ntp: NTP):
        self._ensure_listing()
        manifest = self.manifest_for_ntp(ntp.topic, ntp.partition)
        aso = manifest.get('archive_start_offset', DEFAULT_OFFSET)
        aco = manifest.get('archive_clean_offset', DEFAULT_OFFSET)
        summaries = self.segment_summaries(ntp)
        num = len(summaries)
        if aso > 0 and aco < aso:
            self.logger.debug(
                f"archive cleanup is not complete, start: {aso}, clean: {aco}, len: {num}"
            )
            return False

        if len(summaries) == 0:
            self.logger.debug(
                f"archive is empty, start: {aso}, clean: {aco}, len: {num}")
            return True

        first_segment = min(summaries, key=lambda seg: seg.base_offset)

        if first_segment.base_offset != aso:
            self.logger.debug(
                f"segments are not deleted, start: {aso}, clean: {aco}, first segment: {first_segment}"
            )
            return False

        return True

    def check_archive_integrity(self, ntp: NTP):
        self._ensure_listing()
        manifest = self.manifest_for_ntp(ntp.topic, ntp.partition)
        summaries = self.segment_summaries(ntp)
        if len(summaries) == 0:
            assert 'archive_start_offset' not in manifest
            assert 'archive_start_offset' not in manifest
        else:
            next_base_offset = manifest.get('archive_start_offset')
            stm_start_offset = manifest.get('start_offset')
            expected_last = manifest.get('last_offset')

            for summary in summaries:
                assert next_base_offset == summary.base_offset, f"Unexpected segment {summary}, expected base offset {next_base_offset}"
                next_base_offset = summary.last_offset + 1
            else:
                assert expected_last == summary.last_offset, f"Unexpected last offset {summary.last_offset}, expected: {expected_last}"
