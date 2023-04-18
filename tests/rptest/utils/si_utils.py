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
import pprint
import struct
from collections import defaultdict, namedtuple
from typing import Sequence, Optional, NewType

import xxhash

from rptest.archival.s3_client import ObjectMetadata
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.redpanda import MetricsEndpoint, RESTART_LOG_ALLOW_LIST

EMPTY_SEGMENT_SIZE = 4096

BLOCK_SIZE = 4096

default_log_segment_size = 1048576  # 1MB

NTPBase = namedtuple("NTP", ['ns', 'topic', 'partition'])
NTPRBase = namedtuple("NTP", ['ns', 'topic', 'partition', 'revision'])


class NTP(NTPBase):
    def to_ntpr(self, revision: int) -> 'NTPR':
        return NTPR(self.ns, self.topic, self.partition, revision)


class NTPR(NTPRBase):
    def to_ntp(self) -> NTP:
        return NTP(self.ns, self.topic, self.partition)


TopicManifestMetadata = namedtuple('TopicManifestMetadata',
                                   ['ntp', 'revision'])
SegmentMetadata = namedtuple(
    'SegmentMetadata',
    ['ntp', 'revision', 'base_offset', 'term', 'md5', 'size'])

SegmentPathComponents = namedtuple('SegmentPathComponents', ['ntpr', 'name'])

MISSING_DATA_ERRORS = [
    "No segments found. Empty partition manifest generated",
    "Error during log recovery: cloud_storage::missing_partition_exception",
    "Failed segment download",
]

TRANSIENT_ERRORS = RESTART_LOG_ALLOW_LIST + [
    "raft::offset_monitor::wait_timed_out",
    "Upload loop error: seastar::timed_out_error"
]


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
            assert len(data) == records_size
            return header
        return None

    def __iter__(self):
        while True:
            it = self.read_batch()
            if it is None:
                return
            yield it


def parse_s3_manifest_path(path: str) -> NTPR:
    """Parse S3 manifest path. Return ntp and revision.
    Sample name: 50000000/meta/kafka/panda-topic/0_19/manifest.json
    """
    items = path.split('/')
    ns = items[2]
    topic = items[3]
    part_rev = items[4].split('_')
    partition = int(part_rev[0])
    revision = int(part_rev[1])
    return NTPR(ns=ns, topic=topic, partition=partition, revision=revision)


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
    ntpr = NTPR(ns=ns, topic=topic, partition=partition, revision=revision)
    return SegmentPathComponents(ntpr=ntpr, name=fname)


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
                    assert total_size == ntps[ntp]
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


def gen_manifest_path(ntpr: NTPR):
    x = xxhash.xxh32()
    path = f"{ntpr.ns}/{ntpr.topic}/{ntpr.partition}_{ntpr.revision}"
    x.update(path.encode('ascii'))
    hash = x.hexdigest()[0] + '0000000'
    return f"{hash}/meta/{path}/manifest.json"


def gen_segment_name_from_meta(meta: dict, key: Optional[str]) -> str:
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


NodeReport = NewType('NodeReport', dict[str, (str, int)])
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
    return expected_restored_sizes


def nodes_report_cloud_segments(redpanda, target_segments):
    """
    Returns true if the nodes in the cluster collectively report having
    above the given number of segments.

    NOTE: we're explicitly not checking the manifest via cloud client
    because we expect the number of items in our bucket to be quite large,
    and for associated ListObjects calls to take a long time.
    """
    try:
        num_segments = redpanda.metric_sum(
            "redpanda_cloud_storage_segments",
            metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
        redpanda.logger.info(
            f"Cluster metrics report {num_segments} / {target_segments} cloud segments"
        )
    except:
        return False
    return num_segments >= target_segments


def is_close_size(actual_size, expected_size):
    """Checks if the log size is close to expected size.
    The actual size shouldn't be less than expected. Also, the difference
    between two values shouldn't be greater than the size of one segment.
    """
    lower_bound = expected_size
    upper_bound = expected_size + default_log_segment_size + \
                  int(default_log_segment_size * 0.2)
    return actual_size in range(lower_bound, upper_bound)


class PathMatcher:
    def __init__(self, expected_topics: Optional[Sequence[TopicSpec]] = None):
        self.expected_topics = expected_topics
        if self.expected_topics is not None:
            self.topic_names = {t.name for t in self.expected_topics}
            self.topic_manifest_paths = {
                f'/{t}/topic_manifest.json'
                for t in self.topic_names
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
        if self.topic_manifest_paths is None:
            return True
        else:
            return any(key.endswith(t) for t in self.topic_manifest_paths)

    def is_partition_manifest(self, o: ObjectMetadata) -> bool:
        return o.key.endswith(
            '/manifest.json') and self._match_partition_manifest(o.key)

    def is_topic_manifest(self, o: ObjectMetadata) -> bool:
        return self._match_topic_manifest(o.key)

    def is_segment(self, o: ObjectMetadata) -> bool:
        try:
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


class BucketViewState:
    """
    Results of a full listing of the bucket, if listed is True, or
    partial results if listed is False
    """
    def __init__(self):
        self.listed = False
        self.segment_objects = 0
        self.ignored_objects = 0
        self.partition_manifests = {}


class BucketView:
    """
    A caching view onto an object storage bucket, that knows how to construct paths
    for manifests, and can also scan the bucket for aggregate segment info.

    For directly fetching a manifest or manifest-derived info for a partition,
    this will avoid doing object listings.  Access to properties that require
    object listings (like the object counts) will list the bucket and then cache
    the result.
    """
    def __init__(self, redpanda, topics: Optional[Sequence[TopicSpec]] = None):
        """

        :param redpanda: a RedpandaService, whose SISettings we will use
        :param topics: optional list of topics to filter result to
        """
        self.redpanda = redpanda
        self.logger = redpanda.logger
        self.bucket = redpanda.si_settings.cloud_storage_bucket
        self.client = redpanda.cloud_storage_client
        self.path_matcher = PathMatcher(topics)

        # Cache built on demand from admin API
        self._ntp_to_revision = None

        self._state = BucketViewState()

    def reset(self):
        """
        Drop all cached state, so that subsequent calls will use fresh data
        """
        self._state = BucketViewState()

    def ntp_to_ntpr(self, ntp: NTP) -> NTPR:
        """
        Raises KeyError if the NTP is not found
        """
        if self._ntp_to_revision is None:
            self._ntp_to_revision = {}
            admin = Admin(self.redpanda)
            for p in admin.get_leaders_info():
                self._ntp_to_revision[NTP(
                    ns=p['ns'], topic=p['topic'],
                    partition=p['partition_id'])] = p['partition_revision']

        revision = self._ntp_to_revision[ntp]
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
    def partition_manifests(self) -> dict[NTP, dict]:
        self._ensure_listing()
        return self._state.partition_manifests

    @staticmethod
    def cloud_log_size_from_ntp_manifest(manifest,
                                         include_below_start_offset=True
                                         ) -> int:
        if 'segments' not in manifest:
            return 0

        start_offset = 0
        if not include_below_start_offset:
            start_offset = manifest['start_offset']

        res = sum(seg_meta['size_bytes']
                  for seg_meta in manifest['segments'].values()
                  if seg_meta['base_offset'] >= start_offset)

        return res

    @staticmethod
    def kafka_start_offset(manifest) -> Optional[int]:
        if 'segments' not in manifest or len(manifest['segments']) == 0:
            return None

        start_model_offset = manifest['start_offset']
        first_segment = min(manifest['segments'].values(),
                            key=lambda seg: seg['base_offset'])
        delta = first_segment['delta_offset']

        return start_model_offset - delta

    @staticmethod
    def kafka_last_offset(manifest) -> Optional[int]:
        if 'segments' not in manifest or len(manifest['segments']) == 0:
            return None

        last_model_offset = manifest['last_offset']
        last_segment = max(manifest['segments'].values(),
                           key=lambda seg: seg['base_offset'])
        delta = last_segment['delta_offset_end']

        return last_model_offset - delta

    def total_cloud_log_size(self) -> int:
        self._do_listing()

        total = 0
        for pm in self._state.partition_manifests.values():
            total += BucketView.cloud_log_size_from_ntp_manifest(
                pm, include_below_start_offset=False)

        return total

    def _ensure_listing(self):
        if not self._state.listed is True:
            self._do_listing()
            self._state.listed = True

    def _do_listing(self):
        for o in self.client.list_objects(self.bucket):
            if self.path_matcher.is_partition_manifest(o):
                ntpr = parse_s3_manifest_path(o.key)
                ntp = ntpr.to_ntp()
                manifest = self._load_manifest(ntp, o.key)
                self.logger.debug(f'registered partition manifest for {ntp}: '
                                  f'{pprint.pformat(manifest, indent=2)}')
            elif self.path_matcher.is_segment(o):
                self._state.segment_objects += 1
            elif self.path_matcher.is_topic_manifest(o):
                pass
            else:
                self._state.ignored_objects += 1

    def _load_manifest(self, ntp, path):
        """
        Having composed the path for a manifest, download it cache the result, and return the manifest dict

        Raises KeyError if the object is not found.
        """
        try:
            data = self.client.get_object_data(self.bucket, path)
        except Exception as e:
            # Very generic exception handling because the storage client
            # may be one of several classes with their own exceptions
            self.logger.debug(f"Exception loading {path}: {e}")
            raise KeyError(f"Manifest for ntp {ntp} not found")

        manifest = json.loads(data)

        self.logger.debug(f"Loaded manifest {ntp}: {pprint.pformat(manifest)}")

        self._state.partition_manifests[ntp] = manifest
        return manifest

    def get_partition_manifest(self, ntp: NTP | NTPR):
        """
        Fetch a manifest, looking up revision as needed.
        """
        ntpr = None
        if isinstance(ntp, NTPR):
            ntpr = ntp
            ntp = ntpr.to_ntp()

        if ntp in self._state.partition_manifests:
            return self._state.partition_manifests[ntp]

        if not ntpr:
            ntpr = self.ntp_to_ntpr(ntp)

        manifest_path = gen_manifest_path(ntpr)
        return self._load_manifest(ntp, manifest_path)

    def is_segment_part_of_a_manifest(self, o: ObjectMetadata) -> bool:
        """
        Queries that given object is a segment, and is a part of one of the test partition manifests
        with a matching archiver term
        """
        try:
            if not self.path_matcher.is_segment(o):
                return False

            segment_path = parse_s3_segment_path(o.key)
            partition_manifest = self.get_partition_manifest(segment_path.ntpr)
            if not partition_manifest:
                self.logger.warn(f'no manifest found for {segment_path.ntpr}')
                return False

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
                return False

            # Archiver term should match the value in partition manifest
            manifest_archiver_term = str(segment_entry['archiver_term'])
            if archiver_term == manifest_archiver_term:
                return True

            self.logger.warn(
                f'{segment_path} has archiver term {archiver_term} '
                f'which does not match manifest term {manifest_archiver_term}')
            return False
        except Exception as e:
            self.logger.info(f'error {e} while checking if {o} is a segment')
            return False

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

    def cloud_log_size_for_ntp(self,
                               topic: str,
                               partition: int,
                               ns: str = 'kafka') -> int:
        try:
            manifest = self.manifest_for_ntp(topic, partition, ns)
        except KeyError:
            return 0
        else:
            return BucketView.cloud_log_size_from_ntp_manifest(manifest)

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
