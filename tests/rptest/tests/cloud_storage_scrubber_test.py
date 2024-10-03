# Copyright 2023 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License(the "License"); you may not use this file except in compliance with
# the License.You may obtain a copy of the License at
#
# https: // github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

import collections
import itertools
import json
import random
import re
import time
from dataclasses import dataclass
from typing import DefaultDict, Optional, Tuple

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from requests.exceptions import HTTPError

from rptest.archival.s3_client import ObjectMetadata
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import SISettings, get_cloud_storage_type, MetricsEndpoint
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result
from rptest.utils.allow_logs_on_predicate import AllowLogsOnPredicate
from rptest.utils.si_utils import parse_s3_segment_path, quiesce_uploads, BucketView, NTP, NTPR, SpillMeta

SCRUBBER_LOG_ALLOW_LIST = [
    # Attempts to compute the retention point for the cloud log will fail
    # after a spillover manifest is manually removed.
    r"cloud_storage - .* failed to download manifest {key_not_found}",
    r"cloud_storage - .* failed to download manifest.*cloud_storage::error_outcome:2",
    r"cloud_storage - .* Failed to materialize.*cloud_storage::error_outcome:2",
    r"cloud_storage - .* failed to seek.*cloud_storage::error_outcome:2",
    r"cloud_storage - .* Failed to compute time-based retention",
    r"cloud_storage - .* Failed to compute size-based retention",
    r"cloud_storage - .* Failed to compute retention",
    r"archival - .* Failed to compute archive retention",

    # The test removes a segment from the manifest for the scrubber to detect. A reupload
    # may be attempted somewhere after the base offset of the removed segment. The STM
    # manifest will refuse to apply the command and log the error below.
    r"cloud_storage - .* New replacement segment does not line up with previous segment",
    r"cluster - .* Can't add segment:"
]


@dataclass(frozen=True)
class SegmentMeta:
    base_offset: int
    committed_offset: int
    delta_offset: int
    delta_offset_end: int
    base_timestamp: int
    max_timestamp: int
    size_bytes: int
    is_compacted: bool
    archiver_term: int
    segment_term: int
    ntp_revision: int

    @staticmethod
    def from_dict(json_dict: dict):
        return SegmentMeta(**json_dict)


@dataclass(frozen=True)
class SegmentMetaAnomaly:
    anomaly_type: str
    explanation: str
    at_segment: SegmentMeta
    previous_segment: SegmentMeta

    @staticmethod
    def from_dict(json_dict: dict):
        return SegmentMetaAnomaly(anomaly_type=json_dict["type"],
                                  explanation=json_dict["explanation"],
                                  at_segment=SegmentMeta.from_dict(
                                      json_dict["at_segment"]),
                                  previous_segment=SegmentMeta.from_dict(
                                      json_dict["previous_segment"]))


@dataclass
class OffsetRange:
    first_inclusive: int
    last_inclusive: int

    def overlaps(self, other: 'OffsetRange') -> bool:
        return (self.first_inclusive <= other.last_inclusive
                and self.last_inclusive >= other.first_inclusive)


def overlaps_with_any(r: OffsetRange, others: list[OffsetRange]):
    return any((r.overlaps(o) for o in others))


@dataclass
class Anomalies:
    missing_partition_manifest: bool
    missing_spillover_manifests: set[str]
    missing_segments: set[str]
    segment_metadata_anomalies: set[SegmentMetaAnomaly]
    last_complete_scrub_at: Optional[int]

    @staticmethod
    def make_empty():
        return Anomalies(missing_partition_manifest=False,
                         missing_spillover_manifests=set(),
                         missing_segments=set(),
                         segment_metadata_anomalies=set(),
                         last_complete_scrub_at=None)

    @staticmethod
    def from_dict(json_dict: dict) -> Tuple[NTPR, "Anomalies"]:
        ntpr = NTPR(ns=json_dict["ns"],
                    topic=json_dict["topic"],
                    partition=json_dict["partition"],
                    revision=json_dict["revision_id"])

        missing_spills = set(json_dict.get("missing_spillover_manifests", []))
        missing_segs = set(json_dict.get("missing_segments", []))
        meta_anomalies = set(
            map(lambda a: SegmentMetaAnomaly.from_dict(a),
                json_dict.get("segment_metadata_anomalies", [])))

        return ntpr, Anomalies(missing_partition_manifest=json_dict.get(
            "missing_partition_manifest", False),
                               missing_spillover_manifests=missing_spills,
                               missing_segments=missing_segs,
                               segment_metadata_anomalies=meta_anomalies,
                               last_complete_scrub_at=json_dict.get(
                                   "last_complete_scrub_at", None))

    def is_empty(self) -> bool:
        return (self.missing_partition_manifest == False
                and len(self.missing_spillover_manifests) == 0
                and len(self.missing_segments) == 0
                and len(self.segment_metadata_anomalies) == 0)

    def is_subset_of(self, other: "Anomalies") -> bool:
        if other.missing_partition_manifest and not self.missing_partition_manifest:
            return False

        return (self.missing_segments.issubset(other.missing_segments)
                and self.missing_spillover_manifests.issubset(
                    other.missing_spillover_manifests)
                and self.segment_metadata_anomalies.issubset(
                    other.segment_metadata_anomalies))

    def get_impacted_offset_ranges(self, ntpr: NTPR,
                                   view: BucketView) -> list[OffsetRange]:
        impacted_ranges: list[OffsetRange] = []
        for spill in self.missing_spillover_manifests:
            meta = SpillMeta.make(ntpr=ntpr, path=spill)
            impacted_ranges.append(
                OffsetRange(first_inclusive=meta.base,
                            last_inclusive=meta.last))

        for seg in self.missing_segments:
            obj_meta = ObjectMetadata(key=seg,
                                      bucket="",
                                      etag="",
                                      content_length=0)
            if seg_meta := view.find_segment_in_manifests(obj_meta):
                impacted_ranges.append(
                    OffsetRange(first_inclusive=seg_meta["base_offset"],
                                last_inclusive=seg_meta["committed_offset"]))

        for anomaly_meta in self.segment_metadata_anomalies:
            impacted_ranges.append(
                OffsetRange(
                    first_inclusive=anomaly_meta.at_segment.base_offset,
                    last_inclusive=anomaly_meta.at_segment.committed_offset))

        return impacted_ranges


class CloudStorageScrubberTest(RedpandaTest):
    scrub_timeout = 200
    partition_count = 3
    message_size = 16 * 1024  # 16KiB
    segment_size = 1024 * 1024  # 1MiB
    to_produce = 100 * 1024 * 1024  # 100MiB per partition
    topics = [TopicSpec(partition_count=partition_count)]

    def __init__(self, test_context):
        super().__init__(
            test_context=test_context,
            extra_rp_conf={
                "cloud_storage_partial_scrub_interval_ms": 100,
                "cloud_storage_full_scrub_interval_ms": 1000,
                "cloud_storage_scrubbing_interval_jitter_ms": 50,
                # Small quota forces partial scrubs
                "cloud_storage_background_jobs_quota": 40,
                # Disable segment merging since it can reupload
                # the deleted segment and remove the gap
                "cloud_storage_enable_segment_merging": False,
                "cloud_storage_spillover_manifest_size": None,
            },
            si_settings=SISettings(
                test_context,
                log_segment_size=self.segment_size,
                cloud_storage_spillover_manifest_max_segments=10,
                cloud_storage_housekeeping_interval_ms=1000 * 10,
                fast_uploads=True))

        self.bucket_name = self.si_settings.cloud_storage_bucket
        self.rpk = RpkTool(self.redpanda)
        self.expected_error_logs = []

    def _produce(self):
        # Use a smaller working set for debug builds to keep the test timely
        if self.debug_mode:
            self.to_produce //= 2

        msg_count = self.to_produce * self.partition_count // self.message_size
        KgoVerifierProducer.oneshot(self.test_context,
                                    self.redpanda,
                                    self.topic,
                                    self.message_size,
                                    msg_count,
                                    batch_max_bytes=self.message_size * 8,
                                    timeout_sec=60)

        quiesce_uploads(self.redpanda, [self.topic], timeout_sec=60)

        def all_partitions_spilled():
            bucket = BucketView(self.redpanda)
            for pid in range(self.partition_count):
                spillover_metas = bucket.get_spillover_metadata(
                    NTP(ns="kafka", topic=self.topic, partition=pid))

                if len(spillover_metas) == 0:
                    self.logger.debug(f"{self.topic}/{pid} did not spill yet")
                    return False

            return True

        wait_until(all_partitions_spilled,
                   timeout_sec=60,
                   backoff_sec=10,
                   err_msg="Some or all partitions did not spill")

    def _query_anomalies(self, admin, namespace: str, topic: str,
                         partition: int):
        def query():
            try:
                res = admin.get_cloud_storage_anomalies(namespace=namespace,
                                                        topic=topic,
                                                        partition=partition)
                return True, res
            except HTTPError as ex:
                if ex.response.status_code == 404:
                    return False
                else:
                    raise

        return wait_until_result(query, timeout_sec=5, backoff_sec=1)

    def _collect_anomalies(self) -> dict[NTPR, Anomalies]:
        anomalies_per_ntpr = {}

        admin = Admin(self.redpanda)
        for pid in range(self.partition_count):
            json_anomalies = self._query_anomalies(admin,
                                                   namespace="kafka",
                                                   topic=self.topic,
                                                   partition=pid)

            ntpr, anomalies = Anomalies.from_dict(json_anomalies)
            anomalies_per_ntpr[ntpr] = anomalies

        return anomalies_per_ntpr

    def _is_subset_for_all(self, superset: dict[NTPR, Anomalies],
                           subset: dict[NTPR, Anomalies]):
        self.logger.debug(f"Checking {subset=} is in {superset=}")

        for ntpr, ntpr_subset in subset.items():
            if not ntpr_subset.is_subset_of(superset[ntpr]):
                return False

        return True

    def _assert_no_anomalies(self):
        anomalies_per_ntpr = self._collect_anomalies()
        for ntpr, anomalies in anomalies_per_ntpr.items():
            assert anomalies.is_empty(
            ), f"{ntpr} reported unexpected anomalies: {anomalies}"

    def _delete_segment_and_await_anomaly(
            self, expected_anomalies: DefaultDict[NTPR, Anomalies]):
        segment_metas = [
            meta for meta in self.cloud_storage_client.list_objects(
                self.bucket_name) if "log" in meta.key
        ]

        view = BucketView(self.redpanda)
        attempts = 1
        while True:
            to_delete = random.choice(segment_metas)

            if seg_meta := view.find_segment_in_manifests(to_delete):
                ntpr = parse_s3_segment_path(to_delete.key).ntpr
                selected_range = OffsetRange(
                    first_inclusive=seg_meta["base_offset"],
                    last_inclusive=seg_meta["committed_offset"])

                impacted_ranges = expected_anomalies[
                    ntpr].get_impacted_offset_ranges(ntpr, view)
                if not overlaps_with_any(selected_range, impacted_ranges):
                    break

            attempts += 1
            assert attempts < 100, "Too many attempts to find a segment to delete"

        self.logger.info(f"Deleting segment at {to_delete.key}")
        self.cloud_storage_client.delete_object(self.bucket_name,
                                                to_delete.key,
                                                verify=True)

        segment_name_components = parse_s3_segment_path(to_delete.key)
        ntpr = segment_name_components.ntpr

        self.logger.info(
            f"Waiting for missing segment anomaly {to_delete.key} to be reported"
        )

        def ntpr_missing_segment():
            anomalies_per_ntpr = self._collect_anomalies()
            self.logger.debug(f"Reported anomalies {anomalies_per_ntpr}")

            if ntpr not in anomalies_per_ntpr:
                return False

            found = to_delete.key in anomalies_per_ntpr[ntpr].missing_segments
            return found, to_delete.key

        missing_seg = wait_until_result(
            ntpr_missing_segment,
            timeout_sec=self.scrub_timeout,
            backoff_sec=2,
            err_msg="Missing segment anomaly not reported")

        expected_anomalies[ntpr].missing_segments.add(missing_seg)
        expected_offset = segment_name_components.base_offset + 1
        self.expected_error_logs.append(
            re.compile(
                fr"Can't apply spillover_cmd.*base_offset: {expected_offset}"))

    def _delete_spillover_manifest_and_await_anomaly(
            self, expected_anomalies: DefaultDict[NTPR, Anomalies]):
        attempts = 1
        to_delete = None

        while to_delete is None:
            pid = random.randint(0, 2)
            view = BucketView(self.redpanda)
            spillover_metas = view.get_spillover_metadata(
                NTP(ns="kafka", topic=self.topic, partition=pid))

            to_delete = random.choice(spillover_metas)
            ntpr = to_delete.ntpr

            selected_range = OffsetRange(first_inclusive=to_delete.base,
                                         last_inclusive=to_delete.last)
            impacted_ranges = expected_anomalies[
                ntpr].get_impacted_offset_ranges(ntpr, view)
            if overlaps_with_any(selected_range, impacted_ranges):
                to_delete = None
            else:
                break

            attempts += 1
            assert attempts < 100, "Too many attempts to find a spillover manifest to delete"

        self.logger.info(f"Deleting spillover manifest at {to_delete.path}")
        self.cloud_storage_client.delete_object(self.bucket_name,
                                                to_delete.path,
                                                verify=True)

        self.logger.info(
            f"Waiting for missing spillover anomaly {to_delete.path} to be reported"
        )

        def ntpr_missing_spill_manifest():
            anomalies_per_ntpr = self._collect_anomalies()
            self.logger.debug(f"Reported anomalies {anomalies_per_ntpr}")

            if ntpr not in anomalies_per_ntpr:
                return False

            found = to_delete.path in anomalies_per_ntpr[
                ntpr].missing_spillover_manifests
            return found, to_delete.path

        missing_spill = wait_until_result(
            ntpr_missing_spill_manifest,
            timeout_sec=self.scrub_timeout,
            backoff_sec=2,
            err_msg="Missing spillover manifest anomaly not reported")

        expected_anomalies[ntpr].missing_spillover_manifests.add(missing_spill)

    def _assert_anomalies_stable_after_leader_shuffle(
            self, expected_anomalies: DefaultDict[NTPR, Anomalies]):
        self.logger.info(
            f"Checking anomalies stay stable after leader shuffle. "
            f"Expected subset: {expected_anomalies}")

        old_anomalies = self._collect_anomalies()
        assert any([
            a is not None for a in old_anomalies.values()
        ]), f"Expected anomalies on at least one ntpr, but got {old_anomalies}"

        admin = Admin(self.redpanda)
        for pid in range(self.partition_count):
            admin.transfer_leadership_to(namespace="kafka",
                                         topic=self.topic,
                                         partition=pid)
            admin.await_stable_leader(namespace='kafka',
                                      topic=self.topic,
                                      partition=pid)

        def anomalies_stable():
            anomalies = self._collect_anomalies()
            self.logger.debug(f"Reported anomalies {anomalies}")

            return self._is_subset_for_all(superset=anomalies,
                                           subset=expected_anomalies)

        wait_until(
            anomalies_stable,
            timeout_sec=self.scrub_timeout,
            backoff_sec=2,
            err_msg="Reported anomalies changed after leadership shuffle")

    def _assert_anomalies_stable_after_restart(
            self, expected_anomalies: DefaultDict[NTPR, Anomalies]):
        self.logger.info(
            "Checking anomalies stay stable after full cluster restart")

        old_anomalies = self._collect_anomalies()
        assert any([
            a is not None for a in old_anomalies.values()
        ]), f"Expected anomalies on at least one ntpr, but got {old_anomalies}"

        for node in self.redpanda.nodes:
            self.redpanda.stop_node(node)

        for node in self.redpanda.nodes:
            self.redpanda.start_node(node)

        def anomalies_stable():
            anomalies = self._collect_anomalies()
            self.logger.debug(f"Reported anomalies {anomalies}")

            return self._is_subset_for_all(superset=anomalies,
                                           subset=expected_anomalies)

        wait_until(anomalies_stable,
                   timeout_sec=self.scrub_timeout,
                   backoff_sec=2,
                   err_msg="Reported anomalies changed after full restart")

    def _assert_segment_metadata_anomalies(
            self, expected_anomalies: DefaultDict[NTPR, Anomalies]):
        self.logger.info(
            "Fudging manifest and waiting on segment metadata anomalies")

        view = BucketView(self.redpanda)
        manifest = view.manifest_for_ntp(topic=self.topic, partition=0)

        sorted_segments = sorted(manifest['segments'].items(),
                                 key=lambda entry: entry[1]['base_offset'])
        assert len(
            sorted_segments
        ) > 2, f"Not enough segments in manifest: {json.dumps(manifest)}"

        # Remove the metadata for the penultimate segment, thus creating an offset gap
        # for the scrubber to detect
        found = False
        seg_to_remove_name = None
        seg_to_remove_meta = None
        detect_at_seg_meta = None
        for crnt_seg, next_seg in itertools.pairwise(sorted_segments):
            name, meta = crnt_seg

            ntpr = NTPR(ns="kafka",
                        topic=self.topic,
                        partition=0,
                        revision=meta["ntp_revision"])
            seg_to_remove_name = name
            seg_to_remove_meta = meta
            detect_at_seg_meta = next_seg[1]

            offset_range = OffsetRange(first_inclusive=meta["base_offset"],
                                       last_inclusive=meta["committed_offset"])
            impacted_ranges = expected_anomalies[
                ntpr].get_impacted_offset_ranges(ntpr, view)

            if not overlaps_with_any(offset_range, impacted_ranges):
                found = True
                break

        assert found, "Could not find a segment to remove from the STM manifest"

        self.logger.info(f"Removing segment with meta {seg_to_remove_meta}")
        self.logger.info(f"Anomaly should be detected at {detect_at_seg_meta}")
        manifest['segments'].pop(seg_to_remove_name)
        """
        The operation above may remove the first segment in the manifest. This
        causes a difference between the manifest start offset and the first
        segment start offset. The mismatch causes a validation to fail when
        spillover is applied (which is correct behavior) by archival STM.

        To avoid the test failing due to this expected set of events, the error
        log is added to accept list here.
        """
        sorted_segments = sorted(manifest['segments'].items(),
                                 key=lambda entry: entry[1]['base_offset'])
        expect_error_at_offset = sorted_segments[0][1]['base_offset']
        self.expected_error_logs.append(
            re.compile(
                fr"Can't apply spillover_cmd.*base_offset: {expect_error_at_offset}"
            ))

        # Forcefully reset the manifest. Remote writes are disabled first
        # to make this operation safe.
        json_man = json.dumps(manifest)
        self.logger.info(f"Re-setting manifest to:{json_man}")

        self.rpk.alter_topic_config(self.topic, 'redpanda.remote.write',
                                    'false')
        time.sleep(1)

        admin = Admin(self.redpanda)
        admin.unsafe_reset_cloud_metadata(self.topic, 0, manifest)

        self.rpk.alter_topic_config(self.topic, 'redpanda.remote.write',
                                    'true')

        def gap_reported():
            anomalies_per_ntpr = self._collect_anomalies()
            self.logger.debug(f"Reported anomalies {anomalies_per_ntpr}")

            seg_meta_anomalies = anomalies_per_ntpr[
                ntpr].segment_metadata_anomalies

            for meta in seg_meta_anomalies:
                if (meta.anomaly_type == "offset_gap"
                        and meta.at_segment.base_offset
                        == detect_at_seg_meta["base_offset"]):
                    return True, meta

        meta_anomaly = wait_until_result(
            gap_reported,
            # A new manifest needs to be uploaded, so the timeout is more generous
            timeout_sec=self.scrub_timeout + 10,
            backoff_sec=2,
            err_msg="Gap not reported")

        expected_anomalies[ntpr].segment_metadata_anomalies.add(meta_anomaly)

    def _assert_metrics_updated(self):
        metrics = [
            self.redpanda.metrics(n, MetricsEndpoint.PUBLIC_METRICS)
            for n in self.redpanda.nodes
        ]

        missing_segments = 0
        missing_spills = 0
        offset_gaps = 0

        def validate_labels(labels):
            label_validators = {
                "redpanda_namespace": [lambda val: val == "kafka"],
                "redpanda_topic": [],
                "redpanda_type": [],
                "redpanda_severity":
                [lambda val: val == "high" or val == "low"]
            }

            for name, validators in label_validators.items():
                assert name in labels, f"Label {name} not found in {labels}"
                for v in validators:
                    assert v(labels[name]
                             ), f"Validator for {name}={labels[name]} failed"

            expected_labels = set(label_validators.keys())
            found_labels = set(labels.keys())
            assert expected_labels == found_labels, f"Label keys do not match: {expected_labels=} != {found_labels=}"

        for node_metrics in metrics:
            for family in node_metrics:
                for sample in family.samples:
                    if "redpanda_type" in sample.labels and "redpanda_severity" in sample.labels:
                        self.logger.debug(f"anomaly_metric_sample: {sample}")
                        validate_labels(sample.labels)

                        anomaly_type = sample.labels["redpanda_type"]
                        if anomaly_type == "missing_segments":
                            missing_segments += int(sample.value)
                        elif anomaly_type == "missing_spillover_manifests":
                            missing_spills += int(sample.value)
                        elif anomaly_type == "offset_gaps":
                            offset_gaps += int(sample.value)

        self.logger.info(
            f"Metrics reported: {missing_segments=}, {missing_spills=} {offset_gaps=}"
        )

        assert missing_segments == 1, "No missing segments repoted via metrics"
        assert missing_spills == 1, "No missing spillovers repoted via metrics"
        assert offset_gaps >= 1, "No offset gaps reported via metrics"

    @cluster(num_nodes=4,
             log_allow_list=SCRUBBER_LOG_ALLOW_LIST +
             [AllowLogsOnPredicate(method="match_missing_segment")])
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_scrubber(self, cloud_storage_type):
        """
        Test the internal cloud storage scrubber. Various anomalies are introduced
        by removing files from cloud storage and erroneously force updating the partition
        manifest. We then verify that the scrubber detects the issues and that the set of
        issues remains stable.
        """
        expected_anomalies = collections.defaultdict(Anomalies.make_empty)

        self._produce()
        self._assert_no_anomalies()

        self._delete_spillover_manifest_and_await_anomaly(expected_anomalies)
        self._delete_segment_and_await_anomaly(expected_anomalies)

        self._assert_segment_metadata_anomalies(expected_anomalies)

        self._assert_anomalies_stable_after_leader_shuffle(expected_anomalies)
        self._assert_anomalies_stable_after_restart(expected_anomalies)

        self._assert_metrics_updated()

        # This test deletes segments, spillover manifests
        # and fudges the manifest. rp-storage-tool also picks
        # up on some of these things.
        self.redpanda.si_settings.set_expected_damage({
            "missing_segments", "metadata_offset_gaps",
            "missing_spillover_manifests"
        })

    def match_missing_segment(self, log_entry: str) -> bool:
        return any(expr.search(log_entry) for expr in self.expected_error_logs)
