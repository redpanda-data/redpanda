#Copyright 2023 Redpanda Data, Inc.
#
#Licensed as a Redpanda Enterprise file under the Redpanda Community
#License(the "License"); you may not use this file except in compliance with
#the License.You may obtain a copy of the License at
#
#https: // github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import SISettings, get_cloud_storage_type
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.utils.si_utils import parse_s3_segment_path, quiesce_uploads, BucketView, NTP, NTPR

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

import json
import random
import time

#Attempts to compute the retention point for the cloud log will fail
#after a spillover manifest is manually removed.
SCRUBBER_LOG_ALLOW_LIST = [
    r"cloud_storage - .* failed to download manifest {key_not_found}",
    r"cloud_storage - .* failed to download manifest.*cloud_storage::error_outcome:2",
    r"cloud_storage - .* Failed to materialize.*cloud_storage::error_outcome:2",
    r"cloud_storage - .* failed to seek.*cloud_storage::error_outcome:2",
    r"cloud_storage - .* Failed to compute time-based retention",
    r"cloud_storage - .* Failed to compute size-based retention",
    r"cloud_storage - .* Failed to compute retention",
]


class CloudStorageScrubberTest(RedpandaTest):
    scrub_timeout = 30
    partition_count = 3
    message_size = 16 * 1024  # 16KiB
    segment_size = 1024 * 1024  # 1MiB
    to_produce = 30 * 1024 * 1024  # 100MiB per partition
    topics = [TopicSpec(partition_count=partition_count)]

    def __init__(self, test_context):
        self.si_settings = SISettings(
            test_context,
            log_segment_size=self.segment_size,
            cloud_storage_spillover_manifest_max_segments=5,
            cloud_storage_housekeeping_interval_ms=1000 * 30,
            fast_uploads=True)

        super().__init__(test_context=test_context,
                         extra_rp_conf={
                             "cloud_storage_enable_scrubbing":
                             True,
                             "cloud_storage_scrubbing_interval_ms":
                             10 * 1000,
                             "cloud_storage_scrubbing_interval_jitter_ms":
                             5 * 1000,
                         },
                         si_settings=self.si_settings)

        self.bucket_name = self.si_settings.cloud_storage_bucket
        self.rpk = RpkTool(self.redpanda)

    def _produce(self):
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

    def _collect_anomalies(self):
        anomalies_per_ntpr = {}

        admin = Admin(self.redpanda)
        for pid in range(self.partition_count):
            anomalies = admin.get_cloud_storage_anomalies(namespace="kafka",
                                                          topic=self.topic,
                                                          partition=pid)

            ntpr = NTPR(ns=anomalies["ns"],
                        topic=anomalies["topic"],
                        partition=anomalies["partition"],
                        revision=anomalies["revision_id"])

            if ("missing_partition_manifest" not in anomalies
                    and "missing_spillover_manifests" not in anomalies
                    and "missing_segments" not in anomalies
                    and "segment_metadata_anomalies" not in anomalies):
                anomalies = None

            anomalies_per_ntpr[ntpr] = anomalies

        return anomalies_per_ntpr

    def _assert_no_anomalies(self):
        anomalies_per_ntpr = self._collect_anomalies()
        for ntpr, anomalies in anomalies_per_ntpr.items():
            assert anomalies is None, f"{ntpr} reported unexpected anomalies: {anomalies}"

    def _delete_segment_and_await_anomaly(self):
        segment_metas = [
            meta for meta in self.cloud_storage_client.list_objects(
                self.bucket_name) if "log" in meta.key
        ]

        view = BucketView(self.redpanda)
        to_delete = random.choice(segment_metas)
        attempts = 1
        while view.is_segment_part_of_a_manifest(to_delete) == False:
            to_delete = random.choice(segment_metas)
            attempts += 1

            assert attempts < 100, "Too many attempts to find a segment to delete"

        self.logger.info(f"Deleting segment at {to_delete.key}")
        self.cloud_storage_client.delete_object(self.bucket_name,
                                                to_delete.key,
                                                verify=True)

        ntpr = parse_s3_segment_path(to_delete.key).ntpr

        self.logger.info(
            f"Waiting for missing segment anomaly {to_delete.key} to be reported"
        )

        def ntpr_missing_segment():
            anomalies_per_ntpr = self._collect_anomalies()
            self.logger.debug(f"Reported anomalies {anomalies_per_ntpr}")

            if ntpr not in anomalies_per_ntpr:
                return False

            if anomalies_per_ntpr[ntpr] is None:
                return False

            if "missing_segments" not in anomalies_per_ntpr[ntpr]:
                return False

            return to_delete.key in anomalies_per_ntpr[ntpr][
                "missing_segments"]

        wait_until(ntpr_missing_segment,
                   timeout_sec=self.scrub_timeout,
                   backoff_sec=2,
                   err_msg="Missing segment anomaly not reported")

    def _delete_spillover_manifest_and_await_anomaly(self):
        pid = random.randint(0, 2)
        view = BucketView(self.redpanda)
        spillover_metas = view.get_spillover_metadata(
            NTP(ns="kafka", topic=self.topic, partition=pid))
        to_delete = random.choice(spillover_metas)
        ntpr = to_delete.ntpr

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

            if anomalies_per_ntpr[ntpr] is None:
                return False

            if "missing_spillover_manifests" not in anomalies_per_ntpr[ntpr]:
                return False

            return to_delete.path in anomalies_per_ntpr[ntpr][
                "missing_spillover_manifests"]

        wait_until(ntpr_missing_spill_manifest,
                   timeout_sec=self.scrub_timeout,
                   backoff_sec=2,
                   err_msg="Missing spillover manifest anomaly not reported")

    def _assert_anomalies_stable_after_leader_shuffle(self):
        self.logger.info("Checking anomalies stay stable after leader shuffle")

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

            return anomalies == old_anomalies

        wait_until(
            anomalies_stable,
            timeout_sec=self.scrub_timeout,
            backoff_sec=2,
            err_msg="Reported anomalies changed after leadership shuffle")

    def _assert_anomalies_stable_after_restart(self):
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

            return anomalies == old_anomalies

        wait_until(anomalies_stable,
                   timeout_sec=self.scrub_timeout,
                   backoff_sec=2,
                   err_msg="Reported anomalies changed after full restart")

    def _assert_segment_metadata_anomalies(self):
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
        seg_to_remove_name, seg_to_remove_meta = sorted_segments[-2]
        manifest['segments'].pop(seg_to_remove_name)
        self.logger.info(f"Removing segment with meta {seg_to_remove_meta}")

        detect_at_seg_meta = sorted_segments[-1][1]
        self.logger.info(f"Anomaly should be detected at {detect_at_seg_meta}")

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

        ntpr = NTPR(ns="kafka",
                    topic=self.topic,
                    partition=0,
                    revision=seg_to_remove_meta["ntp_revision"])

        def gap_reported():
            anomalies_per_ntpr = self._collect_anomalies()
            self.logger.debug(f"Reported anomalies {anomalies_per_ntpr}")

            if ntpr not in anomalies_per_ntpr:
                return False

            if anomalies_per_ntpr[ntpr] is None:
                return False

            if "segment_metadata_anomalies" not in anomalies_per_ntpr[ntpr]:
                return False

            seg_meta_anomalies = anomalies_per_ntpr[ntpr][
                "segment_metadata_anomalies"]

            for meta in seg_meta_anomalies:
                if (meta["type"] == "offset_gap"
                        and meta["at_segment"]["base_offset"]
                        == detect_at_seg_meta["base_offset"]):
                    return True

        wait_until(
            gap_reported,
            # A new manifest needs to be uploaded, so the timeout is more generous
            timeout_sec=self.scrub_timeout + 10,
            backoff_sec=2,
            err_msg="Gap not reported")

    @cluster(num_nodes=4, log_allow_list=SCRUBBER_LOG_ALLOW_LIST)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_scrubber(self, cloud_storage_type):
        self._produce()
        self._assert_no_anomalies()
        self._delete_spillover_manifest_and_await_anomaly()
        self._delete_segment_and_await_anomaly()

        self._assert_segment_metadata_anomalies()

        self._assert_anomalies_stable_after_leader_shuffle()
        self._assert_anomalies_stable_after_restart()

        # This test deletes segments, spillover manifests
        # and fudges the manifest. rp-storage-tool also picks
        # up on some of these things.
        self.redpanda.si_settings.set_expected_damage(
            {"missing_segments", "metadata_offset_gaps"})
