# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.redpanda import SISettings, get_cloud_storage_type
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.si_utils import BucketView, ClusterMetadata, parse_controller_snapshot_path


def check_cluster_metadata_is_consistent(s3_snapshot: BucketView):
    """
    Takes the given snapshot and ensures that the highest manifest available
    (that would be used in a recovery) only references objects that exist in
    the bucket.
    """
    latest_cluster_metadata = ClusterMetadata(dict(), dict())
    highest_meta_id = -1
    cluster_uuid = ""
    for uuid, meta in s3_snapshot.cluster_metadata.items():
        for meta_id, _ in meta.cluster_metadata_manifests.items():
            if meta_id > highest_meta_id:
                latest_cluster_metadata = meta
                cluster_uuid = uuid
                highest_meta_id = meta_id
    highest_manifest = latest_cluster_metadata.cluster_metadata_manifests[
        highest_meta_id]
    assert 'controller_snapshot_path' in highest_manifest, highest_manifest
    controller_snapshot_path = highest_manifest['controller_snapshot_path']
    assert len(controller_snapshot_path) > 0, highest_manifest

    _, snap_offset = parse_controller_snapshot_path(controller_snapshot_path)
    assert snap_offset in latest_cluster_metadata.controller_snapshot_sizes, \
        f"Couldn't find {snap_offset} in {latest_cluster_metadata.controller_snapshot_sizes}"
    assert latest_cluster_metadata.controller_snapshot_sizes[snap_offset] > 0
    return cluster_uuid, highest_meta_id


class ClusterMetadataUploadTest(RedpandaTest):
    topic_name = "oolong"
    topics = (TopicSpec(name=topic_name, partition_count=10), )
    segment_size = 1024 * 1024

    def __init__(self, test_context):
        si_settings = SISettings(test_context,
                                 log_segment_size=self.segment_size,
                                 fast_uploads=True)
        extra_rp_conf = {
            'controller_snapshot_max_age_sec': 1,
            'cloud_storage_cluster_metadata_upload_interval_ms': 1000,
            'enable_cluster_metadata_upload_loop': True,
        }
        super().__init__(test_context,
                         si_settings=si_settings,
                         num_brokers=3,
                         extra_rp_conf=extra_rp_conf)

    def bucket_has_metadata(self, at_least: int):
        """
        Returns true if at least some metadata exist in the bucket.
        As new metadata is written, the upload loop may be cleaning up after
        itself, so ignores exceptions.
        """
        try:
            s3_snapshot = BucketView(self.redpanda, topics=self.topics)
            num_cluster_metas = 0
            for _, meta in s3_snapshot.cluster_metadata.items():
                if len(meta.cluster_metadata_manifests) > 0 and len(
                        meta.controller_snapshot_sizes) > 0:
                    num_cluster_metas += 1
            return num_cluster_metas >= at_least
        except Exception as e:
            self.logger.warn(f"Error while populating snapshot: {str(e)}")
            return False

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_uploads_after_restart(self, cloud_storage_type):
        """
        Ensure that metadata uploads proceed after restarting, upholding the
        invariant that manifest IDs are monotonically increasing.
        """
        self.redpanda._admin.put_feature("controller_snapshots",
                                         {"state": "active"})

        wait_until(lambda: self.bucket_has_metadata(1),
                   timeout_sec=10,
                   backoff_sec=1)

        # Stop the cluster so we can check consistency of the bucket without
        # Redpanda deleting objects beneath us.
        self.redpanda.stop()
        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        cluster_uuid, orig_highest_manifest_id = check_cluster_metadata_is_consistent(
            s3_snapshot)
        orig_lowest_manifest_id = \
            min(s3_snapshot.cluster_metadata[cluster_uuid].cluster_metadata_manifests.items())

        self.redpanda.restart_nodes(self.redpanda.nodes)

        # Wait for the metadata ID to increase, as metadata is written by a
        # new leader. As we wait, make sure some other invariants are true:
        # - the metadata ID shouldn't go down
        # - the highest manifest should always point to existing files
        def meta_id_has_grown(by_at_least: int):
            try:
                # Since Redpanda is running and deleting old metadata, it's
                # possible that listing the bucket and parsing everything don't
                # quite match up. Ignore errors until we parse the snapshot
                # successfully without any NoSuchKey errors.
                s3_snapshot = BucketView(self.redpanda, topics=self.topics)
                s3_snapshot._ensure_listing()
            except Exception as e:
                self.logger.warn(f"Error while populating snapshot: {str(e)}")
                return False

            new_lowest_manifest_id = \
                min(s3_snapshot.cluster_metadata[cluster_uuid].cluster_metadata_manifests.items())
            assert new_lowest_manifest_id >= orig_lowest_manifest_id, \
                f"Cluster metadata manifest ID went down from {orig_lowest_manifest_id} to " \
                "{new_lowest_manifest_id}: {s3_snapshot}"
            _, new_highest_manifest_id = check_cluster_metadata_is_consistent(
                s3_snapshot)
            return new_highest_manifest_id > orig_highest_manifest_id + by_at_least

        wait_until(lambda: meta_id_has_grown(5),
                   timeout_sec=10,
                   backoff_sec=0.1)
        # The topic manifest may not have been uploaded yet, but that's fine
        # since this test focuses on cluster metadata only.
        self.redpanda.si_settings.set_expected_damage(
            {"ntr_no_topic_manifest"})

    @cluster(num_nodes=3)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_uploads_after_wipe(self, cloud_storage_type):
        """
        Ensure that metadata uploads proceed after a cluster wipe, upholding
        the invariant that manifest IDs are monotonically increasing, even for
        the new cluster.
        """
        admin = self.redpanda._admin
        admin.put_feature("controller_snapshots", {"state": "active"})
        wait_until(lambda: self.bucket_has_metadata(1),
                   timeout_sec=10,
                   backoff_sec=1)
        orig_cluster_uuid_resp: str = admin.get_cluster_uuid(
            self.redpanda.nodes[0])

        # Wipe the directory away, simulating a full cluster outage.
        self.redpanda.stop()
        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        orig_cluster_uuid, orig_highest_manifest_id = \
            check_cluster_metadata_is_consistent(s3_snapshot)
        assert orig_cluster_uuid == orig_cluster_uuid_resp, \
            f"{orig_cluster_uuid_resp} vs {orig_cluster_uuid}"
        for n in self.redpanda.nodes:
            self.redpanda.remove_local_data(n)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        self.redpanda._admin.put_feature("controller_snapshots",
                                         {"state": "active"})

        # The new cluster should begin uploading new metadata without clearing
        # out the metadata from the old cluster.
        wait_until(lambda: self.bucket_has_metadata(2),
                   timeout_sec=10,
                   backoff_sec=1)
        self.redpanda.stop()

        # The new cluster should upload starting at a higher metadata ID than
        # that used by the first cluster.
        s3_snapshot = BucketView(self.redpanda, topics=self.topics)
        new_cluster_uuid, new_highest_manifest_id = check_cluster_metadata_is_consistent(
            s3_snapshot)
        assert new_cluster_uuid != orig_cluster_uuid

        assert new_highest_manifest_id > orig_highest_manifest_id, \
            f"{new_highest_manifest_id} vs {orig_highest_manifest_id}"

        # The topic manifest may not have been uploaded yet, but that's fine
        # since this test focuses on cluster metadata only.
        self.redpanda.si_settings.set_expected_damage(
            {"ntr_no_topic_manifest"})
