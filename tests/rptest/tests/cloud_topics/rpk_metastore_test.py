# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin as AdminV1
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.cloud_topics.e2e_test import EndToEndCloudTopicsBase
import rptest.tests.cloud_topics.utils as ct_utils


class RpkMetastoreTest(EndToEndCloudTopicsBase):
    """End-to-end coverage for `rpk debug cloud-topics metastore`.

    Produces to a cloud topic and waits for the data to reconcile into L1,
    which writes metadata rows into the L1 metastore. Then drives the `layout`
    and `files` subcommands against the admin API and asserts their structured
    output is well-formed and consistent with the metastore partition count.

    These commands report the metastore database's own LSM structure (its
    memtable plus on-disk SST files), so SST files only appear once the
    metastore flushes its memtable; the test exercises the full command path
    regardless and validates any files that are present.
    """

    topic_name = "rpk_metastore_test"
    # Create our own single-partition cloud topic instead of the base topics.
    topics = ()

    def __init__(self, test_context):
        super().__init__(test_context=test_context)
        assert self.redpanda
        self.admin_v1 = AdminV1(self.redpanda)

    def setUp(self):
        super().setUp()
        self.rpk.create_topic(
            topic=self.topic_name,
            partitions=1,
            replicas=3,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD,
            },
        )

    def _num_metastore_partitions(self) -> int:
        cfg = self.admin_v1.get_cluster_config(include_defaults=True)
        return int(cfg["cloud_topics_num_metastore_partitions"])

    @staticmethod
    def _partition_bytes(p: dict[str, Any]) -> int:
        return (
            int(p.get("activeMemtableBytes", 0))
            + int(p.get("immutableMemtableBytes", 0))
            + int(p.get("totalSizeBytes", 0))
        )

    @cluster(num_nodes=4)
    def test_layout_and_files(self):
        assert self.redpanda
        topic, partition = self.topic_name, 0

        # Produce, then wait for the data to land in L1 and fully reconcile so
        # the metastore holds metadata rows for this partition.
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=16384,
            msg_count=2048,  # ~32 MiB
        )
        ct_utils.wait_until_l1_partition_size(
            self.admin, topic, partition, lambda size: size > 0, timeout_sec=120
        )
        self.wait_until_reconciled(topic=topic, partition=partition)

        num_ms = self._num_metastore_partitions()

        # --- layout: one error-free row per metastore partition ---
        layout = self.rpk.debug_cloud_topics_metastore_layout()
        assert len(layout) == num_ms, f"expected {num_ms} partitions, got {layout}"
        for p in layout:
            assert "error" not in p, (
                f"partition {p['partition']} errored: {p.get('error')}"
            )
        seen = sorted(p["partition"] for p in layout)
        assert seen == list(range(num_ms)), seen

        # Having reconciled rows, the metastore must report non-empty state on
        # at least one partition (memtable bytes, or on-disk size once flushed).
        assert any(self._partition_bytes(p) > 0 for p in layout), (
            f"metastore reported empty after reconcile: {layout}"
        )

        # --- files: same per-partition shape as layout ---
        files = self.rpk.debug_cloud_topics_metastore_files()
        assert {p["partition"] for p in files} == set(seen)

        # A Go nil slice marshals to JSON null (not []), so a level with no
        # SST files arrives as "files": null; `or []` normalizes both.
        total_files = sum(
            len(level.get("files") or [])
            for p in files
            for level in (p.get("levels") or [])
        )
        self.logger.info(f"metastore SST files observed: {total_files}")
        for p in files:
            for level in p.get("levels") or []:
                for f in level.get("files") or []:
                    assert int(f["sizeBytes"]) > 0, f
                    assert f["smallestKeyInfo"], f
                    assert f["largestKeyInfo"], f

        # --- --level filter only returns the requested level ---
        files_l0 = self.rpk.debug_cloud_topics_metastore_files(level=0)
        for p in files_l0:
            for level in p.get("levels") or []:
                assert level["levelNumber"] == 0, level

        # --- --partition filter scopes to a single metastore partition ---
        one = self.rpk.debug_cloud_topics_metastore_layout(partition=0)
        assert [p["partition"] for p in one] == [0], one
