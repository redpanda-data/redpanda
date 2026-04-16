# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import uuid
from dataclasses import dataclass, field
from typing import Any, NamedTuple

from rptest.clients.admin.v2 import metastore_pb
from ducktape.mark import matrix
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.cloud_topics.e2e_test import EndToEndCloudTopicsBase

from connectrpc.errors import ConnectError


class PartitionAndRow(NamedTuple):
    metastore_partition: int
    row: metastore_pb.ReadRow


AT = metastore_pb.AnomalyType


@dataclass
class CorruptionScenario:
    """A single corruption to inject and validate."""

    name: str
    expected_anomalies: set[int]
    # Rows to upsert and keys to delete (computed by the test).
    writes: list[metastore_pb.WriteRow] = field(
        default_factory=lambda: list[metastore_pb.WriteRow]()
    )
    deletes: list[metastore_pb.RowKey] = field(
        default_factory=lambda: list[metastore_pb.RowKey]()
    )
    check_object_metadata: bool = False


INT32_MAX: int = (2**31) - 1
INT64_MAX: int = (2**63) - 1


class DebugRowsTest(EndToEndCloudTopicsBase):
    topics = (
        TopicSpec(
            name="panda_topic",
            partition_count=1,
            replication_factor=3,
        ),
    )

    def _read_rows(
        self,
        metastore_partition: int,
        seek_key: metastore_pb.RowKey | None = None,
        last_key: metastore_pb.RowKey | None = None,
    ) -> list[metastore_pb.ReadRow]:
        """Paginated read of rows from a single metastore partition,
        optionally bounded by seek_key and last_key."""
        metastore = self.admin.metastore()
        all_rows: list[metastore_pb.ReadRow] = []
        next_key: str | None = None
        while True:
            req_kwargs: dict[str, Any] = dict(
                metastore_partition=metastore_partition,
                max_rows=100,
            )
            if next_key is not None:
                req_kwargs["raw_seek_key"] = next_key
            elif seek_key is not None:
                req_kwargs["seek_key"] = seek_key
            if last_key is not None:
                req_kwargs["last_key"] = last_key
            resp = metastore.read_rows(req=metastore_pb.ReadRowsRequest(**req_kwargs))
            all_rows.extend(resp.rows)
            if not resp.next_key:
                break
            next_key = resp.next_key
        return all_rows

    def read_all_rows(self) -> list[PartitionAndRow]:
        """Read all rows across all metastore partitions."""
        all_rows: list[PartitionAndRow] = []
        for mp in range(3):
            try:
                for row in self._read_rows(mp):
                    all_rows.append(PartitionAndRow(mp, row))
            except ConnectError:
                continue
        return all_rows

    def read_metadata(
        self,
        metastore_partition: int,
        topic_id: str,
        partition_id: int,
    ) -> metastore_pb.ReadRow | None:
        key = metastore_pb.RowKey(
            metadata=metastore_pb.MetadataKey(
                topic_id=topic_id, partition_id=partition_id
            )
        )
        rows: list[metastore_pb.ReadRow] = self._read_rows(
            metastore_partition, seek_key=key, last_key=key
        )
        return rows[0] if rows else None

    def read_extents(
        self,
        metastore_partition: int,
        topic_id: str,
        partition_id: int,
    ) -> list[metastore_pb.ReadRow]:
        seek = metastore_pb.RowKey(
            extent=metastore_pb.ExtentKey(
                topic_id=topic_id, partition_id=partition_id, base_offset=0
            )
        )
        last = metastore_pb.RowKey(
            extent=metastore_pb.ExtentKey(
                topic_id=topic_id, partition_id=partition_id, base_offset=INT64_MAX
            )
        )
        return self._read_rows(metastore_partition, seek_key=seek, last_key=last)

    def read_terms(
        self,
        metastore_partition: int,
        topic_id: str,
        partition_id: int,
    ) -> list[metastore_pb.ReadRow]:
        seek = metastore_pb.RowKey(
            term=metastore_pb.TermKey(
                topic_id=topic_id, partition_id=partition_id, term_id=0
            )
        )
        last = metastore_pb.RowKey(
            term=metastore_pb.TermKey(
                topic_id=topic_id, partition_id=partition_id, term_id=INT64_MAX
            )
        )
        return self._read_rows(metastore_partition, seek_key=seek, last_key=last)

    def read_objects(
        self,
        metastore_partition: int,
    ) -> list[metastore_pb.ReadRow]:
        seek = metastore_pb.RowKey(
            object=metastore_pb.ObjectKey(
                object_id="00000000-0000-0000-0000-000000000000"
            )
        )
        last = metastore_pb.RowKey(
            object=metastore_pb.ObjectKey(
                object_id="ffffffff-ffff-ffff-ffff-ffffffffffff"
            )
        )
        return self._read_rows(metastore_partition, seek_key=seek, last_key=last)

    @cluster(num_nodes=4)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_TIERED_CLOUD,
        ],
    )
    def test_read_and_write_rows(self, storage_mode: str) -> None:
        topic: TopicSpec = self.topics[0]
        self.start_producer(num_nodes=1)  # type: ignore[reportUnknownMemberType]
        self.await_num_produced(min_records=5000, timeout_sec=120)  # type: ignore[reportUnknownMemberType]
        self.producer.stop()
        self.wait_until_reconciled(topic=topic.name, partition=0)

        # Read all rows and locate the metadata row.
        rows: list[PartitionAndRow] = self.read_all_rows()
        self.logger.debug(rows)
        metadata_row: PartitionAndRow | None = next(
            (r for r in rows if r.row.key.HasField("metadata")), None
        )
        assert metadata_row is not None, "expected a metadata row after reconciliation"
        md_row: metastore_pb.ReadRow = metadata_row.row
        assert md_row.value.metadata.next_offset > 0

        target_mp: int = metadata_row.metastore_partition
        topic_id: str = md_row.key.metadata.topic_id
        partition_id: int = md_row.key.metadata.partition_id

        # Verify the partition has extents, terms, and objects.
        extents: list[metastore_pb.ReadRow] = self.read_extents(
            target_mp, topic_id, partition_id
        )
        assert len(extents) > 0, "expected at least one extent row"
        for e in extents:
            assert e.value.extent.last_offset >= 0
            assert e.value.extent.object_id != ""

        terms: list[metastore_pb.ReadRow] = self.read_terms(
            target_mp, topic_id, partition_id
        )
        assert len(terms) > 0, "expected at least one term row"

        objects: list[metastore_pb.ReadRow] = self.read_objects(target_mp)
        assert len(objects) > 0, "expected at least one object row"

        original_epoch: int = md_row.value.metadata.compaction_epoch
        new_epoch: int = original_epoch + 1

        # Write back the metadata row with bumped compaction_epoch.
        md_val: metastore_pb.RowValue = metastore_pb.RowValue(
            metadata=metastore_pb.MetadataValue(
                start_offset=md_row.value.metadata.start_offset,
                next_offset=md_row.value.metadata.next_offset,
                compaction_epoch=new_epoch,
                size=md_row.value.metadata.size,
            )
        )
        write_req: metastore_pb.WriteRowsRequest = metastore_pb.WriteRowsRequest(
            metastore_partition=target_mp,
            writes=[metastore_pb.WriteRow(key=md_row.key, value=md_val)],
        )
        write_resp: metastore_pb.WriteRowsResponse = self.admin.metastore().write_rows(
            req=write_req
        )
        assert write_resp.rows_written == 1

        # Re-read the metadata row directly and verify the mutation.
        updated_row = self.read_metadata(target_mp, topic_id, partition_id)
        assert updated_row is not None
        assert updated_row.value.metadata.compaction_epoch == new_epoch, (
            f"expected compaction_epoch={new_epoch}, got {updated_row.value.metadata.compaction_epoch}"
        )

        # Delete all rows and verify they're gone.
        all_keys: list[metastore_pb.RowKey] = [
            r.row.key
            for r in self.read_all_rows()
            if r.metastore_partition == target_mp
        ]
        assert len(all_keys) > 0
        delete_req: metastore_pb.WriteRowsRequest = metastore_pb.WriteRowsRequest(
            metastore_partition=target_mp,
            deletes=all_keys,
        )
        self.admin.metastore().write_rows(req=delete_req)

        assert self.read_metadata(target_mp, topic_id, partition_id) is None
        assert len(self.read_extents(target_mp, topic_id, partition_id)) == 0
        assert len(self.read_terms(target_mp, topic_id, partition_id)) == 0
        assert len(self.read_objects(target_mp)) == 0

    def _validate_partition(
        self,
        topic_id: str,
        partition_id: int,
        check_object_metadata: bool = False,
        max_extents: int = 0,
    ) -> metastore_pb.ValidatePartitionResponse:
        """Run ValidatePartition and return the full response."""
        all_anomalies: list[metastore_pb.MetastoreAnomaly] = []
        extents_total = 0
        resume: int | None = None
        while True:
            req = metastore_pb.ValidatePartitionRequest(
                topic_id=topic_id,
                partition_id=partition_id,
                check_object_metadata=check_object_metadata,
                max_extents=max_extents,
            )
            if resume is not None:
                req.resume_at_offset = resume
            resp = self.admin.metastore().validate_partition(req=req)
            all_anomalies.extend(resp.anomalies)
            extents_total += resp.extents_validated
            if not resp.HasField("resume_at_offset"):
                break
            resume = resp.resume_at_offset

        # Build a synthetic response with aggregated results.
        combined = metastore_pb.ValidatePartitionResponse(
            extents_validated=extents_total,
        )
        combined.anomalies.extend(all_anomalies)
        return combined

    def _write_rows(
        self,
        metastore_partition: int,
        writes: list[metastore_pb.WriteRow] | None = None,
        deletes: list[metastore_pb.RowKey] | None = None,
    ) -> None:
        req = metastore_pb.WriteRowsRequest(
            metastore_partition=metastore_partition,
            writes=writes or [],
            deletes=deletes or [],
        )
        self.admin.metastore().write_rows(req=req)

    def _has_anomaly(
        self,
        resp: metastore_pb.ValidatePartitionResponse,
        anomaly_type: int,
    ) -> bool:
        return any(a.anomaly_type == anomaly_type for a in resp.anomalies)

    def _snapshot_rows(self, mp: int) -> list[metastore_pb.ReadRow]:
        """Snapshot all rows on a metastore partition."""
        return self._read_rows(mp)

    def _restore_snapshot(
        self,
        mp: int,
        snapshot: list[metastore_pb.ReadRow],
    ) -> None:
        """Restore a metastore partition to a previous snapshot.

        Writes back all snapshotted rows and deletes any rows that
        exist now but weren't in the snapshot.
        """
        current = self._read_rows(mp)
        snapshot_keys = {row.key.SerializeToString() for row in snapshot}
        extra_keys = [
            row.key
            for row in current
            if row.key.SerializeToString() not in snapshot_keys
        ]
        self._write_rows(
            mp,
            writes=[
                metastore_pb.WriteRow(key=row.key, value=row.value) for row in snapshot
            ],
            deletes=extra_keys,
        )

    @cluster(num_nodes=4)
    def test_validate_partition(self) -> None:
        """Validate metastore state, then inject corruptions via WriteRows
        and verify the validator detects each one.

        Each scenario is a CorruptionScenario with writes/deletes to apply
        and expected anomaly types. After each scenario, state is restored
        from a snapshot taken before any corruptions.
        """
        self.start_producer(num_nodes=1)  # type: ignore[reportUnknownMemberType]
        self.await_num_produced(min_records=5000, timeout_sec=120)  # type: ignore[reportUnknownMemberType]
        self.producer.stop()
        # Wait for all partitions to fully reconcile L0 -> L1 before
        # snapshotting state, so no background writes race with our
        # corruption/restore cycle.
        self.wait_until_all_reconciled()

        # Locate the partition's metadata.
        rows = self.read_all_rows()
        metadata_row = next((r for r in rows if r.row.key.HasField("metadata")), None)
        assert metadata_row is not None
        md = metadata_row.row
        mp = metadata_row.metastore_partition
        tid = md.key.metadata.topic_id
        pid = md.key.metadata.partition_id
        start = md.value.metadata.start_offset
        next_o = md.value.metadata.next_offset

        # Baseline: valid state should have no anomalies.
        resp = self._validate_partition(tid, pid, check_object_metadata=True)
        assert len(resp.anomalies) == 0, (
            f"expected no anomalies on valid state, got: "
            f"{[(a.anomaly_type, a.description) for a in resp.anomalies]}"
        )
        assert resp.extents_validated > 0
        self.logger.info(f"Baseline: {resp.extents_validated} extents, no anomalies")

        # Paginated validation should also pass.
        resp = self._validate_partition(tid, pid, max_extents=1)
        assert len(resp.anomalies) == 0

        extents = self.read_extents(mp, tid, pid)
        assert len(extents) >= 1
        first_ext = extents[0]
        last_ext = extents[-1]

        def md_write(**overrides: Any) -> metastore_pb.WriteRow:
            """Metadata row with field overrides."""
            fields: dict[str, Any] = dict(
                start_offset=start,
                next_offset=next_o,
                compaction_epoch=md.value.metadata.compaction_epoch,
                size=md.value.metadata.size,
            )
            fields.update(overrides)
            return metastore_pb.WriteRow(
                key=md.key,
                value=metastore_pb.RowValue(
                    metadata=metastore_pb.MetadataValue(**fields)
                ),
            )

        def ext_write(
            ext_row: metastore_pb.ReadRow, **overrides: Any
        ) -> metastore_pb.WriteRow:
            """Extent row with field overrides."""
            ev = ext_row.value.extent
            fields: dict[str, Any] = dict(
                last_offset=ev.last_offset,
                max_timestamp=ev.max_timestamp,
                filepos=ev.filepos,
                len=ev.len,
                object_id=ev.object_id,
            )
            fields.update(overrides)
            return metastore_pb.WriteRow(
                key=ext_row.key,
                value=metastore_pb.RowValue(extent=metastore_pb.ExtentValue(**fields)),
            )

        def compaction_writes(
            cleaned: list[tuple[int, int]],
            tombstones: list[tuple[int, int]] | None = None,
            **md_overrides: Any,
        ) -> list[metastore_pb.WriteRow]:
            """Metadata (with compaction_epoch=1) + compaction row."""
            comp_key = metastore_pb.RowKey(
                compaction=metastore_pb.CompactionKey(topic_id=tid, partition_id=pid)
            )
            ranges = [
                metastore_pb.OffsetRange(base_offset=b, last_offset=l)
                for b, l in cleaned
            ]
            ts_ranges = [
                metastore_pb.CleanedRangeWithTombstones(
                    base_offset=b,
                    last_offset=l,
                    cleaned_with_tombstones_at=1000,
                )
                for b, l in (tombstones or [])
            ]
            return [
                md_write(compaction_epoch=1, **md_overrides),
                metastore_pb.WriteRow(
                    key=comp_key,
                    value=metastore_pb.RowValue(
                        compaction=metastore_pb.CompactionValue(
                            cleaned_ranges=ranges,
                            cleaned_ranges_with_tombstones=ts_ranges,
                        )
                    ),
                ),
            ]

        fake_oid = str(uuid.uuid4())

        scenarios: list[CorruptionScenario] = [
            CorruptionScenario(
                name="next_offset_mismatch",
                expected_anomalies={AT.ANOMALY_TYPE_NEXT_OFFSET_MISMATCH},
                writes=[md_write(next_offset=1)],
            ),
            CorruptionScenario(
                name="extent_gap (delete first extent)",
                expected_anomalies={
                    AT.ANOMALY_TYPE_EXTENT_GAP,
                    AT.ANOMALY_TYPE_NEXT_OFFSET_MISMATCH,
                },
                deletes=[first_ext.key],
            ),
            CorruptionScenario(
                name="object_not_found",
                expected_anomalies={AT.ANOMALY_TYPE_OBJECT_NOT_FOUND},
                check_object_metadata=True,
                writes=[ext_write(last_ext, object_id=fake_oid)],
            ),
            CorruptionScenario(
                name="compaction_range_below_start",
                expected_anomalies={AT.ANOMALY_TYPE_COMPACTION_RANGE_BELOW_START},
                writes=compaction_writes(
                    cleaned=[(start, start + 10)],
                    start_offset=start + 20,
                ),
            ),
        ]

        # Snapshot state before running scenarios.
        snapshot = self._snapshot_rows(mp)

        passed = 0
        failed: list[str] = []
        for scenario in scenarios:
            self.logger.info(f"Running scenario: {scenario.name}")
            try:
                # Apply corruption.
                self._write_rows(
                    mp,
                    writes=scenario.writes,
                    deletes=scenario.deletes,
                )

                # Validate.
                resp = self._validate_partition(
                    tid,
                    pid,
                    check_object_metadata=scenario.check_object_metadata,
                )
                found = {a.anomaly_type for a in resp.anomalies}

                # At least one expected anomaly must be present.
                if not found & scenario.expected_anomalies:
                    msg = (
                        f"FAIL [{scenario.name}]: expected one of "
                        f"{scenario.expected_anomalies}, got {found}"
                    )
                    for a in resp.anomalies:
                        msg += f"\n  [{a.anomaly_type}] {a.description}"
                    self.logger.error(msg)
                    failed.append(msg)
                else:
                    self.logger.info(f"PASS [{scenario.name}]: detected {found}")
                    passed += 1
            finally:
                # Always restore, even on assertion failure.
                self._restore_snapshot(mp, snapshot)

        # Final sanity: restored state is clean.
        resp = self._validate_partition(tid, pid, check_object_metadata=True)
        assert len(resp.anomalies) == 0, (
            f"state not properly restored after scenarios, got: "
            f"{[(a.anomaly_type, a.description) for a in resp.anomalies]}"
        )

        self.logger.info(f"Validation scenarios: {passed}/{len(scenarios)} passed")
        assert not failed, (
            f"{len(failed)}/{len(scenarios)} scenarios failed:\n" + "\n".join(failed)
        )
