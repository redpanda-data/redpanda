# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Any, NamedTuple

from rptest.clients.admin.v2 import metastore_pb
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.cloud_topics.e2e_test import EndToEndCloudTopicsBase

from connectrpc.errors import ConnectError


class PartitionAndRow(NamedTuple):
    metastore_partition: int
    row: metastore_pb.ReadRow


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
    def test_read_and_write_rows(self) -> None:
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
