# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import kafka.protocol.types as types
from kafka import errors as kerr
from kafka.admin import KafkaAdminClient
from kafka.protocol.api import Request, Response
from kafka.protocol.commit import OffsetFetchRequest_v3

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest


# --- ListOffsets v4 (API key 2) ---
# v4 is the first version with leader_epoch in the response.


class ListOffsetsResponse_v4(Response):
    API_KEY = 2
    API_VERSION = 4
    SCHEMA = types.Schema(
        ("throttle_time_ms", types.Int32),
        (
            "topics",
            types.Array(
                ("topic", types.String("utf-8")),
                (
                    "partitions",
                    types.Array(
                        ("partition", types.Int32),
                        ("error_code", types.Int16),
                        ("timestamp", types.Int64),
                        ("offset", types.Int64),
                        ("leader_epoch", types.Int32),
                    ),
                ),
            ),
        ),
    )


class ListOffsetsRequest_v4(Request):
    API_KEY = 2
    API_VERSION = 4
    RESPONSE_TYPE = ListOffsetsResponse_v4
    SCHEMA = types.Schema(
        ("replica_id", types.Int32),
        ("isolation_level", types.Int8),
        (
            "topics",
            types.Array(
                ("topic", types.String("utf-8")),
                (
                    "partitions",
                    types.Array(
                        ("partition", types.Int32),
                        ("current_leader_epoch", types.Int32),
                        ("timestamp", types.Int64),
                    ),
                ),
            ),
        ),
    )


# --- OffsetFetch v5 (API key 9) ---
# Copied from consumer_group_test.py. v5 includes leader_epoch.


class OffsetFetchResponse_v5(Response):
    API_KEY = 9
    API_VERSION = 5
    SCHEMA = types.Schema(
        ("throttle_time_ms", types.Int32),
        (
            "topics",
            types.Array(
                ("topic", types.String("utf-8")),
                (
                    "partitions",
                    types.Array(
                        ("partition", types.Int32),
                        ("offset", types.Int64),
                        ("leader_epoch", types.Int32),
                        ("metadata", types.String("utf-8")),
                        ("error_code", types.Int16),
                    ),
                ),
            ),
        ),
        ("error_code", types.Int16),
    )


class OffsetFetchRequest_v5(Request):
    API_KEY = 9
    API_VERSION = 5
    RESPONSE_TYPE = OffsetFetchResponse_v5
    SCHEMA = OffsetFetchRequest_v3.SCHEMA


# --- OffsetCommit v6 (API key 8) ---
# v6 is the first version with committed_leader_epoch.


class OffsetCommitResponse_v6(Response):
    API_KEY = 8
    API_VERSION = 6
    SCHEMA = types.Schema(
        ("throttle_time_ms", types.Int32),
        (
            "topics",
            types.Array(
                ("topic", types.String("utf-8")),
                (
                    "partitions",
                    types.Array(
                        ("partition", types.Int32),
                        ("error_code", types.Int16),
                    ),
                ),
            ),
        ),
    )


class OffsetCommitRequest_v6(Request):
    API_KEY = 8
    API_VERSION = 6
    RESPONSE_TYPE = OffsetCommitResponse_v6
    SCHEMA = types.Schema(
        ("group_id", types.String("utf-8")),
        ("generation_id", types.Int32),
        ("member_id", types.String("utf-8")),
        (
            "topics",
            types.Array(
                ("topic", types.String("utf-8")),
                (
                    "partitions",
                    types.Array(
                        ("partition", types.Int32),
                        ("committed_offset", types.Int64),
                        ("committed_leader_epoch", types.Int32),
                        ("committed_metadata", types.String("utf-8")),
                    ),
                ),
            ),
        ),
    )


# --- Test class ---


class ListOffsetsLeaderEpochTest(RedpandaTest):
    """
    Verify that ListOffsets returns the correct leader epoch per return path.
    CORE-12505: Redpanda incorrectly returns the current leader epoch instead
    of the historical epoch for earliest, timequery, and empty partition paths.
    """

    topics = (TopicSpec(name="epoch-test", partition_count=1, replication_factor=3),)

    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(
            test_ctx,
            *args,
            num_brokers=3,
            extra_rp_conf={"enable_leader_balancer": False},
            **kwargs,
        )

    def _setup_topic_with_epoch_gap(self):
        """Produce 12 records at epoch 0, then transfer leadership 3 times.

        After this setup all 12 records are from the initial epoch and
        the current leader epoch is >= 3, creating a gap between the
        record epoch and the current epoch.

        Returns (initial_epoch, current_epoch).
        """
        rpk = RpkTool(self.redpanda)
        admin = Admin(self.redpanda)

        # Produce 12 records — all will be in the initial epoch
        for i in range(12):
            rpk.produce("epoch-test", f"key-{i}", f"val-{i}")

        # Confirm HWM
        partitions = list(rpk.describe_topic("epoch-test"))
        assert partitions[0].high_watermark == 12, (
            f"Expected HWM 12, got {partitions[0].high_watermark}"
        )
        initial_epoch = partitions[0].leader_epoch
        self.logger.info(f"Initial state: HWM=12, epoch={initial_epoch}")

        # Transfer leadership 3 times to raise the epoch
        for i in range(3):
            partitions = list(rpk.describe_topic("epoch-test"))
            current_leader = partitions[0].leader
            replicas = partitions[0].replicas
            target = next(r for r in replicas if r != current_leader)
            admin.transfer_leadership_to(
                namespace="kafka",
                topic="epoch-test",
                partition=0,
                target_id=target,
            )
            admin.await_stable_leader(topic="epoch-test", partition=0, timeout_s=30)
            self.logger.info(
                f"Leadership transfer {i + 1}/3: {current_leader} -> {target}"
            )

        # Verify epoch advanced
        partitions = list(rpk.describe_topic("epoch-test"))
        current_epoch = partitions[0].leader_epoch
        assert current_epoch >= 3, (
            f"Expected epoch >= 3 after 3 transfers, got {current_epoch}"
        )
        self.logger.info(
            f"Setup complete: HWM={partitions[0].high_watermark}, "
            f"epoch={current_epoch} (records from epoch {initial_epoch})"
        )

        return initial_epoch, current_epoch

    def _list_offsets(self, topic, partition, timestamp):
        """Call ListOffsets API v4 and return (offset, leader_epoch).

        Args:
            topic: Topic name.
            partition: Partition index.
            timestamp: -2 for earliest, -1 for latest, or a Unix
                       timestamp in milliseconds for timequery.
        """
        rpk = RpkTool(self.redpanda)
        client = KafkaAdminClient(bootstrap_servers=self.redpanda.brokers())

        # Find the partition leader
        partitions = list(rpk.describe_topic(topic))
        leader_id = partitions[partition].leader

        # Ensure the client has metadata for this topic
        f = client._client.add_topic(topic)
        client._wait_for_futures([f])

        request = ListOffsetsRequest_v4(
            replica_id=-1,
            isolation_level=0,  # read_uncommitted
            topics=[
                (
                    topic,
                    [(partition, -1, timestamp)],  # -1 = no epoch fencing
                )
            ],
        )
        future = client._send_request_to_node(leader_id, request)
        client._wait_for_futures([future])
        response = future.value

        for resp_topic, resp_partitions in response.topics:
            for (
                part_id,
                error_code,
                resp_ts,
                resp_offset,
                leader_epoch,
            ) in resp_partitions:
                if part_id == partition:
                    error = kerr.for_code(error_code)
                    if error is not kerr.NoError:
                        raise error(
                            f"ListOffsets error for {topic}/{partition}: {error_code}"
                        )
                    return (resp_offset, leader_epoch)

        raise RuntimeError(f"Partition {partition} not found in ListOffsets response")

    def _offset_commit(
        self, group, topic, partition, offset, leader_epoch, timeout_sec=30
    ):
        """Commit an offset with a specific leader_epoch for a consumer group.

        Sends an OffsetCommitRequest_v6 directly, bypassing rpk and
        franz-go, so we can control the exact epoch value.  Waits for
        a stable coordinator before committing — the coordinator lookup
        can return a stale node after leadership transfers.
        """

        def try_commit():
            client = KafkaAdminClient(bootstrap_servers=self.redpanda.brokers())
            coordinator = client._find_coordinator_ids([group])[group]

            request = OffsetCommitRequest_v6(
                group_id=group,
                generation_id=-1,
                member_id="",
                topics=[
                    (
                        topic,
                        [(partition, offset, leader_epoch, "")],
                    )
                ],
            )
            future = client._send_request_to_node(coordinator, request)
            client._wait_for_futures([future])
            response = future.value

            for resp_topic, resp_partitions in response.topics:
                for part_id, error_code in resp_partitions:
                    if resp_topic == topic and part_id == partition:
                        error = kerr.for_code(error_code)
                        if error in (
                            kerr.NotCoordinatorForGroupError,
                            kerr.GroupCoordinatorNotAvailableError,
                        ):
                            return False
                        if error is not kerr.NoError:
                            raise error(
                                f"OffsetCommit error for "
                                f"{topic}/{partition}: {error_code}"
                            )
                        return True

            raise RuntimeError(
                f"{topic}/{partition} not found in OffsetCommit "
                f"response for group {group}"
            )

        wait_until(
            try_commit,
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=f"OffsetCommit for group {group} failed",
        )

    def _offset_fetch(self, group, topic, partition):
        """Fetch the committed offset and leader_epoch for a consumer group.

        Returns (offset, leader_epoch).  Returns (-1, -1) when no offset
        has been committed for this partition.
        """
        client = KafkaAdminClient(bootstrap_servers=self.redpanda.brokers())

        coordinator = client._find_coordinator_ids([group])[group]

        request = OffsetFetchRequest_v5(
            consumer_group=group,
            topics=[(topic, [partition])],
        )
        future = client._send_request_to_node(coordinator, request)
        client._wait_for_futures([future])
        response = future.value

        error = kerr.for_code(response.error_code)
        if error is not kerr.NoError:
            raise error(f"OffsetFetch error for group {group}")

        for resp_topic, resp_partitions in response.topics:
            for part_id, offset, leader_epoch, metadata, error_code in resp_partitions:
                if resp_topic == topic and part_id == partition:
                    if error_code != 0:
                        raise kerr.for_code(error_code)(
                            f"OffsetFetch partition error: {topic}/{partition}"
                        )
                    return (offset, leader_epoch)

        raise RuntimeError(
            f"{topic}/{partition} not found in OffsetFetch response for group {group}"
        )

    @cluster(num_nodes=3)
    @parametrize(expect_fix=True)
    def test_list_offsets_epoch(self, expect_fix):
        """Verify ListOffsets returns the correct leader epoch for each
        timestamp query type.

        All 12 records are produced before leadership is transferred 3
        times.  The earliest and timequery paths should return the
        initial epoch (the record epoch), while the latest path should
        return the current leader epoch (correct per Kafka).
        """
        initial_epoch, current_epoch = self._setup_topic_with_epoch_gap()

        # --- Earliest (timestamp = -2) ---
        offset, epoch = self._list_offsets("epoch-test", 0, timestamp=-2)
        self.logger.info(
            f"Earliest: offset={offset}, epoch={epoch}, current_epoch={current_epoch}"
        )
        if expect_fix:
            assert epoch == initial_epoch, (
                f"Earliest epoch should be {initial_epoch} (record epoch), got {epoch}"
            )
        else:
            assert epoch == current_epoch, (
                f"Bug expected: earliest epoch should be current "
                f"({current_epoch}), got {epoch}"
            )

        # --- Latest (timestamp = -1) ---
        offset, epoch = self._list_offsets("epoch-test", 0, timestamp=-1)
        self.logger.info(
            f"Latest: offset={offset}, epoch={epoch}, current_epoch={current_epoch}"
        )
        # Kafka returns the current leader epoch for timestamp=-1.
        assert epoch == current_epoch, (
            f"Latest epoch should be current leader epoch "
            f"({current_epoch}), got {epoch}"
        )

        # --- Timequery (timestamp = 0) ---
        # timestamp=0 is earlier than any wall-clock record timestamp,
        # so the query returns the start of the log.
        offset, epoch = self._list_offsets("epoch-test", 0, timestamp=0)
        self.logger.info(
            f"Timequery: offset={offset}, epoch={epoch}, current_epoch={current_epoch}"
        )
        if expect_fix:
            assert epoch == initial_epoch, (
                f"Timequery epoch should be {initial_epoch} (record epoch), got {epoch}"
            )
        else:
            assert epoch == current_epoch, (
                f"Bug expected: timequery epoch should be current "
                f"({current_epoch}), got {epoch}"
            )

    @cluster(num_nodes=3)
    def test_offset_commit_accepts_lower_epoch(self):
        """Verify the server accepts offset commits with a lower epoch
        than the previously committed epoch.

        This confirms there is no epoch-based rejection in the offset
        commit path.  The server's try_upsert_offset only compares the
        raft log_offset of the commit record, not committed_leader_epoch.

        Sequence:
        1. Produce 12 records (so the topic/partition exists)
        2. Commit offset=0 with epoch=4 (simulates buggy rpk seek)
        3. Commit offset=12 with epoch=1 (correct, lower epoch)
        4. Verify the server stored offset=12, epoch=1

        No leadership transfers needed — the server does not validate
        the committed epoch against the partition's actual leader epoch.
        """
        rpk = RpkTool(self.redpanda)
        group = "epoch-ordering-test"

        # Produce records so the topic/partition is valid
        for i in range(12):
            rpk.produce("epoch-test", f"key-{i}", f"val-{i}")

        # Commit offset=0 with a high epoch — this is what a buggy
        # rpk seek does today via ListOffsets.
        self._offset_commit(group, "epoch-test", 0, offset=0, leader_epoch=4)
        offset, epoch = self._offset_fetch(group, "epoch-test", 0)
        self.logger.info(f"After high-epoch commit: offset={offset}, epoch={epoch}")
        assert offset == 0 and epoch == 4

        # Now commit offset=12 with a lower epoch — this is what a
        # fixed ListOffsets would produce.
        self._offset_commit(group, "epoch-test", 0, offset=12, leader_epoch=1)
        offset, epoch = self._offset_fetch(group, "epoch-test", 0)
        self.logger.info(f"After low-epoch commit: offset={offset}, epoch={epoch}")
        assert offset == 12, (
            f"Server should accept lower-epoch commit: expected offset 12, got {offset}"
        )
        assert epoch == 1, (
            f"Server should store the lower epoch: expected 1, got {epoch}"
        )

    @cluster(num_nodes=3)
    @parametrize(expect_fix=True)
    def test_list_offsets_seek_reconsume(self, expect_fix):
        """End-to-end test for CORE-12505: rpk seek + re-consume.

        Workflow:
        1. Produce records, transfer leadership to raise epoch.
        2. Consume all records with a consumer group.
        3. Seek the group back to start via rpk (calls ListOffsets).
        4. Verify the seek committed the correct epoch.
        5. Re-consume and verify the group advances to the end.
        """
        initial_epoch, current_epoch = self._setup_topic_with_epoch_gap()
        rpk = RpkTool(self.redpanda)
        group = "test-group"

        # --- Step 1: consume all 12 records ---
        rpk.consume("epoch-test", group=group, n=12)
        group_desc = rpk.group_describe(group)
        assert group_desc.partitions[0].current_offset == 12, (
            f"Expected offset 12 after consuming, "
            f"got {group_desc.partitions[0].current_offset}"
        )
        assert group_desc.partitions[0].lag == 0, (
            f"Expected lag 0, got {group_desc.partitions[0].lag}"
        )

        # --- Step 2: inspect committed epoch after consumption ---
        offset, epoch = self._offset_fetch(group, "epoch-test", 0)
        self.logger.info(f"After consume: committed offset={offset}, epoch={epoch}")

        # --- Step 3: seek to start ---
        rpk.group_seek_to(group, "start")
        group_desc = rpk.group_describe(group)
        assert group_desc.partitions[0].current_offset == 0, (
            f"Expected offset 0 after seek, "
            f"got {group_desc.partitions[0].current_offset}"
        )

        # --- Step 4: inspect committed epoch after seek ---
        # rpk seek calls ListOffsets for the start offset, then commits
        # the result.  With the fix, the epoch should be the record
        # epoch, not the current leader epoch.
        offset, epoch = self._offset_fetch(group, "epoch-test", 0)
        self.logger.info(
            f"After seek: committed offset={offset}, epoch={epoch}, "
            f"current_epoch={current_epoch}"
        )
        if expect_fix:
            assert epoch == initial_epoch, (
                f"After seek, committed epoch should be {initial_epoch} "
                f"(record epoch), got {epoch}"
            )
        else:
            assert epoch == current_epoch, (
                f"Bug expected: after seek, committed epoch should be "
                f"current ({current_epoch}), got {epoch}"
            )

        # --- Step 5: re-consume ---
        # Run without -n so rpk stays alive long enough for franz-go's
        # 5-second auto-commit interval to fire.  The 10-second timeout
        # kills rpk after auto-commit has had time to run.
        #
        # NOTE: rpk consume with -n exits immediately after reading the
        # requested records, often before auto-commit fires.  This is
        # why the original reproduction appeared to show epoch-based
        # commit rejection — the commit was never sent, not rejected.
        try:
            rpk.consume("epoch-test", group=group, timeout=10)
        except Exception as e:
            self.logger.info(f"Re-consume timed out (expected): {e}")

        # --- Step 6: verify the group advanced ---
        group_desc = rpk.group_describe(group)
        final_offset, final_epoch = self._offset_fetch(group, "epoch-test", 0)
        self.logger.info(
            f"After re-consume: "
            f"committed offset={final_offset}, epoch={final_epoch}, "
            f"lag={group_desc.partitions[0].lag}"
        )
        if expect_fix:
            assert group_desc.partitions[0].lag == 0, (
                f"Expected lag 0 after re-consume, got {group_desc.partitions[0].lag}"
            )
        else:
            self.logger.info(
                f"DIAGNOSTIC (expect_fix=False): "
                f"final_offset={final_offset}, final_epoch={final_epoch}"
            )
