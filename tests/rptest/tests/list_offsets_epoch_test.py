# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import tempfile
import time
from typing import Any

from kafka import errors as kerr
from kafka.admin import KafkaAdminClient
from kafka.protocol.commit import OffsetFetchRequest_v5
from kafka.protocol.list_offsets import ListOffsetsRequest_v4

from ducktape.mark.resource import cluster as ducktape_cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.version import V_3_0_0

from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kafka import KafkaServiceAdapter
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until_result


TOPIC_NAME = "epoch-test"


class BaseListOffsetsLeaderEpochTest:
    """
    Shared logic for verifying that ListOffsets returns the correct
    leader epoch per return path.  Subclasses supply the cluster (Redpanda
    or Kafka) and a cluster-specific `_advance_leader_epoch` that bumps
    the partition's leader epoch without modifying the log.

    CORE-12505: Redpanda incorrectly returns the current leader epoch
    instead of the historical epoch for earliest, timequery, and empty
    partition paths.
    """

    logger: Any

    def client(self) -> DefaultClient:
        raise NotImplementedError

    def _restart_leader(self, cluster, topic, partition):
        """Restart the current leader for the given partition.

        Subclasses resolve the leader node (cluster-specific) and invoke
        the cluster's restart primitive.  `_advance_leader_epoch` wraps
        this with pre/post epoch checks.
        """
        raise NotImplementedError

    def _wait_for_stable_partition(
        self, rpk: RpkTool, topic: str, partition: int, timeout_sec: int = 30
    ) -> Any:
        """Return the row for `topic`/`partition`, waiting until the
        partition has a non-negative leader and a known epoch.

        `describe_topic` without `tolerant=True` silently omits rows that
        are missing metadata during leadership transitions, which would
        otherwise surface as `IndexError` when indexing by position.
        """

        def ready() -> Any:
            rows = list(rpk.describe_topic(topic, tolerant=True))
            if len(rows) <= partition:
                return None
            row = rows[partition]
            if row.leader is None or row.leader < 0 or row.leader_epoch is None:
                return None
            return row

        return wait_until_result(
            ready,
            timeout_sec=timeout_sec,
            backoff_sec=0.5,
            err_msg=f"No stable leader for {topic}/{partition}",
        )

    def _advance_leader_epoch(self, cluster, topic, partition):
        """Bump the leader epoch by restarting the current leader.

        Under our config (Kafka: auto.leader.rebalance.enable=false,
        default controlled shutdown; Redpanda: enable_leader_balancer
        disabled), the controller elects a different in-sync replica
        during the restart and does not hand leadership back when the
        restarted node rejoins.  The log is unchanged, so a gap opens
        between the record epoch and the current leader epoch.
        """
        rpk = RpkTool(cluster)
        prior_epoch = self._wait_for_stable_partition(
            rpk, topic, partition
        ).leader_epoch

        self._restart_leader(cluster, topic, partition)

        def epoch_advanced():
            rows = list(rpk.describe_topic(topic, tolerant=True))
            if len(rows) <= partition:
                return False
            epoch = rows[partition].leader_epoch
            return epoch is not None and epoch > prior_epoch

        wait_until(
            epoch_advanced,
            timeout_sec=60,
            backoff_sec=1,
            err_msg=(
                f"Leader epoch for {topic}/{partition} did not advance "
                f"past {prior_epoch}"
            ),
        )

    def _setup_topic_with_epoch_gap(
        self,
        cluster,
        num_records: int = 12,
        num_epoch_advances: int = 3,
    ):
        """Produce `num_records` records at the initial epoch, then advance
        the leader epoch `num_epoch_advances` times.

        After this setup all records (if any) are from the initial epoch
        and the current leader epoch is >= num_epoch_advances, creating a
        gap between the record epoch and the current epoch.  Pass
        `num_records=0` to exercise the empty-partition path.  Pass a
        smaller `num_epoch_advances` (e.g. 1) when only a single advance
        is needed — each advance is a broker restart, so reducing the
        count cuts setup time.

        Returns (initial_epoch, current_epoch).
        """
        rpk = RpkTool(cluster)

        topic = TopicSpec(name=TOPIC_NAME, partition_count=1, replication_factor=3)
        self.client().create_topic(topic)

        # Produce records — all will be in the initial epoch
        for i in range(num_records):
            rpk.produce(TOPIC_NAME, f"key-{i}", f"val-{i}")

        partitions = list(rpk.describe_topic(TOPIC_NAME))
        initial_epoch = partitions[0].leader_epoch
        self.logger.info(
            f"Initial state: HWM={partitions[0].high_watermark}, "
            f"epoch={initial_epoch}, records={num_records}"
        )

        # Advance the leader epoch num_epoch_advances times
        for i in range(num_epoch_advances):
            self._advance_leader_epoch(cluster, TOPIC_NAME, 0)
            partitions = list(rpk.describe_topic(TOPIC_NAME))
            self.logger.info(
                f"Epoch advance {i + 1}/{num_epoch_advances}: "
                f"epoch={partitions[0].leader_epoch}"
            )

        current_epoch = partitions[0].leader_epoch
        self.logger.info(
            f"Setup complete: HWM={partitions[0].high_watermark}, "
            f"epoch={current_epoch} (records from epoch {initial_epoch})"
        )

        return initial_epoch, current_epoch

    def _list_offsets(self, cluster, topic, partition, timestamp):
        """Call ListOffsets API v4 and return (offset, leader_epoch).

        Args:
            cluster: Cluster to query (Redpanda or Kafka).
            topic: Topic name.
            partition: Partition index.
            timestamp: -2 for earliest, -1 for latest, or a Unix
                       timestamp in milliseconds for timequery.
        """
        rpk = RpkTool(cluster)
        leader_id = self._wait_for_stable_partition(rpk, topic, partition).leader

        client = KafkaAdminClient(bootstrap_servers=cluster.brokers())
        try:
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
        finally:
            client.close()

        for _resp_topic, resp_partitions in response.topics:
            for (
                part_id,
                error_code,
                _resp_ts,
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

    def _get_committed(self, cluster, group, topic, partition):
        """Read the committed (offset, leader_epoch) for a partition via OffsetFetch v5.

        Returns (-1, -1) if no commit exists for the partition.

        `rpk group describe` only exposes CURRENT-OFFSET, not the
        committed_leader_epoch field.  This helper goes around rpk and
        issues OffsetFetch v5 directly so the test can assert on epoch.
        """
        client = KafkaAdminClient(bootstrap_servers=cluster.brokers())
        try:
            coordinator_id = client._find_coordinator_ids([group])[group]

            # Ensure the client has metadata for this topic
            f = client._client.add_topic(topic)
            client._wait_for_futures([f])

            request = OffsetFetchRequest_v5(
                consumer_group=group,
                topics=[(topic, [partition])],
            )
            future = client._send_request_to_node(coordinator_id, request)
            client._wait_for_futures([future])
            response = future.value
        finally:
            client.close()

        for _resp_topic, resp_partitions in response.topics:
            for (
                part_id,
                offset,
                leader_epoch,
                _metadata,
                error_code,
            ) in resp_partitions:
                if part_id == partition:
                    error = kerr.for_code(error_code)
                    if error is not kerr.NoError:
                        raise error(
                            f"OffsetFetch error for {topic}/{partition}: {error_code}"
                        )
                    return (offset, leader_epoch)

        return (-1, -1)

    def _test_list_offsets_epoch(self, cluster, expect_incorrect_behavior):
        """Verify ListOffsets returns the correct leader epoch for each
        timestamp query type.

        All records are produced before leadership is transferred 3
        times.  The earliest and timequery paths should return the
        initial epoch (the record epoch), while the latest path should
        return the current leader epoch (correct per Kafka).
        """
        initial_epoch, current_epoch = self._setup_topic_with_epoch_gap(cluster)

        # --- Earliest (timestamp = -2) ---
        offset, epoch = self._list_offsets(cluster, TOPIC_NAME, 0, timestamp=-2)
        self.logger.info(
            f"Earliest: offset={offset}, epoch={epoch}, current_epoch={current_epoch}"
        )
        if expect_incorrect_behavior:
            assert epoch == current_epoch, (
                f"Bug expected: earliest epoch should be current "
                f"({current_epoch}), got {epoch}"
            )
        else:
            assert epoch == initial_epoch, (
                f"Earliest epoch should be {initial_epoch} (record epoch), got {epoch}"
            )

        # --- Latest (timestamp = -1) ---
        offset, epoch = self._list_offsets(cluster, TOPIC_NAME, 0, timestamp=-1)
        self.logger.info(
            f"Latest: offset={offset}, epoch={epoch}, current_epoch={current_epoch}"
        )
        # Both Redpanda and Kafka return the current leader epoch for timestamp=-1.
        assert epoch == current_epoch, (
            f"Latest epoch should be current leader epoch "
            f"({current_epoch}), got {epoch}"
        )

        # --- Timequery (timestamp = 0) ---
        # timestamp=0 is earlier than any wall-clock record timestamp,
        # so the query returns the start of the log.
        offset, epoch = self._list_offsets(cluster, TOPIC_NAME, 0, timestamp=0)
        self.logger.info(
            f"Timequery: offset={offset}, epoch={epoch}, current_epoch={current_epoch}"
        )
        if expect_incorrect_behavior:
            assert epoch == current_epoch, (
                f"Bug expected: timequery epoch should be current "
                f"({current_epoch}), got {epoch}"
            )
        else:
            assert epoch == initial_epoch, (
                f"Timequery epoch should be {initial_epoch} (record epoch), got {epoch}"
            )

    def _test_empty_partition_list_offsets_epoch(
        self, cluster, expect_incorrect_behavior
    ):
        """Verify ListOffsets on an empty partition whose leader epoch has
        advanced.

        No records are produced, so earliest/latest both point at
        offset 0 and there is no record epoch to return.  Kafka returns
        the current leader epoch for those paths; a timequery has no
        matching record and returns offset=-1, leader_epoch=-1.
        """
        _initial_epoch, current_epoch = self._setup_topic_with_epoch_gap(
            cluster, num_records=0
        )

        # --- Earliest (timestamp = -2) ---
        offset, epoch = self._list_offsets(cluster, TOPIC_NAME, 0, timestamp=-2)
        self.logger.info(
            f"Earliest (empty): offset={offset}, epoch={epoch}, "
            f"current_epoch={current_epoch}"
        )
        assert offset == 0, (
            f"Earliest offset should be 0 on empty partition, got {offset}"
        )
        # Both Redpanda and Kafka should return the current epoch for earliest on an empty partition.
        assert epoch == current_epoch, (
            f"Earliest epoch on empty partition should be current "
            f"({current_epoch}), got {epoch}"
        )

        # --- Latest (timestamp = -1) ---
        offset, epoch = self._list_offsets(cluster, TOPIC_NAME, 0, timestamp=-1)
        self.logger.info(
            f"Latest (empty): offset={offset}, epoch={epoch}, "
            f"current_epoch={current_epoch}"
        )
        assert offset == 0, (
            f"Latest offset should be 0 on empty partition, got {offset}"
        )
        # Both Redpanda and Kafka should return the current epoch for latest on an empty partition.
        assert epoch == current_epoch, (
            f"Latest epoch should be current leader epoch "
            f"({current_epoch}), got {epoch}"
        )

        # --- Timequery (timestamp = 0) ---
        # No record matches any timestamp on an empty partition, so the
        # server returns offset=-1 with leader_epoch=-1.
        offset, epoch = self._list_offsets(cluster, TOPIC_NAME, 0, timestamp=0)
        self.logger.info(
            f"Timequery (empty): offset={offset}, epoch={epoch}, "
            f"current_epoch={current_epoch}"
        )
        assert offset == -1, (
            f"Timequery on empty partition should return -1, got {offset}"
        )
        if expect_incorrect_behavior:
            assert epoch == current_epoch, (
                f"Bug expected: timequery epoch on empty partition should be "
                f"current ({current_epoch}), got {epoch}"
            )
        else:
            assert epoch == -1, (
                f"Timequery epoch on empty partition should be -1, got {epoch}"
            )


class ListOffsetsLeaderEpochRedpandaTest(RedpandaTest, BaseListOffsetsLeaderEpochTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super().__init__(
            test_ctx,
            *args,
            num_brokers=3,
            extra_rp_conf={"enable_leader_balancer": False},
            **kwargs,
        )

    def _restart_leader(self, cluster, topic, partition):
        rpk = RpkTool(cluster)
        leader_id = list(rpk.describe_topic(topic))[partition].leader
        leader_node = self.redpanda.get_node_by_id(leader_id)
        assert leader_node is not None, (
            f"Could not resolve leader node for id {leader_id}"
        )
        self.redpanda.restart_nodes(leader_node)

    def _consume_and_wait_for_autocommit(
        self, group, topic, expected_count, timeout=30
    ):
        """Spawn an RpkConsumer for `group`, drain `expected_count` messages,
        stop, and return the actual count of messages received.

        For the negative-assertion case (expected_count == 0), this waits
        15 s before returning so any stragglers have time to arrive.

        rpk topic consume --group uses franz-go's AutoCommitMarks, which is
        the path that exhibits the silent commit-mark discard symptom from
        CORE-12505.  RpkConsumer's stop_node uses SIGTERM, so rpk's signal
        handler triggers franz-go client.Close() — which flushes a final
        commit and sends LeaveGroup synchronously before the process
        exits.  By the time stop()/free() return, the broker has the
        final commit and the group is empty.
        """
        consumer = RpkConsumer(
            self.test_context,
            self.redpanda,
            topic,
            group=group,
            save_msgs=False,
            clean_shutdown=True,
        )
        consumer.start()
        try:
            if expected_count > 0:
                wait_until(
                    lambda: consumer.message_count >= expected_count,
                    timeout_sec=timeout,
                    backoff_sec=0.5,
                    err_msg=lambda: (
                        f"Consumer did not reach {expected_count} messages, "
                        f"got {consumer.message_count}"
                    ),
                )
            else:
                # Negative assertion: consumer should not receive messages.
                # Wait long enough that any stragglers have arrived.
                time.sleep(15)
            return consumer.message_count
        finally:
            consumer.stop()
            consumer.free()

    def _apply_throwaway_hack(self, real_group, throwaway_group, topic, partition):
        """Apply the throwaway-group hack to seed `real_group` with a clean
        commit (offset, epoch=-1) for `topic`/`partition`.

        Steps:
          1. rpk group seek <throwaway> --to start --topics T --allow-new-topics
             (issues ListOffsets — affected by the bug, but we only need the
             offset value; the wrong epoch is discarded in step 3.)
          2. rpk group describe <throwaway> — read the committed offset back.
          3. Write `topic partition offset` to a local seek file (rpk runs on
             the test runner, not on a Redpanda node, so the file must live
             on the local filesystem).
          4. rpk group seek <real-group> --to-file <file>
             (rpk hard-codes LeaderEpoch: -1 in the --to-file path, sidestepping
             the bug.)
          5. rpk group delete <throwaway> — clean up.  Local file is also
             removed in the finally block.
        """
        rpk = RpkTool(self.redpanda)
        seek_file = os.path.join(
            tempfile.gettempdir(), f"seek-{real_group}-{partition}.txt"
        )

        try:
            # Step 1: seek the throwaway group to start.
            # rpk.group_describe has a built-in 10-attempt retry on
            # COORDINATOR_NOT_AVAILABLE (rpk.py:1199-1211); call it first
            # as a side-effect-only barrier to wait for the coordinator
            # to be available before the seek.
            rpk.group_describe(throwaway_group)
            rpk.group_seek_to(
                throwaway_group,
                "start",
                topics=[topic],
                allow_new_topics=True,
            )

            # Step 2: read the committed offset back from the throwaway group.
            described = rpk.group_describe(throwaway_group)
            offset = None
            for p in described.partitions:
                if p.topic == topic and p.partition == partition:
                    assert p.current_offset is not None, (
                        f"Partition {topic}/{partition} found in "
                        f"{throwaway_group} but current_offset is None "
                        f"(seek did not commit?)"
                    )
                    offset = p.current_offset
                    break
            assert offset is not None, (
                f"Could not find partition {topic}/{partition} in "
                f"throwaway group {throwaway_group}"
            )

            # Step 3: write the seek file on the local filesystem (rpk runs
            # locally, not on a Redpanda node).
            seek_line = f"{topic} {partition} {offset}\n"
            with open(seek_file, "w") as f:
                f.write(seek_line)

            # Step 4: seek the real group via --to-file (epoch=-1 hardcoded).
            rpk.group_seek_to_file(real_group, seek_file)
        finally:
            # Clean up the local seek file.
            try:
                os.unlink(seek_file)
            except FileNotFoundError:
                pass
            except Exception as e:
                self.logger.warning(f"Failed to delete seek file {seek_file}: {e}")

            # Step 5: always clean up the throwaway group, even on failure.
            try:
                rpk.group_delete(throwaway_group)
            except Exception as e:
                self.logger.warning(
                    f"Failed to delete throwaway group {throwaway_group}: {e}"
                )

    @cluster(num_nodes=3)
    def test_list_offsets_epoch(self):
        self._test_list_offsets_epoch(self.redpanda, expect_incorrect_behavior=True)

    @cluster(num_nodes=3)
    def test_list_offsets_epoch_empty_partition(self):
        self._test_empty_partition_list_offsets_epoch(
            self.redpanda, expect_incorrect_behavior=True
        )

    @cluster(num_nodes=4)
    def test_seek_to_start_poisons_commit(self):
        """End-to-end reproduction of CORE-12505 via `rpk group seek --to start`.

        With a stagnant topic whose records were produced at an older
        leader epoch, `rpk group seek --to start` writes (offset=0,
        epoch=current_epoch) into __consumer_offsets — the wrong-high
        epoch.  A franz-go AutoCommitMarks consumer (rpk topic consume
        --group) reads all records but its commit-marking discards every
        mark, because head is seeded at (0, current_epoch) and
        EpochOffset.Less treats the wrong-high epoch as newer than any
        real (record_offset, initial_epoch) pair.

        Symptom: consumer reads N records, committed offset stays at 0,
        restart re-reads everything.

        The bug is broader than this single test case: it manifests for
        any seek that resolves to records produced at an older leader
        epoch (`--to start`, `--to <past-timestamp>`, or any tool that
        does ListOffsets earliest/timequery → OffsetCommit).  This test
        exercises `--to start` because it is the most reproducible
        trigger.
        """
        group = "poisoned-group"
        partition = 0
        num_records = 12

        _initial_epoch, current_epoch = self._setup_topic_with_epoch_gap(
            self.redpanda, num_records=num_records, num_epoch_advances=1
        )

        # Seek poisons __consumer_offsets with (offset=0, epoch=current_epoch).
        # Use rpk.group_describe as a coordinator-availability barrier.
        # It has a built-in 10-attempt retry on COORDINATOR_NOT_AVAILABLE
        # at rpk.py:1199-1211 (comment: "try to wait for leadership to
        # stabilize"); we use it for its side effect (the wait) before
        # the seek that would otherwise hit the fresh-cluster race.
        rpk = RpkTool(self.redpanda)
        rpk.group_describe(group)
        rpk.group_seek_to(group, "start", topics=[TOPIC_NAME], allow_new_topics=True)

        # Pre-consumer assertion: the poison is in __consumer_offsets.
        committed = self._get_committed(self.redpanda, group, TOPIC_NAME, partition)
        self.logger.info(f"Pre-consume committed: {committed}")
        assert committed == (0, current_epoch), (
            f"Expected poisoned commit (0, {current_epoch}), got {committed}"
        )

        # First consume run: reads all 12 records.
        count = self._consume_and_wait_for_autocommit(
            group, TOPIC_NAME, expected_count=num_records
        )
        assert count >= num_records, (
            f"First run: expected at least {num_records} messages, got {count}"
        )

        # Single-pass assertion: committed offset is unchanged from the poison.
        # franz-go's mark-discard kept head stuck; autocommit just re-committed
        # the same poisoned pair.
        committed = self._get_committed(self.redpanda, group, TOPIC_NAME, partition)
        self.logger.info(f"Post-consume committed: {committed}")
        assert committed == (0, current_epoch), (
            f"Bug expected: committed offset should still be "
            f"(0, {current_epoch}), got {committed}"
        )

        # Second consume run: full replay — the failure cycle.
        count = self._consume_and_wait_for_autocommit(
            group, TOPIC_NAME, expected_count=num_records
        )
        assert count >= num_records, (
            f"Second run: expected full replay of {num_records} messages, got {count}"
        )

        # Replay verification: commit is still poisoned, confirming the
        # second-run replay was caused by the stuck commit (not by some
        # other reason the consumer happened to read N messages).
        committed = self._get_committed(self.redpanda, group, TOPIC_NAME, partition)
        assert committed == (0, current_epoch), (
            f"Replay confirmed: commit still poisoned at (0, {current_epoch}), "
            f"got {committed}"
        )

    @cluster(num_nodes=4)
    def test_throwaway_hack_mitigates_seek_to_start(self):
        """Verify the throwaway-group hack mitigates CORE-12505.

        Same setup as test_seek_to_start_poisons_commit, but instead of
        `rpk group seek --to start` (which calls ListOffsets and writes
        the wrong-high epoch), apply the throwaway-group hack:

          1. Seek a throwaway group to start (still hits the bug, but we
             only use the offset value, not the epoch).
          2. Read the committed offset back via rpk group describe.
          3. Write `topic partition offset` to a seek file.
          4. Seek the real group via --to-file (rpk hard-codes
             LeaderEpoch: -1, sidestepping the bug).
          5. Delete the throwaway group.

        The seeded commit is (0, -1).  Records flow in at initial_epoch;
        franz-go's EpochOffset.Less treats initial_epoch > -1, so marks
        are accepted, head advances, autocommit commits.  No replay on
        restart.
        """
        real_group = "mitigated-group"
        throwaway_group = "seek-helper"
        partition = 0
        num_records = 12

        initial_epoch, _current_epoch = self._setup_topic_with_epoch_gap(
            self.redpanda, num_records=num_records, num_epoch_advances=1
        )

        # Apply the literal 5-step hack.
        self._apply_throwaway_hack(real_group, throwaway_group, TOPIC_NAME, partition)

        # Pre-consumer assertion: the seek file's hardcoded epoch=-1 is in
        # __consumer_offsets.
        committed = self._get_committed(
            self.redpanda, real_group, TOPIC_NAME, partition
        )
        self.logger.info(f"Pre-consume committed: {committed}")
        assert committed == (0, -1), (
            f"Expected clean commit (0, -1) from --to-file, got {committed}"
        )

        # First consume run: reads all 12 records.
        count = self._consume_and_wait_for_autocommit(
            real_group, TOPIC_NAME, expected_count=num_records
        )
        assert count >= num_records, (
            f"First run: expected at least {num_records} messages, got {count}"
        )

        # Single-pass assertion: committed offset advanced to num_records,
        # epoch is initial_epoch (the term in which the records were produced).
        # MarkCommitRecords writes head = (record.LeaderEpoch, record.Offset+1),
        # so after the last record (offset = num_records - 1) head is
        # (initial_epoch, num_records).
        committed = self._get_committed(
            self.redpanda, real_group, TOPIC_NAME, partition
        )
        self.logger.info(f"Post-consume committed: {committed}")
        assert committed == (num_records, initial_epoch), (
            f"Mitigation expected: committed offset should be "
            f"({num_records}, {initial_epoch}), got {committed}"
        )

        # Second consume run: no replay — commits persisted, consumer starts
        # at offset num_records and finds no records.
        count = self._consume_and_wait_for_autocommit(
            real_group, TOPIC_NAME, expected_count=0, timeout=15
        )
        assert count == 0, f"Second run: expected no replay, got {count} messages"

        # Post-no-replay: confirm the commit didn't roll backward during
        # the second consume run.  This makes the no-replay assertion a
        # structural argument: zero messages because the consumer started
        # from (num_records, initial_epoch), not for some other reason.
        committed = self._get_committed(
            self.redpanda, real_group, TOPIC_NAME, partition
        )
        assert committed == (num_records, initial_epoch), (
            f"Post-no-replay: commit should still be "
            f"({num_records}, {initial_epoch}), got {committed}"
        )


class ListOffsetsLeaderEpochKafkaTest(Test, BaseListOffsetsLeaderEpochTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.zk = ZookeeperService(self.test_context, num_nodes=1, version=V_3_0_0)

        # Disable auto preferred-leader rebalance so leadership does not
        # bounce back on its own after we restart the leader.
        server_prop_overrides = [
            ["auto.leader.rebalance.enable", "false"],
        ]

        self.kafka = KafkaServiceAdapter(
            self.test_context,
            KafkaService(
                self.test_context,
                num_nodes=3,
                zk=self.zk,
                server_prop_overrides=server_prop_overrides,
                version=V_3_0_0,
            ),
        )

        self._client = DefaultClient(self.kafka)

    def client(self):
        return self._client

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def tearDown(self):
        self.logger.info("Stopping Kafka...")
        self.kafka.stop()

        self.logger.info("Stopping Zookeeper...")
        self.zk.stop()

    def _restart_leader(self, cluster, topic, partition):
        leader_node = self.kafka.leader(topic, partition)
        self.kafka.restart_node(leader_node, clean_shutdown=True)

    @ducktape_cluster(num_nodes=4)
    def test_list_offsets_epoch(self):
        # Kafka defines the correct behavior we compare against.
        self._test_list_offsets_epoch(self.kafka, expect_incorrect_behavior=False)

    @ducktape_cluster(num_nodes=4)
    def test_list_offsets_epoch_empty_partition(self):
        # Kafka defines the correct behavior we compare against.
        self._test_empty_partition_list_offsets_epoch(
            self.kafka, expect_incorrect_behavior=False
        )
