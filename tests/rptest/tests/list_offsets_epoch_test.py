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


class ListOffsetsLeaderEpochTest(RedpandaTest):
    """
    Verify that ListOffsets returns the correct leader epoch per return
    path.  Redpanda incorrectly returned the current leader epoch instead
    of the historical epoch for earliest, timequery, and empty partition
    paths.
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
        """Produce 12 records, then transfer leadership 3 times.

        After this setup all 12 records are from the initial epoch and
        the current leader epoch is >= 3, creating a gap between the
        record epoch and the current epoch.

        Returns (initial_epoch, current_epoch).
        """
        rpk = RpkTool(self.redpanda)
        admin = Admin(self.redpanda)

        for i in range(12):
            rpk.produce("epoch-test", f"key-{i}", f"val-{i}")

        partitions = list(rpk.describe_topic("epoch-test"))
        assert partitions[0].high_watermark == 12, (
            f"Expected HWM 12, got {partitions[0].high_watermark}"
        )
        initial_epoch = partitions[0].leader_epoch
        self.logger.info(f"Initial state: HWM=12, epoch={initial_epoch}")

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
        """Call ListOffsets API v4 and return (offset, leader_epoch)."""
        rpk = RpkTool(self.redpanda)
        client = KafkaAdminClient(bootstrap_servers=self.redpanda.brokers())
        try:
            partitions = list(rpk.describe_topic(topic))
            leader_id = partitions[partition].leader

            f = client._client.add_topic(topic)
            client._wait_for_futures([f])

            request = ListOffsetsRequest_v4(
                replica_id=-1,
                isolation_level=0,
                topics=[
                    (
                        topic,
                        [(partition, -1, timestamp)],
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

            raise RuntimeError(
                f"Partition {partition} not found in ListOffsets response"
            )
        finally:
            client.close()

    @cluster(num_nodes=3)
    def test_list_offsets_epoch(self):
        """Verify ListOffsets returns the correct leader epoch for each
        timestamp query type.

        All 12 records are produced before leadership is transferred 3
        times.  The earliest and timequery paths should return the
        initial epoch (the record epoch), while the latest path should
        return the current leader epoch.
        """
        initial_epoch, current_epoch = self._setup_topic_with_epoch_gap()

        # Earliest (timestamp = -2): should return the record epoch.
        offset, epoch = self._list_offsets("epoch-test", 0, timestamp=-2)
        self.logger.info(
            f"Earliest: offset={offset}, epoch={epoch}, current_epoch={current_epoch}"
        )
        assert epoch == initial_epoch, (
            f"Earliest epoch should be {initial_epoch} (record epoch), got {epoch}"
        )

        # Latest (timestamp = -1): should return the current leader epoch.
        offset, epoch = self._list_offsets("epoch-test", 0, timestamp=-1)
        self.logger.info(
            f"Latest: offset={offset}, epoch={epoch}, current_epoch={current_epoch}"
        )
        assert epoch == current_epoch, (
            f"Latest epoch should be current leader epoch "
            f"({current_epoch}), got {epoch}"
        )

        # Timequery (timestamp = 0): should return the record epoch.
        # timestamp=0 is earlier than any wall-clock record timestamp,
        # so the query returns the start of the log.
        offset, epoch = self._list_offsets("epoch-test", 0, timestamp=0)
        self.logger.info(
            f"Timequery: offset={offset}, epoch={epoch}, current_epoch={current_epoch}"
        )
        assert epoch == initial_epoch, (
            f"Timequery epoch should be {initial_epoch} (record epoch), got {epoch}"
        )
