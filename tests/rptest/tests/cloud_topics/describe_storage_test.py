# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""
Coverage for `rpk topic describe-storage` on cloud-topic partitions.

The command renders the cloud storage status: for a cloud topic the size
section must use the L0/L1 layout, not the CLOUD-BYTES/CLOUD-SEGMENTS columns
used for segment-based tiered storage. A local segment count is shown only for
a tiered_cloud_topic, which keeps a real local log.
"""

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.tests.cloud_topics.e2e_test import EndToEndCloudTopicsBase


def _sections(output: str) -> dict[str, list[str]]:
    """Split describe-storage output into {SECTION_NAME: [content lines]}.

    Each section is a header word, then a line of '=', then content lines up to
    the next blank line.
    """
    sections: dict[str, list[str]] = {}
    lines = output.splitlines()
    current: str | None = None
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        if stripped and i + 1 < len(lines) and set(lines[i + 1].strip()) == {"="}:
            current = stripped
            sections[current] = []
            i += 2
            continue
        if current is not None and stripped:
            sections[current].append(lines[i])
        i += 1
    return sections


def _size_row(output: str) -> tuple[list[str], dict[str, str]]:
    """Return (column headers, partition-0 row as {column: value}) for SIZE."""
    lines = _sections(output).get("SIZE", [])
    assert len(lines) >= 2, f"no SIZE table in describe-storage output:\n{output}"
    header = lines[0].split()
    row = lines[1].split()
    return header, dict(zip(header, row))


class CloudTopicDescribeStorageTest(EndToEndCloudTopicsBase):
    topics = (
        TopicSpec(
            name=EndToEndCloudTopicsBase.s3_topic_name,
            partition_count=1,
            replication_factor=3,
        ),
    )

    def __init__(self, test_context: TestContext):
        super().__init__(test_context=test_context)
        self.msg_size = 16 * 1024
        # ~32 MiB: enough to land committed data in L1 after reconciliation.
        self.msg_count = 2000

    @cluster(num_nodes=4)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
        ],
    )
    def test_describe_storage_by_mode(self, storage_mode: str):
        # The topic is created in `storage_mode` by the base setUp.
        assert self.redpanda is not None
        topic = self.s3_topic_name
        is_tiered = storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2
        expected_mode = "tiered_cloud_topic" if is_tiered else "cloud_topic"

        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic,
            msg_size=self.msg_size,
            msg_count=self.msg_count,
            timeout_sec=180,
        )
        self.wait_until_reconciled(topic=topic, partition=0, timeout_sec=120)

        # Poll the CLI until it reports L1 bytes, which can lag reconciliation.
        def l1_reported() -> bool:
            output = self.rpk.describe_storage(topic)
            self.logger.info(f"describe-storage ({storage_mode}):\n{output}")
            header, row = _size_row(output)
            return "L1-BYTES" in header and int(row["L1-BYTES"]) > 0

        wait_until(
            l1_reported,
            timeout_sec=60,
            backoff_sec=3,
            retry_on_exc=True,
            err_msg=f"describe-storage never reported L1 bytes for {expected_mode}",
        )

        output = self.rpk.describe_storage(topic)
        header, row = _size_row(output)

        # SIZE uses the cloud-topic L0/L1 columns, not the CLOUD-BYTES/
        # CLOUD-SEGMENTS columns used for segment-based tiered storage.
        for col in ("LOCAL-BYTES", "L0-BYTES", "L1-BYTES", "TOTAL-BYTES", "L1-EXTENTS"):
            assert col in header, f"missing {col} in {header}:\n{output}"
        for col in ("CLOUD-BYTES", "CLOUD-SEGMENTS"):
            assert col not in header, (
                f"cloud-topic size table unexpectedly has {col} in {header}:\n{output}"
            )

        # A local segment count is shown only for tiered_cloud_topic.
        if is_tiered:
            assert "LOCAL-SEGMENTS" in header, output
            assert int(row["LOCAL-SEGMENTS"]) > 0, output
        else:
            assert "LOCAL-SEGMENTS" not in header, output
