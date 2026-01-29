# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess

from ducktape.tests.test import TestContext
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.kgo_verifier_services import KgoVerifierProducer


class RpkConsumeEmptyPartitionWarningTest(RedpandaTest):
    """
    Test that rpk topic consume prints a warning when trying to consume from
    a partition where the requested end offset is <= the available start offset.
    This verifies the change in https://github.com/redpanda-data/redpanda/pull/28979
    """

    def __init__(self, test_context: TestContext):
        super(RpkConsumeEmptyPartitionWarningTest, self).__init__(
            test_context=test_context,
        )

    @cluster(num_nodes=4)
    def test_warning_on_empty_partition_consume(self):
        """
        Test that rpk prints a warning when the requested end offset is
        less than or equal to the available start offset (e.g., after
        data has been deleted by trim_prefix).
        """

        topic = TopicSpec(
            partition_count=1,
            replication_factor=3,
            cleanup_policy=TopicSpec.CLEANUP_DELETE,
        )
        self.client().create_topic(topic)

        rpk = RpkTool(self.redpanda)

        # Produce 10 messages (offsets 0-9)
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size=1024,
            msg_count=10,
        )
        producer.start()
        producer.wait()
        producer.free()

        # Verify initial state
        partitions = list(rpk.describe_topic(topic.name))
        p = partitions[0]
        assert p.start_offset == 0
        assert p.high_watermark == 10

        # Use trim_prefix to move start offset to 8
        # This simulates data deletion (offsets 0-7 are now gone)
        new_start_offset = 8
        rpk.trim_prefix(topic.name, offset=new_start_offset, partitions=[0])

        # Verify that the start offset has moved forward
        partitions = list(rpk.describe_topic(topic.name))
        p = partitions[0]
        assert p.start_offset == new_start_offset, (
            f"Expected start offset to be {new_start_offset}, but got {p.start_offset}"
        )

        # Try to consume from offset 0 to 7 (before the new start offset)
        # This should trigger the warning message
        end_offset = new_start_offset - 1
        offset_range = f"0:{end_offset}"

        # Run rpk consume directly with subprocess to capture stderr
        cmd = [
            rpk._rpk_binary(),
            "topic",
            "consume",
            topic.name,
            "-o",
            offset_range,
            "--brokers",
            self.redpanda.brokers(),
            "-v",
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10,
        )

        stderr = result.stderr

        # Verify that the warning message appears in stderr
        expected_warning = f"no data to consume for {topic.name}/0: requested end offset {end_offset}, available start offset {new_start_offset}"
        assert expected_warning in stderr, (
            f"Expected warning message not found in stderr. Expected substring: '{expected_warning}', Got stderr: '{stderr}'"
        )
