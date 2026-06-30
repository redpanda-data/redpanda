# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import math
import time

from ducktape.mark import parametrize
from ducktape.tests.test import TestContext

from rptest.clients.rpk import RpkTool, RPKACLInput
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest


class DescribeAclsScaleTest(RedpandaTest):
    """DescribeAcls groups matching bindings by resource pattern, so a
    non-chunked container makes one allocation that grows with the pattern
    count and overflows at scale (CORE-16758). The harness fails the test if a
    broker logs an `oversized allocation` above 200KiB while building the
    response.
    """

    PRINCIPAL = "User:scale-test"
    BATCH_SIZE = 1000

    def __init__(self, test_context: TestContext) -> None:
        super().__init__(test_context=test_context, num_brokers=1)
        self._rpk = RpkTool(self.redpanda)

    def _create_topic_acls(self, num_acls: int) -> None:
        """Create num_acls allow-read ACLs, one per distinct topic, to seed
        that many resource patterns. The topics need not exist. Batched into
        single rpk invocations of BATCH_SIZE."""
        created = 0
        while created < num_acls:
            end = min(created + self.BATCH_SIZE, num_acls)
            topics = [f"scale-topic-{i}" for i in range(created, end)]
            self._rpk.acl_create(
                RPKACLInput(
                    allow_principal=[self.PRINCIPAL],
                    operation=["read"],
                    topic=topics,
                )
            )
            created = end

    @cluster(num_nodes=1)
    @parametrize(num_acls=10000)
    def test_describe_acls_at_scale(self, num_acls: int) -> None:
        # Each CreateACLs batch replicates as a single create_acls_cmd
        # controller record (not one per binding), so raise the guard by the
        # batch count.
        num_batches = math.ceil(num_acls / self.BATCH_SIZE)
        self.redpanda.set_expected_controller_records(1000 + num_batches)

        seed_start = time.time()
        self._create_topic_acls(num_acls)
        self.logger.info(f"created {num_acls} ACLs in {time.time() - seed_start:.1f}s")

        # DescribeAcls builds the grouping container with num_acls patterns.
        list_start = time.time()
        out = self._rpk.acl_list()
        self.logger.info(f"described ACLs in {time.time() - list_start:.1f}s")

        # Sanity check that the describe returned the seeded ACLs.
        found = out.count(self.PRINCIPAL)
        assert found >= num_acls, (
            f"expected at least {num_acls} ACL rows in describe response, found {found}"
        )
