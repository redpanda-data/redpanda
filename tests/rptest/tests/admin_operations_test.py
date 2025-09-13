# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import uuid


from rptest.services.admin import Admin
from rptest.services.admin_ops_fuzzer import (
    AdminOperationsFuzzer,
)
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest


class AdminOperationsTest(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        self.admin_fuzz = None

        super().__init__(test_context=test_context, num_brokers=3, *args, **kwargs)

    def tearDown(self):
        if self.admin_fuzz is not None:
            self.admin_fuzz.stop()

        return super().tearDown()

    @cluster(num_nodes=3)
    def test_admin_operations(self):
        self.admin_fuzz = AdminOperationsFuzzer(
            self.redpanda, min_replication=1, operations_interval=1
        )

        self.admin_fuzz.start()
        self.admin_fuzz.wait(50, 360)
        self.admin_fuzz.stop()


class UUIDFormatTest(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context, num_brokers=1, *args, **kwargs)

    @cluster(num_nodes=1)
    def test_uuid_format(self):
        """
        test that GET get_cluster_uuid returns a correctly formatted uuid, see https://github.com/redpanda-data/redpanda/issues/16162
        """
        cluster_uuid = Admin(self.redpanda).get_cluster_uuid(self.redpanda.nodes[0])
        assert cluster_uuid is not None, "expected uuid from cluster"
        # try to decode cluster_uuid and compare the result to the input, since UUID ignores {}-
        assert str(uuid.UUID(cluster_uuid)) == cluster_uuid.lower(), (
            f"get_cluster_uuid response '{cluster_uuid}' is not formatted properly"
        )

    @cluster(num_nodes=1)
    def test_metrics_uuid_format(self):
        """
        test that GET get_metrics_uuid returns a correctly formatted uuid
        """
        admin = Admin(self.redpanda)
        admin.await_stable_leader(
            topic="controller",
            partition=0,
            namespace="redpanda",
            hosts=[n.account.hostname for n in self.redpanda._started],
            timeout_s=30,
            backoff_s=1,
        )
        metrics_uuid = admin.get_metrics_uuid()
        assert metrics_uuid is not None, "expected metrics uuid from cluster"
        self.logger.debug(f"metrics_uuid: {metrics_uuid}")
        assert str(uuid.UUID(metrics_uuid)) == metrics_uuid.lower(), (
            f"get_metrics_uuid response '{metrics_uuid}' is not formatted properly"
        )
