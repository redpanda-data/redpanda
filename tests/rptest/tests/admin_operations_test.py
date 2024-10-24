# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import threading
from rptest.services.admin import Admin
from rptest.tests.prealloc_nodes import PreallocNodesTest
import uuid

from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.services.admin_ops_fuzzer import AdminOperationsFuzzer, RedpandaAdminOperation
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.clients.default import DefaultClient
from rptest.services.kgo_verifier_services import KgoVerifierConsumerGroupConsumer, KgoVerifierProducer
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST, PREV_VERSION_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import RedpandaInstaller
from rptest.tests.redpanda_test import RedpandaTest
from rptest.utils.mode_checks import cleanup_on_early_exit, skip_debug_mode
from rptest.utils.node_operations import FailureInjectorBackgroundThread, NodeOpsExecutor, generate_random_workload

from rptest.clients.offline_log_viewer import OfflineLogViewer


class AdminOperationsTest(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        self.admin_fuzz = None

        super().__init__(test_context=test_context,
                         num_brokers=3,
                         *args,
                         **kwargs)

    def tearDown(self):
        if self.admin_fuzz is not None:
            self.admin_fuzz.stop()

        return super().tearDown()

    @cluster(num_nodes=3)
    def test_admin_operations(self):

        self.admin_fuzz = AdminOperationsFuzzer(self.redpanda,
                                                min_replication=1,
                                                operations_interval=1)

        self.admin_fuzz.start()
        self.admin_fuzz.wait(50, 360)
        self.admin_fuzz.stop()


class UUIDFormatTest(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context,
                         num_brokers=1,
                         *args,
                         **kwargs)

    @cluster(num_nodes=1)
    def test_uuid_format(self):
        """
        test that GET get_cluster_uuid returns a correctly formatted uuid, see https://github.com/redpanda-data/redpanda/issues/16162
        """
        cluster_uuid = Admin(self.redpanda).get_cluster_uuid(
            self.redpanda.nodes[0])
        assert cluster_uuid is not None, "expected uuid from cluster"
        # try to decode cluster_uuid and compare the result to the input, since UUID ignores {}-
        assert str(uuid.UUID(cluster_uuid)) == cluster_uuid.lower(), \
                f"get_cluster_uuid response '{cluster_uuid}' is not formatted properly"
