# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import random

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import SISettings
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types


class DatalakeRecoveryModeTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeRecoveryModeTest,
              self).__init__(test_ctx,
                             num_brokers=3,
                             si_settings=SISettings(test_ctx),
                             extra_rp_conf={
                                 "iceberg_enabled": "true",
                                 "iceberg_catalog_commit_interval_ms": 5000
                             },
                             *args,
                             **kwargs)

    def setUp(self):
        # redpanda will be started by DatalakeServices
        pass

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types(),
            filesystem_catalog_mode=[True, False])
    def test_recovery_mode(self, cloud_storage_type, filesystem_catalog_mode):
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=filesystem_catalog_mode,
                              include_query_engines=[QueryEngineType.SPARK
                                                     ]) as dl:
            count = 1000
            rpk = RpkTool(self.redpanda)

            dl.create_iceberg_enabled_topic("foo", partitions=10)
            rpk.create_topic("bar", partitions=10, replicas=3)

            dl.produce_to_topic("foo", 1024, count)

            # test partial recovery mode
            self.redpanda.restart_nodes(
                random.sample(self.redpanda.nodes, 1),
                override_cfg_params={"recovery_mode_enabled": True})

            time.sleep(15)

            self.redpanda.restart_nodes(
                self.redpanda.nodes,
                override_cfg_params={"recovery_mode_enabled": True})
            self.redpanda.wait_for_membership(first_start=False)

            admin = Admin(self.redpanda)
            admin.await_stable_leader(namespace="redpanda", topic="controller")

            rpk.alter_topic_config("bar", TopicSpec.PROPERTY_ICEBERG_MODE,
                                   "key_value")
            time.sleep(15)

            self.redpanda.restart_nodes(
                self.redpanda.nodes,
                override_cfg_params={"recovery_mode_enabled": False})
            self.redpanda.wait_for_membership(first_start=False)

            dl.produce_to_topic("foo", 1024, count)
            dl.produce_to_topic("bar", 1024, count)

            dl.wait_for_translation("foo", msg_count=2 * count)
            dl.wait_for_translation("bar", msg_count=count)

    @cluster(num_nodes=6)
    @matrix(cloud_storage_type=supported_storage_types(),
            filesystem_catalog_mode=[True, False])
    def test_disabled_partitions(self, cloud_storage_type,
                                 filesystem_catalog_mode):
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              filesystem_catalog_mode=filesystem_catalog_mode,
                              include_query_engines=[QueryEngineType.SPARK
                                                     ]) as dl:

            count = 1000
            rpk = RpkTool(self.redpanda)

            dl.create_iceberg_enabled_topic("foo", partitions=10)
            rpk.create_topic("bar", partitions=10, replicas=3)

            dl.produce_to_topic("foo", 1024, count)

            admin = Admin(self.redpanda)
            admin.set_partitions_disabled(ns="kafka", topic="foo")
            admin.set_partitions_disabled(ns="kafka", topic="bar")

            rpk.alter_topic_config("bar", TopicSpec.PROPERTY_ICEBERG_MODE,
                                   "key_value")

            time.sleep(15)
            admin.set_partitions_disabled(ns="kafka", topic="foo", value=False)
            admin.set_partitions_disabled(ns="kafka", topic="bar", value=False)

            dl.produce_to_topic("foo", 1024, count)
            dl.produce_to_topic("bar", 1024, count)

            dl.wait_for_translation("foo", msg_count=2 * count)
            dl.wait_for_translation("bar", msg_count=count)
