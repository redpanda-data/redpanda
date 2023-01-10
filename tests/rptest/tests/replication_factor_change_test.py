# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.util import expect_exception

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services.admin import Admin


class ReplicationFactorChangeTest(RedpandaTest):
    topics = (TopicSpec(partition_count=3, replication_factor=3), )

    def __init__(self, test_context):
        super(ReplicationFactorChangeTest,
              self).__init__(test_context=test_context, num_brokers=4)

        self._rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)

        self.rf_property = "replication.factor"
        self.topic_name = self.topics[0].name

    def check_rf(self, new_rf):
        for partition in self._rpk.describe_topic(self.topic_name):
            if len(partition.replicas) != new_rf:
                return False
        return True

    @cluster(num_nodes=4)
    def simple_test(self):
        self.replication_factor = 4
        self._rpk.alter_topic_config(self.topic_name, self.rf_property,
                                     self.replication_factor)
        self.check_rf(self.replication_factor)

        def wait_rec():
            return len(self.admin.list_reconfigurations()) == 0

        wait_until(wait_rec,
                   timeout_sec=60,
                   backoff_sec=2,
                   err_msg="Can not wait end of reconfiguration")

        self.replication_factor = 2
        self._rpk.alter_topic_config(self.topic_name, self.rf_property,
                                     self.replication_factor)
        self.check_rf(self.replication_factor)

    @cluster(num_nodes=4)
    def check_error_test(self):
        self.replication_factor = 3
        new_rf = -1

        with expect_exception(
                RpkException, lambda e: "INVALID_REPLICATION_FACTOR" in e.msg
                or "INVALID_CONFIG" in e.msg):
            self._rpk.alter_topic_config(self.topic_name, self.rf_property,
                                         new_rf)

        assert len(self.admin.list_reconfigurations()) == 0
        self.check_rf(self.replication_factor)

        new_rf = 0
        with expect_exception(
                RpkException, lambda e: "INVALID_REPLICATION_FACTOR" in e.msg
                or "INVALID_CONFIG" in e.msg):
            self._rpk.alter_topic_config(self.topic_name, self.rf_property,
                                         new_rf)
        assert len(self.admin.list_reconfigurations()) == 0
        self.check_rf(self.replication_factor)

        new_rf = 10000
        with expect_exception(
                RpkException, lambda e: "INVALID_REPLICATION_FACTOR" in e.msg
                or "INVALID_CONFIG" in e.msg):
            self._rpk.alter_topic_config(self.topic_name, self.rf_property,
                                         new_rf)
        assert len(self.admin.list_reconfigurations()) == 0
        self.check_rf(self.replication_factor)
