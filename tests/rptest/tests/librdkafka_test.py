# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import os

from rptest.services.cluster import cluster
from ducktape.mark import ignore, matrix

from rptest.services.librdkafka_test_case import LibrdkafkaTestcase
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception
from rptest.utils.mode_checks import skip_debug_mode
from rptest.clients.python_librdkafka import PythonLibrdkafka

from confluent_kafka.admin import (AclBinding, AclBindingFilter, ResourceType,
                                   ResourcePatternType, AclOperation,
                                   AclPermissionType)
from confluent_kafka.cimpl import KafkaException


def tests_to_run():
    ignored_tests = set([
        14,
        51,
        67,
        90,
        99,
        103,
        # timequery issue
        52,
        # consumer offsets coordinator and tx_coordinator reported to early
        61,
        # librdkafka interceptor test, not important for Redpanda
        66,
        # compaction test - lack of low water mark support
        77,
        # create topic test - topic is being created despite invalid config
        81,
        # fetch max bytes test
        82,
        # additional topic configuration reported by Redpanda
        92,
        # autocreate topics test - security
        109,
        # cooperative rebalance issue
        113,
        # ACL test - TODO: add security config to the test
        115,
        119,
        # tests that are flaky in CI
        30,
        84,
        # using mocked cluster, not relevant
        105,
        # failing always - requires proper RCA done
        75,
    ])
    return [t for t in range(120) if t not in ignored_tests]


class LibrdkafkaTest(RedpandaTest):
    """
    Execute the librdkafka test suite against redpanda.
    """
    TESTS_DIR = "/opt/librdkafka/tests"
    CONF_FILE = os.path.join(TESTS_DIR, "test.conf")

    def __init__(self, context):
        super(LibrdkafkaTest, self).__init__(context,
                                             num_brokers=3,
                                             extra_rp_conf={
                                                 "auto_create_topics_enabled":
                                                 True,
                                                 "default_topic_partitions": 4
                                             })

    @cluster(num_nodes=4)
    @matrix(test_num=tests_to_run())
    @skip_debug_mode
    def test_librdkafka(self, test_num):
        tc = LibrdkafkaTestcase(self.test_context, self.redpanda, test_num)
        tc.start()
        tc.wait()

        assert tc.error is None, f"Failure in librdkafka test case {test_num:04}"

    @cluster(num_nodes=3)
    def test_create_role_acl(self):
        client = PythonLibrdkafka(self.redpanda)
        admin = client.get_client()
        ROLE_NAME = "RedpandaRole:foo"
        binding = AclBinding(ResourceType.TOPIC, "*",
                             ResourcePatternType.LITERAL, ROLE_NAME, '*',
                             AclOperation.DESCRIBE, AclPermissionType.ALLOW)

        res = admin.create_acls([binding], request_timeout=10)
        for k in res:
            res[k].result()

        filter = AclBindingFilter(ResourceType.ANY, "*",
                                  ResourcePatternType.ANY, ROLE_NAME, '*',
                                  AclOperation.ANY, AclPermissionType.ALLOW)

        acls = admin.describe_acls(filter).result()
        assert len(acls) == 1, f"Wrong number of acls: {len(acls)}"
        assert acls[
            0].principal == ROLE_NAME, f"Expected principal={ROLE_NAME} got {acls[0].principal}"

    @cluster(num_nodes=3)
    def test_create_bad_acl(self):
        """
        Verify that Redpanda rejects (and librdkafka correctly handles)
        ACL bindings with a bogus principal type
        """
        client = PythonLibrdkafka(self.redpanda)
        admin = client.get_client()
        ROLE_NAME = "InvalidPrefix:foo"
        binding = AclBinding(ResourceType.TOPIC, "*",
                             ResourcePatternType.LITERAL, ROLE_NAME, '*',
                             AclOperation.DESCRIBE, AclPermissionType.ALLOW)

        with expect_exception(
                KafkaException,
                lambda e: "Invalid principal name" in e.args[0].str()):
            res = admin.create_acls([binding], request_timeout=10)
            for k in res:
                res[k].result()

        filter = AclBindingFilter(ResourceType.ANY, "*",
                                  ResourcePatternType.ANY, ROLE_NAME, '*',
                                  AclOperation.ANY, AclPermissionType.ALLOW)

        acls = admin.describe_acls(filter).result()
        assert len(
            acls) == 0, f"Expected no ACLs (binding rejected), got: {acls}"
