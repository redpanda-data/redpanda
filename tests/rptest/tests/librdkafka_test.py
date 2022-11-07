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
from ducktape.mark import matrix, ignore

from rptest.services.librdkafka_test_case import LibrdkafkaTestcase
from rptest.tests.redpanda_test import RedpandaTest

from rptest.utils.mode_checks import skip_debug_mode


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
    # coordinator being reported before it is an elected leader
    @ignore(test_num=14)
    @ignore(test_num=51)
    @ignore(test_num=67)
    @ignore(test_num=90)
    @ignore(test_num=99)
    @ignore(test_num=103)
    # timequery issue
    @ignore(test_num=52)
    # consumer offsets coordinator and tx_coordinator reported to early
    @ignore(test_num=61)
    # librdkafka interceptor test, not important for Redpanda
    @ignore(test_num=66)
    # compaction test - lack of low water mark support
    @ignore(test_num=77)
    # create topic test - topic is being created despite invalid config
    @ignore(test_num=81)
    # fetch max bytes test
    @ignore(test_num=82)
    # additional topic configuration reported by Redpanda
    @ignore(test_num=92)
    # autocreate topics test - security
    @ignore(test_num=109)
    # cooperative rebalance issue
    @ignore(test_num=113)
    # ACL test - TODO: add security config to the test
    @ignore(test_num=115)
    @ignore(test_num=119)
    # tests that are flaky in CI
    @ignore(test_num=30)
    @ignore(test_num=84)
    @matrix(test_num=range(120))
    @skip_debug_mode
    def test_librdkafka(self, test_num):
        tc = LibrdkafkaTestcase(self.test_context, self.redpanda, test_num)
        tc.start()
        tc.wait()
