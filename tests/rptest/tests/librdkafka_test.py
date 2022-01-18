# Copyright 2020 Vectorized, Inc.
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

from ducktape.tests.test import Test
from rptest.services.redpanda import RedpandaService


class LibrdkafkaTest(Test):
    """
    Execute the librdkafka test suite against redpanda.
    """
    TESTS_DIR = "/opt/librdkafka/tests"
    CONF_FILE = os.path.join(TESTS_DIR, "test.conf")

    def __init__(self, context):
        super(LibrdkafkaTest, self).__init__(context)
        self._context = context
        self._extra_rp_conf = dict(
            auto_create_topics_enabled=True,
            default_topic_partitions=4,
        )
        self._redpanda = None

    def _start_redpanda(self, num_brokers):
        self._redpanda = RedpandaService(self._context,
                                         num_brokers,
                                         extra_rp_conf=self._extra_rp_conf)
        self._redpanda.start()

        with open(LibrdkafkaTest.CONF_FILE, "w") as f:
            brokers = self._redpanda.brokers()
            f.write("metadata.broker.list={}".format(brokers))

    def teardown(self):
        self._redpanda.stop()
        os.remove(LibrdkafkaTest.CONF_FILE)

    # yapf: disable
    @cluster(num_nodes=3)
    @ignore(test_num=19, num_brokers=1) # segfault in group membership https://app.clubhouse.io/vectorized/story/963/dereferencing-null-pointer-in-kafka-group-membership
    @ignore(test_num=52, num_brokers=1) # https://app.clubhouse.io/vectorized/story/997/librdkafka-tests-failing-due-to-consumer-out-of-range-timestamps
    @ignore(test_num=54, num_brokers=1) # timequery issues: https://app.clubhouse.io/vectorized/story/995/librdkafka-offset-time-query
    @ignore(test_num=63, num_brokers=1) # cluster-id: https://app.clubhouse.io/vectorized/story/939/generate-cluster-id-uuid-on-bootstrap-and-expose-through-metadata-request
    @ignore(test_num=67, num_brokers=1) # empty topic offset edge case: https://app.clubhouse.io/vectorized/story/940/consuming-from-empty-topic-should-return-eof
    @ignore(test_num=77, num_brokers=1) # topic compaction settings: https://app.clubhouse.io/vectorized/story/999/support-create-topic-configurations-for-compaction-retention-policies
    @ignore(test_num=92, num_brokers=1) # no support for v2 -> v1 message version conversion in the broker
    @ignore(test_num=44, num_brokers=1) # we do not support runtime changes to topic partition count
    @ignore(test_num=69, num_brokers=1) # we do not support runtime changes to topic partition count
    @ignore(test_num=81, num_brokers=1) # we do not support replica assignment
    @ignore(test_num=61, num_brokers=1) # transactions
    @ignore(test_num=76, num_brokers=1) # idempotent producer
    @ignore(test_num=86, num_brokers=1) # idempotent producer
    @ignore(test_num=90, num_brokers=1) # idempotent producer
    @ignore(test_num=94, num_brokers=1) # idempotent producer
    @ignore(test_num=98, num_brokers=1) # transactions
    # @ignore appears to not be quite smart enough to handle the partial
    # parameterization so we repeat them here with num_brokers=3. this would be
    # a nice enhancement that we could upstream.
    @ignore(test_num=19, num_brokers=3)
    @ignore(test_num=52, num_brokers=3)
    @ignore(test_num=54, num_brokers=3)
    @ignore(test_num=63, num_brokers=3)
    @ignore(test_num=67, num_brokers=3)
    @ignore(test_num=77, num_brokers=3)
    @ignore(test_num=92, num_brokers=3)
    @ignore(test_num=44, num_brokers=3)
    @ignore(test_num=69, num_brokers=3)
    @ignore(test_num=81, num_brokers=3)
    @ignore(test_num=61, num_brokers=3)
    @ignore(test_num=76, num_brokers=3)
    @ignore(test_num=86, num_brokers=3)
    @ignore(test_num=90, num_brokers=3)
    @ignore(test_num=94, num_brokers=3)
    @ignore(test_num=98, num_brokers=3)
    @matrix(test_num=range(101), num_brokers=[1, 3])
    # yapf: enable
    def test_librdkafka(self, test_num, num_brokers):
        self._start_redpanda(num_brokers)
        p = subprocess.Popen(["make"],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             shell=True,
                             cwd=LibrdkafkaTest.TESTS_DIR,
                             env=dict(TESTS="%04d" % test_num, **os.environ))
        for line in iter(p.stdout.readline, b''):
            self.logger.debug(line.rstrip())
        p.wait()
        if p.returncode != 0:
            raise RuntimeError("librdkafka test failed {}".format(
                p.returncode))
