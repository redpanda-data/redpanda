# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.errors import DucktapeError

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool

import subprocess
import os
from time import sleep

from confluent_kafka import (Consumer, Producer, KafkaException)

from ducktape.mark import matrix, ignore

ERROR_LOGS = [
    # e.g. rpc - server.cc:91 - kafka rpc protocol - Error[applying protocol] remote address: 172.18.0.11:34024 - std::out_of_range (Invalid skip(n). Expected:220385, but skipped:79943)
    "rpc - server.cc.*std::out_of_range.*"
]


def on_delivery(err, msg):
    if err is not None:
        raise KafkaException(err)


class LibrdkafkaTest(RedpandaTest):
    """
    Execute the librdkafka test suite against redpanda.
    """
    TESTS_DIR = "/opt/librdkafka/tests"
    CONF_FILE = os.path.join(TESTS_DIR, "test.conf")
    """
    Verify that segment indices are recovered on startup.
    """
    def __init__(self, test_context):
        extra_rp_conf = {
            "auto_create_topics_enabled": True,
            "enable_idempotence": True,
            "enable_transactions": True,
            "transaction_coordinator_replication": 3,
            "id_allocator_replication": 3,
            "default_topic_replications": 1,
            "default_topic_partitions": 4,
            "enable_leader_balancer": False,
            "enable_auto_rebalance_on_node_add": False
        }

        super(LibrdkafkaTest, self).__init__(test_context=test_context,
                                             extra_rp_conf=extra_rp_conf)

    def warmup(self):
        producer = Producer({
            "bootstrap.servers": self.redpanda.brokers(),
            "enable.idempotence": True,
            "retries": 5
        })
        producer.produce("topic1",
                         key="key1".encode('utf-8'),
                         value="value1".encode('utf-8'),
                         callback=on_delivery)
        producer.flush()
        consumer = Consumer({
            "bootstrap.servers": self.redpanda.brokers(),
            "group.id": "group1",
            "auto.offset.reset": "earliest"
        })
        consumer.subscribe(["topic1"])
        producer = Producer({
            "bootstrap.servers": self.redpanda.brokers(),
            "enable.idempotence": True,
            "retries": 5,
            "transactional.id": "tx1"
        })
        producer.init_transactions()

    # yapf: disable
    @cluster(num_nodes=3, log_allow_list=ERROR_LOGS)
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
    @ignore(test_num=14, num_brokers=1)
    @ignore(test_num=34, num_brokers=1)
    @ignore(test_num=51, num_brokers=1)
    @ignore(test_num=82, num_brokers=1)
    @ignore(test_num=99, num_brokers=1)
    @ignore(test_num=30, num_brokers=1)
    # @ignore appears to not be quite smart enough to handle the partial
    # parameterization so we repeat them here with num_brokers=3. this would be
    # a nice enhancement that we could upstream.
    @ignore(test_num=14, num_brokers=3)
    @ignore(test_num=34, num_brokers=3)
    @ignore(test_num=51, num_brokers=3)
    @ignore(test_num=82, num_brokers=3)
    @ignore(test_num=99, num_brokers=3)
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
    @matrix(test_num=range(101), num_brokers=[1, 3])
    # yapf: enable
    def test_librdkafka(self, test_num, num_brokers):
        with open(LibrdkafkaTest.CONF_FILE, "w") as f:
            brokers = self.redpanda.brokers()
            f.write("metadata.broker.list={}".format(brokers))

        retries = 12
        while True:
            retries -= 1
            try:
                self.warmup()
                break
            except:
                if retries == 0:
                    raise
                sleep(5)

        retries = 12
        while True:
            # retrying failing tests to avoid transient 'Broker: Not coordinator' error
            retries -= 1

            p = subprocess.Popen(["make"],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT,
                                 shell=True,
                                 cwd=LibrdkafkaTest.TESTS_DIR,
                                 env=dict(TESTS="%04d" % test_num,
                                          **os.environ))
            for line in iter(p.stdout.readline, b''):
                self.logger.debug(line.rstrip())
            p.wait()
            if p.returncode == 0:
                return
            if retries == 0:
                raise RuntimeError("librdkafka test failed {}".format(
                    p.returncode))
            sleep(5)
