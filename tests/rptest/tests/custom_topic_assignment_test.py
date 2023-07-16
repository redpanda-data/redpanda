# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.rpk import RpkTool
from rptest.tests.redpanda_test import RedpandaTest
from confluent_kafka.admin import NewTopic
from confluent_kafka.error import KafkaException, KafkaError

from rptest.clients.kafka_cli_tools import KafkaCliTools


class CustomTopicAssignmentTest(RedpandaTest):
    def __init__(self, test_context):
        # Disable balancer so replicas are not shuffled around after creation
        rp_conf = {"partition_autobalancing_mode": "off"}
        super(CustomTopicAssignmentTest,
              self).__init__(test_context=test_context,
                             num_brokers=5,
                             extra_rp_conf=rp_conf)

    def create_and_validate(self, name, custom_assignment):
        self.redpanda.logger.info(
            f"creating topic {name} with {custom_assignment}")
        rpk = RpkTool(self.redpanda)

        self.client().create_topic_with_assignment(name, custom_assignment)

        def replica_matches():
            replicas_per_partition = {}
            for p in rpk.describe_topic(name):
                replicas_per_partition[p.id] = list(p.replicas)
            self.redpanda.logger.debug(
                f"requested replicas: {custom_assignment}, current replicas: {replicas_per_partition}"
            )

            for p_id, replicas in enumerate(custom_assignment):
                if p_id not in replicas_per_partition:
                    return False

                if set(replicas) != set(replicas_per_partition[p_id]):
                    return False

            return True

        # each assignment defines a partition
        wait_until(replica_matches, 10, backoff_sec=1)

    @cluster(num_nodes=5)
    def test_create_topic_with_custom_partition_assignment(self):
        # 3 partitions with single replica
        self.create_and_validate("topic-1", [[1], [3], [5]])
        # 3 partitions with replication factor of 2
        self.create_and_validate("topic-2", [[1, 2], [3, 4], [5, 1]])
        # 1 partition with replication factor of 3
        self.create_and_validate("topic-3", [[2, 4, 1]])

    @cluster(num_nodes=5)
    def test_custom_assignment_validation(self):
        client = PythonLibrdkafka(self.redpanda).get_client()

        def expect_failed_create_topic(name, custom_assignment,
                                       expected_error):
            topics = [
                NewTopic(name,
                         num_partitions=len(custom_assignment),
                         replica_assignment=custom_assignment)
            ]
            res = client.create_topics(topics, request_timeout=10)
            assert len(res) == 1
            fut = res[name]
            try:
                fut.result()
                assert False
            except KafkaException as e:
                kafka_error = e.args[0]
                self.redpanda.logger.debug(
                    f"topic {name} creation failed: {kafka_error}, expected error: {expected_error}"
                )
                assert kafka_error.code() == expected_error

        # not unique replicas
        expect_failed_create_topic("invalid-1", [[1, 1, 2]],
                                   KafkaError.INVALID_REQUEST)
        # not existing broker
        expect_failed_create_topic("invalid-1", [[1, 10, 2]],
                                   KafkaError.BROKER_NOT_AVAILABLE)

        # different replication factors
        expect_failed_create_topic("invalid-1", [[1, 2, 3], [4]],
                                   KafkaError.INVALID_REPLICATION_FACTOR)
