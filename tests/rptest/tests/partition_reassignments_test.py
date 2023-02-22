# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
import random
import json
import re
import subprocess

from ducktape.mark import ignore
from ducktape.tests.test import TestLoggerMaker
from ducktape.utils.util import wait_until
from rptest.services.cluster import cluster
from rptest.services.redpanda import LoggingConfig, RedpandaService, SecurityConfig
from rptest.services.verifiable_producer import VerifiableProducer
from rptest.services.admin import Admin
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.kcl import KCL
from typing import Optional


def get_topics_and_partitions(reassignments: dict):
    topic_names = []
    partition_idxs = []
    for reassignment in reassignments["partitions"]:
        topic_names.append(reassignment["topic"])
        partition_idxs.append(reassignment["partition"])

    return topic_names, partition_idxs


def check_execute_reassign_partitions(lines: list[str], reassignments: dict,
                                      logger: TestLoggerMaker):
    topic_names, partition_idxs = get_topics_and_partitions(reassignments)

    # Output has the following structure
    # Line 0 - "Current partition replica assignment"
    # Line 1 - empty
    # Line 2 - Json structure showing the current replica sets for each partition
    # Line 3 - empty
    # Line 4 - "Save this to use as the --reassignment-json-file option during rollback"
    # Line 5 - "Successfully started partition reassignments for topic1-p0,topic1-p1,...,topic2-p0,topic2-p1,..."

    # The last line should list topic partitions
    line = lines.pop().strip()
    assert line.startswith("Successfully started partition reassignments for ")
    topic_partitions = line.removeprefix(
        "Successfully started partition reassignments for ").split(',')

    tp_re = re.compile(r"^(?P<topic>[a-z\-]+?)-(?P<pid>[0-9]+?)$")
    for tp in topic_partitions:
        tp_match = tp_re.match(tp)
        logger.debug(f"topic partition match: {tp}, {tp_match}")
        assert tp_match is not None
        assert tp_match.group("topic") in topic_names
        assert int(tp_match.group("pid")) in partition_idxs

    # The next lines are exact strings
    assert lines.pop().strip(
    ) == "Save this to use as the --reassignment-json-file option during rollback"
    assert len(lines.pop()) == 0

    # Then a json structure
    current_assignment = json.loads(lines.pop().strip())
    assert type(current_assignment) == type({}), "Expected JSON object"

    # Then another exact string
    assert len(lines.pop()) == 0
    assert lines.pop().strip() == "Current partition replica assignment"

    if len(lines) != 0:
        raise RuntimeError(f"Unexpected output: {lines}")


def check_verify_reassign_partitions(lines: list[str], reassignments: dict,
                                     logger: TestLoggerMaker):

    topic_names, partition_idxs = get_topics_and_partitions(reassignments)

    # Check output
    # Output has the following structure
    # Line 0 - "Status of partition reassignment:"
    #        - One line for each topic partition
    #        - Empty string
    #        - "Clearing broker-level throttles on brokers node_id1,node_id2,..."
    #        - An InvalidConfigurationException because the kafka script attempts to alter broker configs
    #          on a per-node basis which Redpanda does not support

    lines.reverse()

    # First line is an exact string
    assert lines.pop().strip() == "Status of partition reassignment:"

    # Then there is one line for each topic partition
    tp_re_complete = re.compile(
        r"^Reassignment of partition (?P<topic>[a-z\-]+?)-(?P<pid>[0-9]+?) is complete.$"
    )
    tp_re_no_active = re.compile(
        r"^There is no active reassignment of partition (?P<topic>[a-z\-]+?)-(?P<pid>[0-9]+?), but replica set is.*$"
    )

    def re_match(line):
        m = tp_re_complete.match(line)
        if m is not None:
            return m

        return tp_re_no_active.match(line)

    line = lines.pop().strip()
    tp_match = re_match(line)
    logger.debug(f"topic partition match: {line} {tp_match}")
    while tp_match is not None:
        assert tp_match.group("topic") in topic_names
        assert int(tp_match.group("pid")) in partition_idxs
        line = lines.pop().strip()
        tp_match = re_match(line)
        logger.debug(f"topic partition match: {line} {tp_match}")

    if len(lines) != 0:
        raise RuntimeError(f"Unexpected output: {lines}")


def check_cancel_reassign_partitions(lines: list[str], reassignments: dict,
                                     logger: TestLoggerMaker):
    topic_names, partition_idxs = get_topics_and_partitions(reassignments)

    # Output has the following structure
    # Line 0 - "Successfully cancelled partition reassignments for: topic1-p0,topic1-p1,...,topic2-p0,topic2-p1,..."
    # Line 1 - "None of the specified partition moves are active."

    lines.reverse()

    # The last line should list topic partitions
    line = lines.pop().strip()
    assert line.startswith(
        "Successfully cancelled partition reassignments for: ")
    topic_partitions = line.removeprefix(
        "Successfully cancelled partition reassignments for: ").split(',')

    tp_re = re.compile(r"^(?P<topic>[a-z\-]+?)-(?P<pid>[0-9]+?)$")
    for tp in topic_partitions:
        tp_match = tp_re.match(tp)
        logger.debug(f"topic partition match: {tp}, {tp_match}")
        assert tp_match is not None
        assert tp_match.group("topic") in topic_names
        assert int(tp_match.group("pid")) in partition_idxs

    # The next lines are exact strings
    assert lines.pop().strip(
    ) == "None of the specified partition moves are active."

    if len(lines) != 0:
        raise RuntimeError(f"Unexpected output: {lines}")


def alter_partition_reassignments_with_kcl(
        kcl: KCL,
        topics: dict[str, dict[int, list[int]]],
        user_cred: Optional[dict[str, str]] = None,
        timeout_s: int = 10):
    ok_re = re.compile(r"^(?P<topic>[a-z\-]+?) +(?P<partition>[0-9]+?) +OK$")

    output = kcl.alter_partition_reassignments(topics,
                                               user_cred=user_cred,
                                               timeout_s=timeout_s)

    for line in output:
        assert ok_re.match(line.strip()) is not None

    return output


log_config = LoggingConfig('info',
                           logger_levels={
                               'kafka': 'trace',
                               'kafka/client': 'trace',
                               'cluster': 'trace',
                               'raft': 'trace'
                           })


class PartitionReassignmentsTest(RedpandaTest):
    REPLICAS_COUNT = 3
    PARTITION_COUNT = 3
    topics = [
        TopicSpec(partition_count=PARTITION_COUNT),
        TopicSpec(partition_count=PARTITION_COUNT),
    ]

    def __init__(self, test_context):
        super(PartitionReassignmentsTest,
              self).__init__(test_context=test_context,
                             num_brokers=4,
                             log_config=log_config)

    def get_missing_node_idx(self, lhs: list[int], rhs: list[int]):
        missing_nodes = set(lhs).difference(set(rhs))
        assert len(missing_nodes) == 1, "Expected one missing node"
        return missing_nodes.pop()

    def make_reassignments_for_cli(self, all_node_idx, initial_assignments):
        reassignments_json = {"version": 1, "partitions": []}
        log_dirs = ["any" for _ in range(self.PARTITION_COUNT)]

        for assignment in initial_assignments:
            for partition in assignment.partitions:
                assert len(partition.replicas) == self.REPLICAS_COUNT
                missing_node_idx = self.get_missing_node_idx(
                    all_node_idx, partition.replicas)
                # Replace one of the replicas with the missing one
                new_replica_assignment = partition.replicas
                new_replica_assignment[2] = missing_node_idx
                reassignments_json["partitions"].append({
                    "topic":
                    assignment.name,
                    "partition":
                    partition.id,
                    "replicas":
                    new_replica_assignment,
                    "log_dirs":
                    log_dirs
                })
                self.logger.debug(
                    f"{assignment.name} partition {partition.id}, new replica assignments: {new_replica_assignment}"
                )

        return reassignments_json

    def make_producer(self, topic_name: str, throughput: int):
        prod = VerifiableProducer(self.test_context,
                                  num_nodes=1,
                                  redpanda=self.redpanda,
                                  topic=topic_name,
                                  throughput=throughput)
        prod.start()
        return prod

    def wait_producers(self, producers: list[VerifiableProducer],
                       num_messages: int):
        assert num_messages > 0

        for prod in producers:
            wait_until(lambda: prod.num_acked > num_messages,
                           timeout_sec=180,
                           err_msg="Producer failed to produce messages for %ds." %\
                           60)
            self.logger.info("Stopping producer after writing up to offsets %s" %\
                            str(prod.last_acked_offsets))
            prod.stop()

    def initial_setup_steps(self,
                            producer_config: dict,
                            recovery_rate: Optional[int] = None):
        initial_assignments = self.client().describe_topics()
        self.logger.debug(f"Initial assignments: {initial_assignments}")

        all_node_idx = [
            self.redpanda.node_id(node) for node in self.redpanda.nodes
        ]
        self.logger.debug(f"All node idx: {all_node_idx}")

        if recovery_rate is not None:
            self.redpanda.set_cluster_config(
                {"raft_learner_recovery_rate": str(recovery_rate)})

        assert "throughput" in producer_config
        assert "topics" in producer_config

        producers = []
        for topic in producer_config["topics"]:
            producers.append(
                self.make_producer(topic,
                                   throughput=producer_config["throughput"]))

        return initial_assignments, all_node_idx, producers

    def execute_reassign_partitions(self,
                                    reassignments: dict,
                                    timeout_s: int = 10):
        kafka_tools = KafkaCliTools(self.redpanda)
        return kafka_tools.reassign_partitions(
            reassignments=reassignments,
            operation="execute",
            # RP may report that the topic does not exist, this can
            # happen when the recieving broker has out-of-date metadata. So
            # retry the request a few times.
            msg_retry="Topic or partition is undefined",
            timeout_s=timeout_s).splitlines()

    def verify_reassign_partitions(self,
                                   reassignments: dict,
                                   msg_retry: Optional[str] = None,
                                   timeout_s: int = 10):
        kafka_tools = KafkaCliTools(self.redpanda)
        return kafka_tools.reassign_partitions(
            reassignments=reassignments,
            operation="verify",
            # Retry the script if there is a reassignment still in progress
            msg_retry="is still in progress.",
            timeout_s=timeout_s).splitlines()

    def cancel_reassign_partitions(self, reassignments: dict):
        kafka_tools = KafkaCliTools(self.redpanda)
        return kafka_tools.reassign_partitions(
            reassignments=reassignments, operation="cancel").splitlines()

    @cluster(num_nodes=6)
    def test_reassignments_kafka_cli(self):
        initial_assignments, all_node_idx, producers = self.initial_setup_steps(
            producer_config={
                "topics": [self.topics[0].name, self.topics[1].name],
                "throughput": 1024
            })

        self.wait_producers(producers, num_messages=100000)

        reassignments_json = self.make_reassignments_for_cli(
            all_node_idx, initial_assignments)

        output = self.execute_reassign_partitions(
            reassignments=reassignments_json, timeout_s=30)
        check_execute_reassign_partitions(output, reassignments_json,
                                          self.logger)

        output = self.verify_reassign_partitions(
            reassignments=reassignments_json, timeout_s=180)
        check_verify_reassign_partitions(output, reassignments_json,
                                         self.logger)

    @cluster(num_nodes=6)
    def test_reassignments(self):
        all_topic_names = [self.topics[0].name, self.topics[1].name]
        initial_assignments, all_node_idx, producers = self.initial_setup_steps(
            producer_config={
                "topics": all_topic_names,
                "throughput": 1024
            })

        self.wait_producers(producers, num_messages=100000)

        reassignments = {}
        for assignment in initial_assignments:
            if assignment.name not in reassignments:
                reassignments[assignment.name] = {}

            for partition in assignment.partitions:
                assert partition.id not in reassignments[assignment.name]
                assert len(partition.replicas) == self.REPLICAS_COUNT
                missing_node_idx = self.get_missing_node_idx(
                    all_node_idx, partition.replicas)
                # Trigger replica add and removal by replacing one of the replicas
                partition.replicas[2] = missing_node_idx
                reassignments[assignment.name][
                    partition.id] = partition.replicas

        self.logger.debug(
            f"Replacing replicas. New assignments: {reassignments}")
        kcl = KCL(self.redpanda)
        alter_partition_reassignments_with_kcl(kcl, reassignments)

        all_partition_idx = [p for p in range(self.PARTITION_COUNT)]

        # Test ListPartitionReassignments with specific topic-partitions
        responses = kcl.list_partition_reassignments({
            self.topics[0].name:
            all_partition_idx,
            self.topics[1].name:
            all_partition_idx
        })

        all_node_idx_set = set(all_node_idx)
        for res in responses:
            assert res.topic in all_topic_names
            assert type(res.partition) == int
            assert res.partition in all_partition_idx
            assert set(res.replicas).issubset(all_node_idx_set)
            assert set(res.adding_replicas).issubset(all_node_idx_set)
            assert set(res.removing_replicas).issubset(all_node_idx_set)

        self.logger.debug("Wait for reassignments to finish")

        # Wait for the reassignment to finish by checking all
        # in-progress reassignments
        def reassignments_done():
            responses = kcl.list_partition_reassignments()
            self.logger.debug(responses)

            for res in responses:
                assert res.topic in all_topic_names
                assert type(res.partition) == int
                assert res.partition in all_partition_idx
                assert set(res.replicas).issubset(all_node_idx_set)

                # Retry if any topic_partition has ongoing reassignments
                if len(res.adding_replicas) > 0 or len(
                        res.removing_replicas) > 0:
                    return False
            return True

        wait_until(reassignments_done, timeout_sec=180, backoff_sec=1)

        # Test even replica count by adding a replica. Expect a failure because
        # even replication factor is not supported in Redpanda
        for topic in reassignments:
            for pid in reassignments[topic]:
                # Add a node to the replica set
                missing_node_idx = self.get_missing_node_idx(
                    all_node_idx, reassignments[topic][pid])
                reassignments[topic][pid].append(missing_node_idx)

        self.logger.debug(
            f"Even replica count by adding. Expect fail. New assignments: {reassignments}"
        )

        kcl = KCL(self.redpanda)

        def try_even_replication_factor(topics):
            try:
                alter_partition_reassignments_with_kcl(kcl, topics)
                raise Exception(
                    "Even replica count accepted but it should be rejected")
            except RuntimeError as ex:
                if str(ex) == "Number of replicas != topic replication factor":
                    pass
                else:
                    raise

        try_even_replication_factor(reassignments)

        # Test even replica count by removing two replicas. Expect a failure
        # because even replication factor is not supported in Redpanda
        for topic in reassignments:
            for pid in reassignments[topic]:
                reassignments[topic][pid].pop()
                reassignments[topic][pid].pop()

        self.logger.debug(
            f"Even replica count by removal. Expect fail. New assignments: {reassignments}"
        )
        try_even_replication_factor(reassignments)

    @cluster(num_nodes=6)
    def test_reassignments_cancel(self):
        initial_assignments, all_node_idx, producers = self.initial_setup_steps(
            producer_config={
                "topics": [self.topics[0].name, self.topics[1].name],
                "throughput": 1024
            },
            # Set a low throttle to slowdown partition move enough that there is
            # something to cancel
            recovery_rate=10)

        self.wait_producers(producers, num_messages=10000)

        reassignments_json = self.make_reassignments_for_cli(
            all_node_idx, initial_assignments)

        output = self.execute_reassign_partitions(
            reassignments=reassignments_json, timeout_s=30)
        check_execute_reassign_partitions(output, reassignments_json,
                                          self.logger)

        output = self.cancel_reassign_partitions(
            reassignments=reassignments_json)
        check_cancel_reassign_partitions(output, reassignments_json,
                                         self.logger)

        output = self.verify_reassign_partitions(
            reassignments=reassignments_json, timeout_s=30)
        check_verify_reassign_partitions(output, reassignments_json,
                                         self.logger)

    @cluster(num_nodes=4)
    def test_disable_alter_reassignment_api(self):
        # works
        kcl = KCL(self.redpanda)
        kcl.alter_partition_reassignments({})

        # disable api
        self.redpanda.set_cluster_config(
            dict(kafka_enable_partition_reassignment=False))

        # doesn't work
        try:
            kcl.alter_partition_reassignments({})
            assert "alter partition reassignments should have failed"
        except subprocess.CalledProcessError as e:
            assert "AlterPartitionReassignment API is disabled. See" in e.output


class PartitionReassignmentsACLsTest(RedpandaTest):
    topics = [TopicSpec()]
    password = "password"
    algorithm = "SCRAM-SHA-256"

    def __init__(self, test_context):
        security = SecurityConfig()
        security.kafka_enable_authorization = True
        security.endpoint_authn_method = 'sasl'
        super(PartitionReassignmentsACLsTest,
              self).__init__(test_context=test_context,
                             num_brokers=4,
                             log_config=log_config,
                             security=security)

    @cluster(num_nodes=4)
    def test_reassignments_with_acls(self):
        admin = Admin(self.redpanda)
        username = "regular_user"
        admin.create_user(username, self.password, self.algorithm)

        # wait for user to propagate to nodes
        def user_exists():
            for node in self.redpanda.nodes:
                users = admin.list_users(node=node)
                if username not in users:
                    return False
            return True

        wait_until(user_exists, timeout_sec=10, backoff_sec=1)

        # Only one partition is in this test and it does not matter if a
        # partition is moved since this test is intended to check for
        # ACLs only. So we hardcode in the replica set assignment
        reassignments = {self.topic: {0: [1, 2, 3]}}
        self.logger.debug(f"New replica assignments: {reassignments}")

        user_cred = {
            "user": username,
            "passwd": self.password,
            "method": self.algorithm.lower().replace('-', '_')
        }
        kcl = KCL(self.redpanda)
        try:
            alter_partition_reassignments_with_kcl(kcl,
                                                   reassignments,
                                                   user_cred=user_cred)
            raise Exception(
                f'AlterPartition with user {user_cred} passed. Expected fail.')
        except subprocess.CalledProcessError as e:
            if e.output.startswith("CLUSTER_AUTHORIZATION_FAILED"):
                pass
            else:
                raise

        tps_to_list = {self.topic: [0]}
        try:
            kcl.list_partition_reassignments(tps_to_list, user_cred=user_cred)
            raise Exception(
                f'ListPartition with user {user_cred} passed. Expected fail.')
        except subprocess.CalledProcessError as e:
            if e.output.startswith("CLUSTER_AUTHORIZATION_FAILED"):
                pass
            else:
                raise

        super_username, super_password, super_algorithm = self.redpanda.SUPERUSER_CREDENTIALS
        user_cred = {
            "user": super_username,
            "passwd": super_password,
            "method": super_algorithm.lower().replace('-', '_')
        }
        alter_partition_reassignments_with_kcl(kcl,
                                               reassignments,
                                               user_cred=user_cred)

        def reassignments_done():
            responses = kcl.list_partition_reassignments(tps_to_list,
                                                         user_cred=user_cred)
            self.logger.debug(responses)

            # Any response means the reassignment is on-going, so before
            # we retry, check that the output is expected
            if len(responses) > 0:
                # Should only be one value, if any, since we are reassigning
                # one partition
                assert len(responses) == 1
                res = responses.pop()
                assert res.replicas == reassignments[self.topic][0]
                return False

            return True

        wait_until(reassignments_done, timeout_sec=10, backoff_sec=1)
