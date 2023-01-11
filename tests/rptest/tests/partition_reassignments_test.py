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

from ducktape.tests.test import TestLoggerMaker
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest

from rptest.clients.types import TopicSpec
from rptest.clients.kafka_cli_tools import KafkaCliTools


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

    # Check the output in reverse order

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

    # Then another set of exact strings
    assert len(lines.pop()) == 0
    assert lines.pop().strip() == "Current partition replica assignment"

    if len(lines) != 0:
        raise RuntimeError(f"Unexpected output: {lines}")


def check_verify_reassign_partitions(lines: list[str], reassignments: dict,
                                     rp_node_idxs: list[str],
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

    # Reverse the order so we do not have to look at the entire Java exception
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

    # The previous line is an empty string
    assert len(line) == 0

    # Then a line with each node id
    line = lines.pop().strip()
    assert line.startswith("Clearing broker-level throttles on brokers ")
    node_idxs = line.removeprefix(
        "Clearing broker-level throttles on brokers ").split(",")
    node_idxs.sort()
    rp_node_idxs.sort()
    logger.debug(f'{node_idxs}  {rp_node_idxs}')
    assert node_idxs == rp_node_idxs

    # Then check for the expected exception
    assert "Setting broker properties on named brokers is unsupported" in lines.pop(
    ).strip()


class PartitionReassignmentsTest(RedpandaTest):
    PARTITION_COUNT = 3
    topics = [
        TopicSpec(partition_count=PARTITION_COUNT),
        TopicSpec(partition_count=PARTITION_COUNT),
    ]

    def __init__(self, test_context):
        super(PartitionReassignmentsTest,
              self).__init__(test_context=test_context, num_brokers=4)

    def get_missing_node_idx(self, lhs: list[int], rhs: list[int]):
        missing_nodes = set(lhs).difference(set(rhs))
        assert len(missing_nodes) == 1, "Expected one missing node"
        return missing_nodes.pop()

    @cluster(num_nodes=4)
    def test_reassignments(self):
        initial_assignments = self.client().describe_topics()
        self.logger.debug(f"Initial assignments: {initial_assignments}")

        all_node_idx = [
            self.redpanda.node_id(node) for node in self.redpanda.nodes
        ]
        self.logger.debug(f"All node idx: { all_node_idx}")

        reassignments_json = {"version": 1, "partitions": []}
        log_dirs = ["any" for _ in range(self.PARTITION_COUNT)]

        for assignment in initial_assignments:
            for partition in assignment.partitions:
                assert len(partition.replicas) == self.PARTITION_COUNT
                missing_node_idx = self.get_missing_node_idx(
                    all_node_idx, partition.replicas)
                # Replace one of the replicas with the missing one
                new_replica_assignment = partition.replicas
                new_replica_assignment[0] = missing_node_idx
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

        kafka_tools = KafkaCliTools(self.redpanda)
        output = kafka_tools.execute_reassign_partitions(
            reassignments=reassignments_json)
        check_execute_reassign_partitions(output, reassignments_json,
                                          self.logger)

        output = kafka_tools.verify_reassign_partitions(
            reassignments=reassignments_json)
        rp_node_idxs = [
            str(self.redpanda.node_id(node)) for node in self.redpanda.nodes
        ]
        check_verify_reassign_partitions(output, reassignments_json,
                                         rp_node_idxs, self.logger)
