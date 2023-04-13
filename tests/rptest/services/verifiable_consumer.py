# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Modifications Copyright 2021 Redpanda Data, Inc.
# - Reformatted code
# - Replaced dependency on Kafka with Redpanda

import json
import os
import signal
from collections import namedtuple

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.background_thread import BackgroundThreadService
from datetime import datetime

TopicPartition = namedtuple('TopicPartition', ['topic', 'partition'])


class ConsumerState:
    Started = 1
    Dead = 2
    Rebalancing = 3
    Joined = 4


class ConsumerEventHandler(object):
    def __init__(self, node, verify_offsets, idx):
        self.node = node
        self.idx = idx
        self.state = ConsumerState.Dead
        self.revoked_count = 0
        self.assigned_count = 0
        self.assignment = []
        self.position = {}
        self.committed = {}
        self.total_consumed = 0
        self.verify_offsets = verify_offsets

    def handle_shutdown_complete(self):
        self.state = ConsumerState.Dead
        self.assignment = []
        self.position = {}

    def handle_startup_complete(self):
        self.state = ConsumerState.Started

    def handle_offsets_committed(self, event, node, logger):
        if event["success"]:
            for offset_commit in event["offsets"]:
                if offset_commit.get("error", "") != "":
                    logger.debug("%s: Offset commit failed for: %s" %
                                 (str(node.account), offset_commit))
                    continue

                topic = offset_commit["topic"]
                partition = offset_commit["partition"]
                tp = TopicPartition(topic, partition)
                offset = offset_commit["offset"]
                assert tp in self.assignment, \
                    "Committed offsets for partition %s not assigned (current assignment: %s)" % \
                    (str(tp), str(self.assignment))
                assert tp in self.position, "No previous position for %s: %s" % (
                    str(tp), event)
                assert self.position[tp] >= offset, \
                    "The committed offset %d was greater than the current position %d for partition %s" % \
                    (offset, self.position[tp], str(tp))
                self.committed[tp] = offset

    def handle_records_consumed(self, event, logger):
        assert self.state == ConsumerState.Joined, \
            "Consumed records should only be received when joined (current state: %s)" % str(self.state)

        for record_batch in event["partitions"]:
            tp = TopicPartition(topic=record_batch["topic"],
                                partition=record_batch["partition"])
            min_offset = record_batch["minOffset"]
            max_offset = record_batch["maxOffset"]

            assert tp in self.assignment, \
                "Consumed records for partition %s which is not assigned (current assignment: %s)" % \
                (str(tp), str(self.assignment))
            if tp not in self.position or self.position[tp] == min_offset:
                self.position[tp] = max_offset + 1
            else:
                msg = "Consumed from an unexpected offset (%d, %d) for partition %s" % \
                      (self.position.get(tp), min_offset, str(tp))
                if self.verify_offsets:
                    raise AssertionError(msg)
                else:
                    if tp in self.position:
                        self.position[tp] = max_offset + 1
                    logger.warn(msg)
        self.total_consumed += event["count"]

    def handle_partitions_revoked(self, event):
        self.revoked_count += 1
        self.state = ConsumerState.Rebalancing
        self.position = {}

    def handle_partitions_assigned(self, event):
        self.assigned_count += 1
        self.state = ConsumerState.Joined
        assignment = []
        for topic_partition in event["partitions"]:
            topic = topic_partition["topic"]
            partition = topic_partition["partition"]
            assignment.append(TopicPartition(topic, partition))
        self.assignment = assignment

    def handle_offsets_fetched(self, event):
        for committed_offset in event["offsets"]:

            topic = committed_offset["topic"]
            partition = committed_offset["partition"]
            tp = TopicPartition(topic, partition)
            offset = committed_offset["offset"]
            assert tp in self.assignment, \
                "Committed offsets for partition %s not assigned (current assignment: %s)" % \
                (str(tp), str(self.assignment))

            self.committed[tp] = offset

    def handle_kill_process(self, clean_shutdown):
        # if the shutdown was clean, then we expect the explicit
        # shutdown event from the consumer
        if not clean_shutdown:
            self.handle_shutdown_complete()

    def current_assignment(self):
        return list(self.assignment)

    def current_position(self, tp):
        if tp in self.position:
            return self.position[tp]
        else:
            return None

    def last_commit(self, tp):
        if tp in self.committed:
            return self.committed[tp]
        else:
            return None


class VerifiableConsumer(BackgroundThreadService):
    """
    This service wraps org.apache.kafka.tools.VerifiableConsumer for use in
    system testing. 
    """

    PERSISTENT_ROOT = "/mnt/verifiable_consumer"
    STDOUT_CAPTURE = os.path.join(PERSISTENT_ROOT,
                                  "verifiable_consumer.stdout")
    STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT,
                                  "verifiable_consumer.stderr")
    LOG_DIR = os.path.join(PERSISTENT_ROOT, "logs")
    LOG_FILE = os.path.join(LOG_DIR, "verifiable_consumer.log")
    LOG4J_CONFIG = os.path.join(PERSISTENT_ROOT, "tools-log4j.properties")
    CONFIG_FILE = os.path.join(PERSISTENT_ROOT,
                               "verifiable_consumer.properties")

    logs = {
        "verifiable_consumer_stdout": {
            "path": STDOUT_CAPTURE,
            "collect_default": False
        },
        "verifiable_consumer_stderr": {
            "path": STDERR_CAPTURE,
            "collect_default": False
        },
        "verifiable_consumer_log": {
            "path": LOG_FILE,
            "collect_default": True
        }
    }

    class WorkerState:
        def __init__(self, account) -> None:
            self.account_str = str(account)
            self.position_first = {}
            self.position = {}
            self.committed = {}

        def record_position(self, tp: TopicPartition, min_offset, max_offset,
                            logger, verify_offsets: bool):
            # verify that the position never gets behind the current commit.
            if tp in self.committed and self.committed[tp] > min_offset:
                msg = "%s: Consumed position %d is behind the current "\
                    "committed offset %d for partition %s" % \
                    (self.account_str, min_offset, self.committed[tp], str(tp))
                if self.verify_offsets:
                    raise AssertionError(msg)
                else:
                    logger.warn(msg)

            # verify that position is growing without gaps
            if tp in self.position and self.position[tp] != min_offset:
                # the consumer cannot generally guarantee that the position
                # increases monotonically without gaps in the face of hard
                # failures, so we only log a warning when this happens
                logger.warn(
                    "%s: Expected next consumed offset of %d for partition %s, "
                    "but instead saw %d" %
                    (self.account_str, self.position[tp], str(tp), min_offset))

            self.position[tp] = max_offset + 1
            if tp in self.position_first:
                if min_offset <= self.position_first[tp]:
                    logger.warn(
                        f"{self.account_str}: Expected the beginning of the "
                        f"consumed offset range (minOffset: {min_offset}) for "
                        f"partition {str(tp)} to be greater than any other such "
                        f"value from previous batches. The minimum so far is "
                        f"{self.position_first[tp]}")
                self.position_first[tp] = min(self.position_first[tp],
                                              min_offset)
            else:
                self.position_first[tp] = min_offset

        def record_committed(self, tp: TopicPartition, offset,
                             verify_position: bool):
            if verify_position:
                assert self.position[tp] >= offset, \
                    "%s: Committed offset %d for partition %s is ahead of the current position %d" % \
                    (self.account_str, offset, str(tp), self.position[tp])
            self.committed[tp] = offset

        def current_position(self, tp):
            if tp in self.position:
                return self.position[tp]
            else:
                return None

        def last_commit(self, tp):
            if tp in self.committed:
                return self.committed[tp]
            else:
                return None

    def __init__(self,
                 context,
                 num_nodes,
                 redpanda,
                 topic,
                 group_id,
                 static_membership=False,
                 max_messages=-1,
                 session_timeout_sec=30,
                 enable_autocommit=False,
                 assignment_strategy=None,
                 stop_timeout_sec=45,
                 on_record_consumed=None,
                 reset_policy="earliest",
                 verify_offsets=True):
        super(VerifiableConsumer, self).__init__(context, num_nodes)
        self.redpanda = redpanda
        self.topic = topic
        self.group_id = group_id
        self.reset_policy = reset_policy
        self.static_membership = static_membership
        self.max_messages = max_messages
        self.session_timeout_sec = session_timeout_sec
        self.enable_autocommit = enable_autocommit
        self.assignment_strategy = assignment_strategy
        self.prop_file = ""
        self.stop_timeout_sec = stop_timeout_sec
        self.on_record_consumed = on_record_consumed
        self.verify_offsets = verify_offsets
        self.last_consumed = None

        self.event_handlers = {}
        # global state is per worker, in order to handle scenarios when upon
        # shutdown, when the first consumer is stopped, consumer group is
        # rebalanced and other consumer begin receiving from its partitions
        self.global_state = {}

    def _worker(self, idx, node):
        state = VerifiableConsumer.WorkerState(node.account)
        self.global_state[idx] = state

        with self.lock:
            if node not in self.event_handlers:
                self.event_handlers[node] = ConsumerEventHandler(
                    node, self.verify_offsets, idx)
            handler = self.event_handlers[node]

        node.account.ssh("mkdir -p %s" % VerifiableConsumer.PERSISTENT_ROOT,
                         allow_fail=False)

        # Create and upload log properties
        log_config = self.render('tools_log4j.properties',
                                 log_file=VerifiableConsumer.LOG_FILE)
        node.account.create_file(VerifiableConsumer.LOG4J_CONFIG, log_config)

        # Create and upload config file
        self.logger.info("verifiable_consumer.properties:")
        self.logger.info(self.prop_file)
        node.account.create_file(VerifiableConsumer.CONFIG_FILE,
                                 self.prop_file)

        # apply group.instance.id to the node for static membership validation
        node.group_instance_id = None
        if self.static_membership:
            node.group_instance_id = self.group_id + "-instance-" + str(idx)

        cmd = self.start_cmd(node)
        self.logger.debug("VerifiableConsumer %d command: %s" % (idx, cmd))

        for line in node.account.ssh_capture(cmd):
            event = self.try_parse_json(node, line.strip())
            if event is not None:
                with self.lock:
                    name = event["name"]
                    if name == "shutdown_complete":
                        handler.handle_shutdown_complete()
                    elif name == "startup_complete":
                        handler.handle_startup_complete()
                    elif name == "offsets_committed":
                        handler.handle_offsets_committed(
                            event, node, self.logger)
                        self._update_global_committed(event, state)
                    elif name == "records_consumed":
                        handler.handle_records_consumed(event, self.logger)
                        self._update_global_position(event, state)
                    elif name == "record_data" and self.on_record_consumed:
                        self.on_record_consumed(event, node)
                    elif name == "partitions_revoked":
                        handler.handle_partitions_revoked(event)
                    elif name == "partitions_assigned":
                        handler.handle_partitions_assigned(event)
                    elif name == "offsets_fetched":
                        handler.handle_offsets_fetched(event)
                        self._update_global_committed_fetched(event, state)
                    else:
                        self.logger.debug("%s: ignoring unknown event: %s" %
                                          (str(node.account), event))

    def _update_global_position(self, consumed_event, state: WorkerState):
        for consumed_partition in consumed_event["partitions"]:
            tp = TopicPartition(consumed_partition["topic"],
                                consumed_partition["partition"])
            state.record_position(tp, consumed_partition["minOffset"],
                                  consumed_partition["maxOffset"], self.logger,
                                  self.verify_offsets)
            self.last_consumed = datetime.now()

    def _update_global_committed(self, commit_event, state: WorkerState):
        if commit_event["success"]:
            for offset_commit in commit_event["offsets"]:
                tp = TopicPartition(offset_commit["topic"],
                                    offset_commit["partition"])
                state.record_committed(tp,
                                       offset_commit["offset"],
                                       verify_position=True)

    def _update_global_committed_fetched(self, fetch_offsets_ev,
                                         state: WorkerState):
        for offset in fetch_offsets_ev["offsets"]:
            tp = TopicPartition(offset["topic"], offset["partition"])
            state.record_committed(tp, offset["offset"], verify_position=False)

    def start_cmd(self, node):
        cmd = "java -cp /opt/redpanda-tests/java/e2e-verifiers/target/e2e-verifiers-1.0.jar"
        cmd += " -Dlog4j.configuration=file:%s" % VerifiableConsumer.LOG4J_CONFIG
        cmd += " org.apache.kafka.tools.VerifiableConsumer"
        if self.on_record_consumed:
            cmd += " --verbose"

        if node.group_instance_id:
            cmd += " --group-instance-id %s" % node.group_instance_id

        if self.assignment_strategy:
            cmd += " --assignment-strategy %s" % self.assignment_strategy

        if self.enable_autocommit:
            cmd += " --enable-autocommit "

        cmd += " --reset-policy %s --group-id %s --topic %s --broker-list %s --session-timeout %s" % \
               (self.reset_policy, self.group_id, self.topic,
                self.redpanda.brokers(),
                self.session_timeout_sec*1000)

        if self.max_messages > 0:
            cmd += " --max-messages %s" % str(self.max_messages)

        cmd += " --consumer.config %s" % VerifiableConsumer.CONFIG_FILE
        cmd += " 2>> %s | tee -a %s &" % (VerifiableConsumer.STDOUT_CAPTURE,
                                          VerifiableConsumer.STDOUT_CAPTURE)
        return cmd

    def pids(self, node):
        try:
            cmd = "jps | grep -i VerifiableConsumer | awk '{print $1}'"
            pid_arr = [
                pid for pid in node.account.ssh_capture(
                    cmd, allow_fail=True, callback=int)
            ]
            return pid_arr
        except (RemoteCommandError, ValueError):
            return []

    def try_parse_json(self, node, string):
        """Try to parse a string as json. Return None if not parseable."""
        try:
            return json.loads(string)
        except ValueError:
            self.logger.debug("%s: Could not parse as json: %s" %
                              (str(node.account), str(string)))
            return None

    def stop_all(self):
        for node in self.nodes:
            self.stop_node(node)

    def kill_node(self, node, clean_shutdown=True, allow_fail=False):
        sig = signal.SIGTERM
        if not clean_shutdown:
            sig = signal.SIGKILL
        for pid in self.pids(node):
            node.account.signal(pid, sig, allow_fail)

        with self.lock:
            self.event_handlers[node].handle_kill_process(clean_shutdown)

    def stop_node(self, node, clean_shutdown=True):
        self.kill_node(node, clean_shutdown=clean_shutdown)

        stopped = self.wait_node(node, timeout_sec=self.stop_timeout_sec)
        assert stopped, "Node %s: did not stop within the specified timeout of %s seconds" % \
                        (str(node.account), str(self.stop_timeout_sec))

    def clean_node(self, node):
        self.kill_node(node, clean_shutdown=False)
        node.account.ssh("rm -rf " + self.PERSISTENT_ROOT, allow_fail=False)

    def current_assignment(self):
        with self.lock:
            return {
                handler.node: handler.current_assignment()
                for handler in self.event_handlers.values()
            }

    def current_position(self, tp):
        with self.lock:
            current_positions = (s.current_position(tp)
                                 for s in self.global_state.values())
            return max((p for p in current_positions if p is not None),
                       default=None)

    def owner(self, tp):
        with self.lock:
            for handler in self.event_handlers.values():
                if tp in handler.current_assignment():
                    return handler.node
            return None

    def last_commit(self, tp):
        with self.lock:
            last_commits = (s.last_commit(tp)
                            for s in self.global_state.values())
            return max((c for c in last_commits if c is not None),
                       default=None)

    def total_consumed(self):
        with self.lock:
            return sum(handler.total_consumed
                       for handler in self.event_handlers.values())

    def num_rebalances(self):
        with self.lock:
            return max(handler.assigned_count
                       for handler in self.event_handlers.values())

    def num_revokes_for_alive(self, keep_alive=1):
        with self.lock:
            return max(handler.revoked_count
                       for handler in self.event_handlers.values()
                       if handler.idx <= keep_alive)

    def joined_nodes(self):
        with self.lock:
            return [
                handler.node for handler in self.event_handlers.values()
                if handler.state == ConsumerState.Joined
            ]

    def rebalancing_nodes(self):
        with self.lock:
            return [
                handler.node for handler in self.event_handlers.values()
                if handler.state == ConsumerState.Rebalancing
            ]

    def dead_nodes(self):
        with self.lock:
            return [
                handler.node for handler in self.event_handlers.values()
                if handler.state == ConsumerState.Dead
            ]

    def alive_nodes(self):
        with self.lock:
            return [
                handler.node for handler in self.event_handlers.values()
                if handler.state != ConsumerState.Dead
            ]

    def get_committed_offsets(self):
        with self.lock:
            return dict((i, worker.committed)
                        for i, worker in self.global_state.items())

    def get_last_consumed(self):
        with self.lock:
            return self.last_consumed

    def verify_position_offsets_consistency(self):
        msg = []
        with self.lock:
            tps = set().union(s.position.keys
                              for s in self.global_state.values())
            for tp in tps:
                # if there was more than 1 worker receiving messages from a tp,
                # verify there is no gap and no overlap in position intervals
                # between the workers
                fail_pre = False
                for idx, s in self.global_state.items():
                    if not tp in s.position_first:
                        msg.append(f"Start of consumed offset range "\
                            f"not recorded for partiton {str(tp)}, worker "\
                            f"{idx} {s.account_str}")
                        fail_pre = True
                if fail_pre:
                    continue

                ranges = [(s.position_first[tp], s.position[tp])
                          for s in self.global_state.values()]
                ranges.sort()
                adj_pairs = (ranges[n:n + 2] for n in range(len(ranges) - 1))
                if not all(pair[0][1] == pair[1][0] for pair in adj_pairs):
                    msg.append(
                        f"A gap in consumed offsets is detected in partition "
                        f"{str(tp)}. List of consumed ranges per worker "
                        f"(consumer instance): {ranges}")

        return len(msg) != 0, msg
