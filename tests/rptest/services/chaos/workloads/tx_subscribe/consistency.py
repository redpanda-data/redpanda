# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import sys
import traceback
import logging
import os

from ...types import ConsistencyCheckError
from .log_utils import State, cmds, threads

logger = logging.getLogger("consistency")


class ReadRecord:
    def __init__(self):
        self.offset = None
        self.transformed_by = None
        self.produced_by = None
        self.produced_partition = None
        self.produced_oid = None


class ReadChecker:
    def __init__(self, workload_nodes):
        self.read_front = dict()
        for node in workload_nodes:
            self.read_front[node] = -1
        self.records = dict()
        self.next_offset = dict()

    def seen(self, seen_by, offset, transformed_by, produced_by,
             produced_partition, produced_oid):
        if seen_by not in self.read_front:
            raise Exception(
                f"read data fron an unknown workload node: {seen_by}")
        if transformed_by not in self.read_front:
            raise Exception(
                f"unknown workload node: transformed_by:{transformed_by}")
        if produced_by not in self.read_front:
            raise Exception(
                f"unknown workload node: produced_by:{produced_by}")
        if offset <= self.read_front[seen_by]:
            raise Exception(
                f"workload node {seen_by} observed {offset} after {self.read_front[seen_by]}"
            )
        if offset in self.records:
            op = self.records[offset]
            if op.transformed_by != transformed_by:
                raise Exception(
                    f"transformed_by:{transformed_by} seen by {seen_by} doesn't match already seen transformed_by:{op.transformed_by} for the same offset:{offset}"
                )
            if op.produced_by != produced_by:
                raise Exception(
                    f"produced_by:{produced_by} seen by {seen_by} doesn't match already seen produced_by:{op.produced_by} for the same offset:{offset}"
                )
            if op.produced_partition != produced_partition:
                raise Exception(
                    f"produced_partition:{produced_partition} seen by {seen_by} doesn't match already seen produced_partition:{op.produced_partition} for the same offset:{offset}"
                )
            if op.produced_oid != produced_oid:
                raise Exception(
                    f"produced_oid:{produced_oid} seen by {seen_by} doesn't match already seen produced_oid:{op.produced_oid} for the same offset:{offset}"
                )
            if self.read_front[seen_by] not in self.next_offset:
                raise Exception(
                    f"already seen offset:{offset} should have ancestor in next_offset; {self.read_front[seen_by]} is missing"
                )
            if self.next_offset[self.read_front[seen_by]] != offset:
                raise Exception(
                    f"workload {seen_by} skiped {self.next_offset[self.read_front[seen_by]]} during {self.read_front[seen_by]}->{offset}"
                )
            min_offset = offset
            for key in self.read_front.keys():
                if self.read_front[key] < min_offset:
                    min_offset = self.read_front[key]
            if min_offset >= 0 and self.read_front[seen_by] == min_offset:
                del self.records[min_offset]
                del self.next_offset[min_offset]
            self.read_front[seen_by] = offset
        else:
            if self.read_front[seen_by] in self.next_offset:
                raise Exception(
                    f"workload {seen_by} skiped {self.next_offset[self.read_front[seen_by]]} during {self.read_front[seen_by]}->{offset}"
                )
            self.next_offset[self.read_front[seen_by]] = offset
            self.read_front[seen_by] = offset
            op = ReadRecord()
            op.offset = offset
            op.transformed_by = transformed_by
            op.produced_by = produced_by
            op.produced_partition = produced_partition
            op.produced_oid = produced_oid
            self.records[offset] = op


class LogPlayer:
    def __init__(self, node, read_checker):
        self.node = node
        self.read_checker = read_checker

        self.curr_state = dict()
        self.thread_type = dict()
        self.has_violation = False
        self.measuring = False
        self.errors = 0

        self.ts_us = None

    def consuming_apply(self, thread_id, parts):
        if self.curr_state[thread_id] == State.SEEN:
            try:
                self.read_checker.seen(self.node, int(parts[3]), parts[4],
                                       parts[5], int(parts[6]), int(parts[7]))
            except:
                self.has_violation = True
                e, v = sys.exc_info()[:2]
                trace = traceback.format_exc()
                logger.error(v)
                logger.error(trace)

    def is_violation(self, line):
        if line == None:
            return False
        parts = line.rstrip().split('\t')
        if len(parts) < 3:
            return False
        if parts[2] not in cmds:
            return False
        return cmds[parts[2]] == State.VIOLATION

    def apply(self, line):
        if self.has_violation:
            return
        parts = line.rstrip().split('\t')

        if parts[2] not in cmds:
            raise Exception(f"unknown cmd \"{parts[2]}\"")

        if self.ts_us == None:
            self.ts_us = int(parts[1])
        else:
            delta_us = int(parts[1])
            self.ts_us = self.ts_us + delta_us

        new_state = cmds[parts[2]]

        if new_state == State.EVENT:
            if parts[3] == "measure" and not self.measuring:
                # Reset errors when we start measuring so that we don't count
                # errors (e.g. timeouts) that happened while we were setting
                # the test up
                self.measuring = True
                self.errors = 0
            return
        if new_state == State.VIOLATION:
            self.has_violation = True
            logger.error(parts[3])
            return
        if new_state == State.LOG:
            return
        if new_state == State.ERROR:
            self.errors += 1

        thread_id = int(parts[0])
        if thread_id not in self.curr_state:
            self.thread_type[thread_id] = parts[4]
            self.curr_state[thread_id] = None
            if self.thread_type[thread_id] not in threads:
                raise Exception(f"unknown thread type: {parts[4]}")
        if self.curr_state[thread_id] == None:
            if new_state != State.STARTED:
                raise Exception(
                    f"first logged command of a new thread should be started, got: \"{parts[2]}\""
                )
            self.curr_state[thread_id] = new_state
        else:
            if new_state not in threads[self.thread_type[thread_id]][
                    self.curr_state[thread_id]]:
                raise Exception(
                    f"unknown transition {self.curr_state[thread_id]} -> {new_state}"
                )
            self.curr_state[thread_id] = new_state

        if self.thread_type[thread_id] == "consuming":
            self.consuming_apply(thread_id, parts)


def validate(workload_dir, workload_nodes, fail_on_interruption=False):
    logger.setLevel(logging.DEBUG)
    logger_handler_path = os.path.join(workload_dir, "consistency.log")
    handler = logging.FileHandler(logger_handler_path)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(handler)

    try:
        nodes_with_violations = set()
        nodes_with_interruptions = set()
        checker = ReadChecker(workload_nodes)
        for node in workload_nodes:
            player = LogPlayer(node, checker)
            with open(os.path.join(workload_dir, node, "workload.log"),
                      "r") as workload_file:
                last_line = None
                for line in workload_file:
                    if last_line != None:
                        player.apply(last_line)
                    last_line = line
                if player.is_violation(last_line):
                    player.apply(last_line)
            if player.has_violation:
                nodes_with_violations.add(node)
            if player.errors > 0:
                logger.warn(f"client {node} had {player.errors} interruptions")
                nodes_with_interruptions.add(node)

        if len(nodes_with_violations) > 0:
            raise ConsistencyCheckError(
                f"consistency violation on "
                f"client nodes: {nodes_with_violations}")
        elif fail_on_interruption and len(nodes_with_interruptions) > 0:
            raise ConsistencyCheckError(
                f"client interrupted on nodes: {nodes_with_interruptions}")
        else:
            logger.info("consistency check: PASSED")
    except:
        e, v = sys.exc_info()[:2]
        trace = traceback.format_exc()
        logger.warn(v)
        logger.warn(trace)
        raise
    finally:
        handler.flush()
        handler.close()
        logger.removeHandler(handler)
