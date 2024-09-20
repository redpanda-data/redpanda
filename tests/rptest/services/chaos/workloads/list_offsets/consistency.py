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
from ..retryable_consumer import RetryableConsumer
from .log_utils import State, cmds, transitions, phantoms

logger = logging.getLogger("consistency")


class Write:
    def __init__(self):
        self.key = None
        self.op = None
        self.offset = None
        self.started = None
        self.finished = None
        self.max_offset = None


class LogPlayer:
    def __init__(self, brokers_str, topic, cleanup=None):
        self.brokers_str = brokers_str
        self.topic = topic

        if cleanup is not None and cleanup not in ["compact", "delete"]:
            raise Exception(f"unknown cleanup policy: {cleanup}")
        self.cleanup = cleanup

        self.curr_state = dict()
        self.ts_us = None
        self.has_violation = False

        self.first_offset = sys.maxsize
        self.last_offset = 0
        self.max_offset = -1
        self.last_write = dict()
        self.key = dict()
        self.ok_writes = dict()
        self.err_writes = dict()
        self.inlight_writes = dict()

    def reread_and_check(self):
        if self.has_violation:
            return

        RETRIES = 5
        c = RetryableConsumer(logger, self.brokers_str)
        c.init(self.topic, RETRIES)
        retries = RETRIES

        final_ko = dict()

        prev_offset = -1
        is_active = True
        is_first = True
        while is_active:
            if retries == 0:
                raise Exception("Can't connect to the redpanda cluster")
            msgs = c.consume(timeout=10)
            retries -= 1
            for msg in msgs:
                if msg is None:
                    continue
                if msg.error():
                    logger.debug("Consumer error: {}".format(msg.error()))
                    continue
                retries = RETRIES

                offset = msg.offset()
                value = msg.value().decode('utf-8')
                parts = value.split("\t")
                op = int(parts[0])
                key = msg.key().decode('utf-8')

                if self.cleanup == "compact":
                    final_ko[key] = offset

                if is_first:
                    if self.cleanup == "delete":
                        for woff in list(self.ok_writes.keys()):
                            if woff < offset:
                                del self.ok_writes[woff]
                        for wop in list(self.err_writes.keys()):
                            if wop < op:
                                del self.err_writes[wop]
                    is_first = False

                if offset <= prev_offset:
                    logger.error(
                        f"offsets must increase; observed {offset} after {prev_offset}"
                    )
                    self.has_violation = True
                prev_offset = offset

                if offset < self.first_offset:
                    continue

                if offset in self.ok_writes:
                    write = self.ok_writes[offset]
                    if write.op != op:
                        logger.error(
                            f"read message [{key}]={op}@{offset} doesn't match written message [{write.key}]={write.op}@{offset}"
                        )
                        self.has_violation = True
                    if write.key != key:
                        logger.error(
                            f"read message [{key}]={op}@{offset} doesn't match written message [{write.key}]={write.op}@{offset}"
                        )
                        self.has_violation = True
                    del self.ok_writes[offset]
                    if op in self.err_writes:
                        logger.error(
                            f"op ({op}) of an observed write [{key}]={op}@{offset} found in erroneous writes"
                        )
                        self.has_violation = True
                elif op in self.err_writes:
                    write = self.err_writes[op]
                    if write.key != key:
                        logger.error(
                            f"read message [{key}]={op}@{offset} doesn't match written message [{write.key}]={write.op}"
                        )
                        self.has_violation = True
                    if offset <= write.max_offset:
                        logger.error(
                            f"message got lesser offset that was known ({write.max_offset}) before it's written: [{write.key}]={write.op}@{offset}"
                        )
                        self.has_violation = True
                    del self.err_writes[op]
                else:
                    found = False
                    for thread_id in self.last_write.keys():
                        write = self.last_write[thread_id]
                        if write == None:
                            continue
                        if write.key != key:
                            continue
                        if op != write.op:
                            logger.error(
                                f"read op={op} for key={key} doesn't match inflight op={write.op}"
                            )
                            self.has_violation = True
                            break
                        if offset <= write.max_offset:
                            logger.error(
                                f"message got lesser offset that was known ({write.max_offset}) before it's written: [{write.key}]={write.op}@{offset}"
                            )
                            self.has_violation = True
                            break
                        found = True
                        break
                    if not (found) and not (self.has_violation):
                        logger.error(
                            f"read unknown message [{key}]={op}@{offset}")
                        self.has_violation = True

                if offset >= self.last_offset:
                    is_active = False
                    break
        c.close()

        if len(self.ok_writes) != 0:
            if self.cleanup == "compact":
                for offset in self.ok_writes:
                    write = self.ok_writes[offset]
                    if write.key not in final_ko:
                        self.has_violation = True
                        logger.error(
                            f"lost message found [{write.key}]={write.op}@{offset}"
                        )
                        continue
                    if final_ko[write.key] == offset:
                        raise Exception("an observed record can't be skipped")
                    if final_ko[write.key] < offset:
                        self.has_violation = True
                        logger.error(
                            f"lost message found [{write.key}]={write.op}@{offset} last seen is [{write.key}]@{final_ko[write.key]}"
                        )
                        continue
            else:
                self.has_violation = True
                for offset in self.ok_writes:
                    write = self.ok_writes[offset]
                    logger.error(
                        f"lost message found [{write.key}]={write.op}@{offset}"
                    )

    def writing_apply(self, thread_id, parts):
        if self.curr_state[thread_id] == State.SENDING:
            write = Write()
            write.key = parts[3]
            write.op = int(parts[4])
            write.started = self.ts_us
            write.max_offset = self.max_offset
            self.last_write[thread_id] = write
        elif self.curr_state[thread_id] == State.OK:
            offset = int(parts[3])
            self.first_offset = min(self.first_offset, offset)
            self.last_offset = max(self.last_offset, offset)
            write = self.last_write[thread_id]
            self.last_write[thread_id] = None
            write.offset = offset
            write.finished = self.ts_us
            if offset <= write.max_offset:
                self.has_violation = True
                logger.error(
                    f"message got lesser offset that was known ({write.max_offset}) before it's written: [{write.key}]={write.op}@{offset}"
                )
            self.max_offset = max(self.max_offset, offset)
            if offset in self.ok_writes:
                known = self.ok_writes[offset]
                logger.error(
                    f"message got already assigned offset: [{write.key}]={write.op} vs [{known.key}]={known.op} @ {offset}"
                )
                self.has_violation = True
            self.ok_writes[offset] = write
        elif self.curr_state[thread_id] in [State.ERROR, State.TIMEOUT]:
            if thread_id in self.last_write and self.last_write[
                    thread_id] != None:
                write = self.last_write[thread_id]
                self.last_write[thread_id] = None
                write.offset = None
                write.finished = self.ts_us
                self.err_writes[write.op] = write

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
            return
        if new_state == State.VIOLATION:
            self.has_violation = True
            logger.error(parts[3])
            return
        if new_state == State.LOG:
            return

        thread_id = int(parts[0])
        if thread_id not in self.curr_state:
            self.curr_state[thread_id] = None
        if self.curr_state[thread_id] == None:
            if new_state != State.STARTED:
                raise Exception(
                    f"first logged command of a new thread should be started, got: \"{parts[2]}\""
                )
            self.curr_state[thread_id] = new_state
            self.key[thread_id] = parts[3]
        else:
            if new_state not in transitions[self.curr_state[thread_id]]:
                raise Exception(
                    f"unknown transition {self.curr_state[thread_id]} -> {new_state}"
                )
            self.curr_state[thread_id] = new_state

        self.writing_apply(thread_id, parts)


def validate(workload_dir, workload_nodes, brokers_str, topic):
    logger.setLevel(logging.DEBUG)
    logger_handler_path = os.path.join(workload_dir, "consistency.log")
    handler = logging.FileHandler(logger_handler_path)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    logger.addHandler(handler)

    try:
        if len(workload_nodes) != 1:
            raise Exception("can't validate more than one workload nodes")

        nodes_with_violations = set()
        for node in workload_nodes:
            player = LogPlayer(brokers_str, topic)
            with open(os.path.join(workload_dir, node, "workload.log"),
                      "r") as workload_file:
                last_line = None
                for line in workload_file:
                    if last_line != None:
                        player.apply(last_line)
                    last_line = line
                if player.is_violation(last_line):
                    player.apply(last_line)
            player.reread_and_check()

            if player.has_violation:
                nodes_with_violations.add(node)

        if len(nodes_with_violations) > 0:
            raise ConsistencyCheckError(
                f"consistency violation on "
                f"client nodes: {nodes_with_violations}")
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
