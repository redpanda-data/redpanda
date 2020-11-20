# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import asyncio
import uuid
import random
import time
import sys
import json
import logging
import traceback

from gobekli.kvapi import RequestCanceled, RequestTimedout, RequestViolated
from gobekli.consensus import LinearizabilityRegisterChecker, Violation
from gobekli.logging import (m, log_violation, log_latency)

cmdlog = logging.getLogger("gobekli-cmd")
stdout = logging.getLogger("gobekli-stdout")
statlog = logging.getLogger("gobekli-availability")


class Stat:
    def __init__(self):
        self.counters = dict()
        self.vars = dict()

    def assign(self, key, val):
        self.vars[key] = val

    def inc(self, key):
        if key not in self.counters:
            self.counters[key] = 0
        self.counters[key] += 1

    def reset(self):
        copy = self.counters
        self.counters = dict()
        for key in self.vars:
            copy[key] = self.vars[key]
        return copy


class AvailabilityStatLogger:
    def __init__(self, stat, keys):
        self.stat = stat
        self.keys = keys
        self.started = None
        self.is_active = True

    async def start(self):
        self.started = time.time()
        while self.is_active:
            counters = self.stat.reset()
            entry = dict()
            entry["type"] = "stat"
            entry["tick"] = int(time.time() - self.started)
            line = str(entry["tick"])
            for key in self.keys:
                if key in counters:
                    entry[key] = counters[key]
                    line += "\t" + str(counters[key])
                else:
                    entry[key] = 0
                    line += "\t" + str(0)
            stdout.info(line)
            statlog.info(json.dumps(entry))
            await asyncio.sleep(1)

    def log_fault(self, message):
        entry = dict()
        entry["type"] = "fault"
        entry["tick"] = int((time.time() - self.started) * 1000000)
        entry["message"] = message
        line = str(entry["tick"]) + "\t" + entry["message"]
        stdout.info(line)
        statlog.info(json.dumps(entry))

    def log_recovery(self, message):
        entry = dict()
        entry["type"] = "recovery"
        entry["tick"] = int((time.time() - self.started) * 1000000)
        entry["message"] = message
        line = str(entry["tick"]) + "\t" + entry["message"]
        stdout.info(line)
        statlog.info(json.dumps(entry))

    def stop(self):
        self.is_active = False


class LinearizabilityHashmapChecker:
    def __init__(self):
        self.checkers = dict()
        self.is_valid = True
        self.is_aborted = False
        self.error = None

    def abort(self):
        self.is_aborted = True

    def size(self):
        result = 0
        for key in self.checkers:
            result += self.checkers[key].size()
        return result

    def init(self, write_id, key, version, value):
        if key in self.checkers:
            raise Exception(f"Key {key} is already known, use cas to update")

        self.checkers[key] = LinearizabilityRegisterChecker()
        self.checkers[key].init(write_id, version, value)

    def cas_started(self, write_id, key, prev_write_id, version, value):
        if not self.is_valid:
            return
        if key not in self.checkers:
            raise Exception(f"Key {key} must be put first")

        self.checkers[key].write_started(prev_write_id, write_id, version,
                                         value)

    def cas_ended(self, write_id, key):
        if not self.is_valid:
            return
        if key not in self.checkers:
            raise Exception(f"Key {key} must be put first")

        try:
            self.checkers[key].write_ended(write_id)
        except Violation as e:
            self.error = e.message
            self.is_valid = False
            raise e

    def cas_canceled(self, write_id, key):
        if not self.is_valid:
            return
        if key not in self.checkers:
            raise Exception(f"Key {key} must be put first")

        try:
            self.checkers[key].write_canceled(write_id)
        except Violation as e:
            self.error = e.message
            self.is_valid = False
            raise e

    def cas_timeouted(self, write_id, key):
        if not self.is_valid:
            return
        if key not in self.checkers:
            raise Exception(f"Key {key} must be put first")

        self.checkers[key].write_timeouted(write_id)

    def read_started(self, pid, key):
        if not self.is_valid:
            return
        if key not in self.checkers:
            raise Exception(f"Key {key} must be put first")

        self.checkers[key].read_started(pid)

    def read_none(self, pid, key):
        if not self.is_valid:
            return
        if key not in self.checkers:
            raise Exception(f"Key {key} must be put first")
        self.is_valid = False
        self.error = f"key {key} can't be null"
        raise Violation(self.error)

    def read_ended(self, pid, key, write_id, value):
        if not self.is_valid:
            return
        if key not in self.checkers:
            raise Exception(f"Key {key} must be put first")

        try:
            self.checkers[key].read_ended(pid, write_id, value)
        except Violation as e:
            self.error = e.message
            self.is_valid = False
            raise e

    def read_canceled(self, pid, key):
        if not self.is_valid:
            return
        if key not in self.checkers:
            raise Exception(f"Key {key} must be put first")

        self.checkers[key].read_canceled(pid)

    def report_violation(self, message):
        self.is_valid = False
        self.error = message
        raise Violation(self.error)


class ReaderClient:
    def __init__(self, pid, started_at, stat, checker, node, key):
        self.started_at = started_at
        self.stat = stat
        self.node = node
        self.key = key
        self.checker = checker
        self.pid = pid
        self.is_active = True

    def stop(self):
        self.is_active = False

    async def start(self):
        loop = asyncio.get_running_loop()
        while self.is_active and self.checker.is_valid:
            await asyncio.sleep(random.uniform(0, 0.05))
            op_started = None
            read_id = str(uuid.uuid1())
            try:
                self.stat.assign("size", self.checker.size())
                cmdlog.info(
                    m(type="read_started",
                      node=self.node.name,
                      pid=self.pid,
                      read_id=read_id,
                      key=self.key).with_time())
                self.checker.read_started(self.pid, self.key)
                op_started = loop.time()
                response = await self.node.get_aio(self.key, read_id)
                read = response.record
                op_ended = loop.time()
                log_latency("ok", op_ended - self.started_at,
                            op_ended - op_started, response.metrics)
                if read == None:
                    cmdlog.info(
                        m(type="read_404",
                          node=self.node.name,
                          pid=self.pid,
                          read_id=read_id,
                          key=self.key).with_time())
                    self.checker.read_none(self.pid, self.key)
                else:
                    cmdlog.info(
                        m(type="read_ended",
                          node=self.node.name,
                          pid=self.pid,
                          read_id=read_id,
                          key=self.key,
                          write_id=read.write_id,
                          value=read.value).with_time())
                    self.checker.read_ended(self.pid, self.key, read.write_id,
                                            read.value)
                self.stat.inc(self.node.name + ":ok")
                self.stat.inc("all:ok")
            except RequestTimedout:
                try:
                    op_ended = loop.time()
                    log_latency("out", op_ended - self.started_at,
                                op_ended - op_started)
                    self.stat.inc(self.node.name + ":out")
                    cmdlog.info(
                        m(type="read_timedout",
                          node=self.node.name,
                          pid=self.pid,
                          read_id=read_id,
                          key=self.key).with_time())
                    self.checker.read_canceled(self.pid, self.key)
                except:
                    e, v = sys.exc_info()[:2]

                    cmdlog.info(
                        m("unexpected error on handing read timedout exception",
                          type="error",
                          error_type=str(e),
                          error_value=str(v),
                          stacktrace=traceback.format_exc()).with_time())

                    self.checker.abort()
                    break
            except RequestCanceled:
                try:
                    op_ended = loop.time()
                    log_latency("err", op_ended - self.started_at,
                                op_ended - op_started)
                    self.stat.inc(self.node.name + ".err")
                    cmdlog.info(
                        m(type="read_canceled",
                          node=self.node.name,
                          pid=self.pid,
                          read_id=read_id,
                          key=self.key).with_time())
                    self.checker.read_canceled(self.pid, self.key)
                except:
                    # TODO: handle violation
                    e, v = sys.exc_info()[:2]

                    cmdlog.info(
                        m("unexpected error on handing read canceled exception",
                          type="error",
                          error_type=str(e),
                          error_value=str(v),
                          stacktrace=traceback.format_exc()).with_time())

                    self.checker.abort()
                    break
            except RequestViolated as e:
                try:
                    self.checker.report_violation("internal violation: " +
                                                  json.dumps(e.info))
                except Violation as e:
                    log_violation(self.pid, e.message)
                    break
            except Violation as e:
                log_violation(self.pid, e.message)
                break
