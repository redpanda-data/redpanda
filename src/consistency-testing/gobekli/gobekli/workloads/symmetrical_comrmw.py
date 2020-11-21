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
import sys
import json
import logging
import traceback

from gobekli.kvapi import RequestCanceled, RequestTimedout, RequestViolated
from gobekli.consensus import Violation
from gobekli.workloads.common import (AvailabilityStatLogger, Stat,
                                      LinearizabilityHashmapChecker)
from gobekli.logging import (m, log_latency)

cmdlog = logging.getLogger("gobekli-cmd")


class MWClient:
    def __init__(self, started_at, stat, checker, node, key):
        self.started_at = started_at
        self.stat = stat
        self.node = node
        self.checker = checker
        self.key = key
        self.last_write_id = str(uuid.uuid1())
        self.last_version = 0

    async def act(self):
        loop = asyncio.get_running_loop()
        prev_write_id = self.last_write_id
        curr_write_id = str(uuid.uuid1())
        curr_version = self.last_version + 1
        read_id = str(uuid.uuid1())

        op_started = None
        try:
            self.stat.assign("size", self.checker.size())

            cmdlog.info(
                m(type="write_stared",
                  node=self.node.name,
                  key=self.key,
                  write_id=curr_write_id,
                  read_id=read_id,
                  prev_write_id=prev_write_id,
                  version=curr_version,
                  value=f"42:{curr_version}").with_time())

            self.checker.read_started(read_id, self.key)
            self.checker.cas_started(curr_write_id, self.key, prev_write_id,
                                     curr_version, f"42:{curr_version}")
            op_started = loop.time()
            response = await self.node.cas_aio(self.key, prev_write_id,
                                               f"42:{curr_version}",
                                               curr_write_id)
            data = response.record
            op_ended = loop.time()
            log_latency("ok", op_ended - self.started_at,
                        op_ended - op_started, self.node.idx, response.metrics)
            cmdlog.info(
                m(type="write_ended",
                  node=self.node.name,
                  key=self.key,
                  write_id=data.write_id,
                  value=data.value).with_time())
            if data.write_id == curr_write_id:
                self.checker.cas_ended(curr_write_id, self.key)
            else:
                self.checker.cas_canceled(curr_write_id, self.key)
            self.checker.read_ended(read_id, self.key, data.write_id,
                                    data.value)

            read_version = int(data.value.split(":")[1])
            read_write_id = data.write_id

            if self.last_version < read_version:
                self.last_version = read_version
                self.last_write_id = read_write_id

            self.stat.inc(self.node.name + ":ok")
            self.stat.inc("all:ok")
        except RequestTimedout:
            try:
                self.stat.inc(self.node.name + ":out")
                op_ended = loop.time()
                log_latency("out", op_ended - self.started_at,
                            op_ended - op_started, self.node.idx)
                cmdlog.info(
                    m(type="write_timedout",
                      node=self.node.name,
                      write_id=curr_write_id,
                      key=self.key).with_time())
                self.checker.read_canceled(read_id, self.key)
                self.checker.cas_timeouted(curr_write_id, self.key)
            except:
                e, v = sys.exc_info()[:2]
                cmdlog.info(
                    m("unexpected error on write/timedout",
                      error_type=str(e),
                      error_value=str(v),
                      stacktrace=traceback.format_exc()).with_time())
                self.checker.abort()
        except RequestCanceled:
            try:
                self.stat.inc(self.node.name + ":err")
                op_ended = loop.time()
                log_latency("err", op_ended - self.started_at,
                            op_ended - op_started, self.node.idx)
                cmdlog.info(
                    m(type="write_canceled",
                      node=self.node.name,
                      write_id=curr_write_id,
                      key=self.key).with_time())
                self.checker.read_canceled(read_id, self.key)
                try:
                    self.checker.cas_canceled(curr_write_id, self.key)
                except Violation as e:
                    cmdlog.info(
                        m(e.message,
                          type="linearizability_violation",
                          write_id=curr_write_id).with_time())
            except:
                e, v = sys.exc_info()[:2]
                cmdlog.info(
                    m("unexpected error on write/canceled",
                      error_type=str(e),
                      error_value=str(v),
                      stacktrace=traceback.format_exc()).with_time())
                self.checker.abort()
        except RequestViolated as e:
            try:
                self.checker.report_violation("internal violation: " +
                                              json.dumps(e.info))
            except Violation as e:
                cmdlog.info(
                    m(e.message,
                      type="linearizability_violation",
                      write_id=curr_write_id).with_time())
        except Violation as e:
            cmdlog.info(
                m(e.message,
                  type="linearizability_violation",
                  write_id=curr_write_id).with_time())
        except:
            e, v = sys.exc_info()[:2]
            cmdlog.info(
                m("unexpected error on write",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=traceback.format_exc()).with_time())
            self.checker.abort()


class MRClient:
    def __init__(self, started_at, stat, checker, node, key):
        self.started_at = started_at
        self.stat = stat
        self.node = node
        self.key = key
        self.checker = checker

    async def act(self):
        loop = asyncio.get_running_loop()
        op_started = None
        read_id = str(uuid.uuid1())
        try:
            self.stat.assign("size", self.checker.size())
            cmdlog.info(
                m(type="read_started",
                  node=self.node.name,
                  read_id=read_id,
                  key=self.key).with_time())
            self.checker.read_started(read_id, self.key)
            op_started = loop.time()
            response = await self.node.get_aio(self.key, read_id)
            read = response.record
            op_ended = loop.time()
            log_latency("ok", op_ended - self.started_at,
                        op_ended - op_started, self.node.idx, response.metrics)
            if read == None:
                cmdlog.info(
                    m(type="read_404",
                      node=self.node.name,
                      read_id=read_id,
                      key=self.key).with_time())
                self.checker.read_none(read_id, self.key)
            else:
                cmdlog.info(
                    m(type="read_ended",
                      node=self.node.name,
                      read_id=read_id,
                      key=self.key,
                      write_id=read.write_id,
                      value=read.value).with_time())
                self.checker.read_ended(read_id, self.key, read.write_id,
                                        read.value)
            self.stat.inc(self.node.name + ":ok")
            self.stat.inc("all:ok")
        except RequestTimedout:
            try:
                op_ended = loop.time()
                log_latency("out", op_ended - self.started_at,
                            op_ended - op_started, self.node.idx)
                self.stat.inc(self.node.name + ":out")
                cmdlog.info(
                    m(type="read_timedout",
                      node=self.node.name,
                      read_id=read_id,
                      key=self.key).with_time())
                self.checker.read_canceled(read_id, self.key)
            except:
                e, v = sys.exc_info()[:2]
                cmdlog.info(
                    m("unexpected error on read/timedout",
                      error_type=str(e),
                      error_value=str(v),
                      stacktrace=traceback.format_exc()).with_time())
                self.checker.abort()
        except RequestCanceled:
            try:
                op_ended = loop.time()
                log_latency("err", op_ended - self.started_at,
                            op_ended - op_started, self.node.idx)
                self.stat.inc(self.node.name + ".err")
                cmdlog.info(
                    m(type="read_canceled",
                      node=self.node.name,
                      read_id=read_id,
                      key=self.key).with_time())
                self.checker.read_canceled(read_id, self.key)
            except:
                e, v = sys.exc_info()[:2]
                cmdlog.info(
                    m("unexpected error on read/canceled",
                      error_type=str(e),
                      error_value=str(v),
                      stacktrace=traceback.format_exc()).with_time())
                self.checker.abort()
        except RequestViolated as e:
            try:
                self.checker.report_violation("internal violation: " +
                                              json.dumps(e.info))
            except Violation as e:
                cmdlog.info(
                    m(e.message,
                      type="linearizability_violation",
                      read_id=read_id).with_time())
        except Violation as e:
            cmdlog.info(
                m(e.message, type="linearizability_violation",
                  read_id=read_id).with_time())
        except:
            e, v = sys.exc_info()[:2]
            cmdlog.info(
                m("unexpected error on read",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=traceback.format_exc()).with_time())
            self.checker.abort()


class ValidationResult:
    def __init__(self, is_valid, error):
        self.is_valid = is_valid
        self.error = error


class COMRMWWorkload:
    def __init__(self, period_s, kv_nodes, numOfKeys, numOfReaders,
                 ss_metrics):
        self.kv_nodes = kv_nodes
        self.period_s = period_s
        self.numOfKeys = numOfKeys
        self.numOfReaders = numOfReaders
        self.ss_metrics = ss_metrics
        self.is_active = True
        self.validation_result = None
        self.availability_logger = None

    def stop(self):
        self.is_active = False

    async def dispose(self):
        for kv_node in self.kv_nodes:
            await kv_node.close_aio()

    async def start(self):
        keys = list(map(lambda x: f"key{x}", range(0, self.numOfKeys)))

        checker = LinearizabilityHashmapChecker()

        for key in keys:
            wasSet = False
            for kv in self.kv_nodes:
                try:
                    await kv.put_aio(key, "42:0", "0")
                    checker.init("0", key, 0, "42:0")
                    wasSet = True
                    break
                except:
                    pass
            if not wasSet:
                self.is_active = False
                raise Exception("all kv_nodes rejected init write")

        stat = Stat()
        dims = []
        dims.append("all:ok")
        for kv in self.kv_nodes:
            dims.append(kv.name + ":ok")
            dims.append(kv.name + ":out")
            dims.append(kv.name + ":err")
        dims.append("size")
        self.availability_logger = AvailabilityStatLogger(stat, dims)
        clients = []
        tasks = []

        tasks.append(asyncio.create_task(self.availability_logger.start()))

        loop = asyncio.get_running_loop()
        started_at = loop.time()

        for key in keys:
            for kv in self.kv_nodes:
                clients.append(MWClient(started_at, stat, checker, kv, key))
                for _ in range(0, self.numOfReaders):
                    clients.append(MRClient(started_at, stat, checker, kv,
                                            key))

        while checker.is_valid and (not checker.is_aborted) and self.is_active:
            i = random.randint(0, len(clients) - 1)
            client = clients[i]
            # TODO: keep track of tasks and wait them in the end
            _ = asyncio.create_task(client.act())
            await asyncio.sleep(self.period_s)

        self.validation_result = ValidationResult(checker.is_valid,
                                                  checker.error)
        self.is_active = False

        self.availability_logger.stop()
        for task in tasks:
            await task

        return self.validation_result


async def start_comrsw_workload_aio(kv_nodes, numOfKeys, numOfReaders, timeout,
                                    ss_metrics):
    print("comrsw")
    workload = COMRMWWorkload(1.0 / 400, kv_nodes, numOfKeys, numOfReaders,
                              ss_metrics)
    task = asyncio.create_task(workload.start())

    loop = asyncio.get_running_loop()
    end_time = loop.time() + timeout
    while workload.is_active:
        if (loop.time() + 2) >= end_time:
            workload.stop()
            break
        await asyncio.sleep(2)

    result = await task
    await workload.dispose()
    print(result.is_valid)
    print(result.error)
