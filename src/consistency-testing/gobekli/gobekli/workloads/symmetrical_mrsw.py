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
import logging
import traceback

from gobekli.kvapi import RequestCanceled, RequestTimedout
from gobekli.consensus import Violation
from gobekli.workloads.common import (ReaderClient, AvailabilityStatLogger,
                                      Stat, LinearizabilityHashmapChecker)
from gobekli.logging import (m, log_violation, log_latency)

cmdlog = logging.getLogger("gobekli-cmd")


class WriterClient:
    def __init__(self, pid, started_at, stat, checker, node, key):
        self.started_at = started_at
        self.stat = stat
        self.node = node
        self.checker = checker
        self.key = key
        self.last_write_id = str(uuid.uuid1())
        self.last_version = 0
        self.pid = pid
        self.is_active = True

    def stop(self):
        self.is_active = False

    async def start(self):
        loop = asyncio.get_running_loop()
        while self.is_active and self.checker.is_valid and not self.checker.is_aborted:
            await asyncio.sleep(random.uniform(0, 0.05))
            prev = self.last_write_id
            curr_write_id = str(uuid.uuid1())
            curr_version = self.last_version + 1
            op_started = None
            try:
                self.stat.assign("size", self.checker.size())
                cmdlog.info(
                    m(type="write_stared",
                      node=self.node.name,
                      pid=self.pid,
                      key=self.key,
                      write_id=curr_write_id,
                      prev_write_id=prev,
                      version=curr_version,
                      value=f"42:{curr_version}").with_time())
                self.checker.read_started(self.pid, self.key)
                self.checker.cas_started(curr_write_id, self.key, prev,
                                         curr_version, f"42:{curr_version}")
                op_started = loop.time()
                response = await self.node.cas_aio(self.key, prev,
                                                   f"42:{curr_version}",
                                                   curr_write_id)
                data = response.record
                op_ended = loop.time()
                log_latency("ok", op_ended - self.started_at,
                            op_ended - op_started, response.metrics)
                cmdlog.info(
                    m(type="write_ended",
                      node=self.node.name,
                      pid=self.pid,
                      key=self.key,
                      write_id=data.write_id,
                      value=data.value).with_time())
                if data.write_id == curr_write_id:
                    self.checker.cas_ended(curr_write_id, self.key)
                else:
                    self.checker.cas_canceled(curr_write_id, self.key)
                self.checker.read_ended(self.pid, self.key, data.write_id,
                                        data.value)

                if data.write_id == curr_write_id:
                    self.last_version = curr_version
                self.last_write_id = data.write_id
                self.last_version = int(data.value.split(":")[1])
                self.stat.inc(self.node.name + ":ok")
                self.stat.inc("all:ok")
            except RequestTimedout:
                try:
                    self.stat.inc(self.node.name + ":out")
                    op_ended = loop.time()
                    log_latency("out", op_ended - self.started_at,
                                op_ended - op_started)
                    cmdlog.info(
                        m(type="write_timedout",
                          node=self.node.name,
                          pid=self.pid,
                          key=self.key).with_time())
                    self.checker.read_canceled(self.pid, self.key)
                    self.checker.cas_timeouted(curr_write_id, self.key)
                except:
                    e, v = sys.exc_info()[:2]

                    cmdlog.info(
                        m("unexpected error on handing write timedout exception",
                          type="error",
                          error_type=str(e),
                          error_value=str(v),
                          stacktrace=traceback.format_exc()).with_time())

                    self.checker.abort()
                    break
            except RequestCanceled:
                try:
                    self.stat.inc(self.node.name + ":err")
                    op_ended = loop.time()
                    log_latency("err", op_ended - self.started_at,
                                op_ended - op_started)
                    cmdlog.info(
                        m(type="write_canceled",
                          node=self.node.name,
                          pid=self.pid,
                          key=self.key).with_time())
                    self.checker.read_canceled(self.pid, self.key)
                    try:
                        self.checker.cas_canceled(curr_write_id, self.key)
                    except Violation as e:
                        log_violation(self.pid, e.message)
                        break
                except:
                    e, v = sys.exc_info()[:2]

                    cmdlog.info(
                        m("unexpected error on handing write canceled exception",
                          type="error",
                          error_type=str(e),
                          error_value=str(v),
                          stacktrace=traceback.format_exc()).with_time())

                    self.checker.abort()
                    break
            except Violation as e:
                log_violation(self.pid, e.message)
                break


class ValidationResult:
    def __init__(self, is_valid, error):
        self.is_valid = is_valid
        self.error = error


class MRSWWorkload:
    def __init__(self, kv_nodes, numOfKeys, numOfReaders, ss_metrics):
        self.kv_nodes = kv_nodes
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
        stat = Stat()
        dims = []
        dims.append("all:ok")
        for kv in self.kv_nodes:
            dims.append(kv.name + ":ok")
            dims.append(kv.name + ":out")
            dims.append(kv.name + ":err")
        dims.append("size")
        self.availability_logger = AvailabilityStatLogger(stat, dims)

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
                    e, v = sys.exc_info()[:2]
                    stacktrace = traceback.format_exc()
                    cmdlog.info(
                        m(f"Error on initial put using {kv.name}",
                          type="error",
                          error_type=str(e),
                          error_value=str(v),
                          stacktrace=stacktrace).with_time())
            if not wasSet:
                self.is_active = False
                raise Exception("all kv_nodes rejected init write")

        clients = []

        loop = asyncio.get_running_loop()
        started_at = loop.time()

        wid = 0
        rid = 0
        for key in keys:
            for kv in self.kv_nodes:
                wpid = "w" + str(wid)
                wid += 1
                clients.append(
                    WriterClient(wpid, started_at, stat, checker, kv, key))
                for _ in range(0, self.numOfReaders):
                    rpid = "r" + str(rid)
                    rid += 1
                    clients.append(
                        ReaderClient(rpid, started_at, stat, checker, kv, key))
        tasks = []
        for client in clients:
            tasks.append(asyncio.create_task(client.start()))
        tasks.append(asyncio.create_task(self.availability_logger.start()))

        while checker.is_valid and (not checker.is_aborted) and self.is_active:
            await asyncio.sleep(2)

        self.validation_result = ValidationResult(checker.is_valid,
                                                  checker.error)
        self.is_active = False

        for client in clients:
            client.stop()
        self.availability_logger.stop()
        for task in tasks:
            await task

        return self.validation_result


async def start_mrsw_workload_aio(kv_nodes, numOfKeys, numOfReaders, timeout,
                                  ss_metrics):
    workload = MRSWWorkload(kv_nodes, numOfKeys, numOfReaders, ss_metrics)
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
