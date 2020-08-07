import asyncio
import uuid

from gobekli.kvapi import RequestCanceled, RequestTimedout
from gobekli.consensus import Violation
from gobekli.workloads.common import (Stat, StatDumper, ReaderClient,
                                      LinearizabilityHashmapChecker)


class WriterClient:
    def __init__(self, stat, checker, name, node, key):
        self.stat = stat
        self.node = node
        self.name = name
        self.checker = checker
        self.key = key
        self.last_write_id = str(uuid.uuid1())
        self.last_version = 0
        self.pid = str(uuid.uuid1())
        self.is_active = True

    def stop(self):
        self.is_active = False

    async def start(self):
        while self.is_active and self.checker.is_valid:
            prev = self.last_write_id
            curr_write_id = str(uuid.uuid1())
            curr_version = self.last_version + 1
            try:
                self.checker.read_started(self.pid, self.key)
                self.checker.cas_started(curr_write_id, self.key, prev,
                                         curr_version, f"42:{curr_version}")
                data = await self.node.cas_aio(self.key, prev,
                                               f"42:{curr_version}",
                                               curr_write_id)
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
                self.stat.inc(self.name + ":ok")
            except RequestTimedout:
                self.stat.inc(self.name + ":out")
                self.checker.read_canceled(self.pid, self.key)
                self.checker.cas_timeouted(curr_write_id, self.key)
            except RequestCanceled:
                self.stat.inc(self.name + ":err")
                self.checker.read_canceled(self.pid, self.key)
                try:
                    self.checker.cas_canceled(curr_write_id, self.key)
                except Violation as e:
                    print(f"violation: {e.message}")
                    break
            except Violation:
                print(f"violation: {e.message}")
                break


async def start_mrsw_workload_aio(kv_nodes, numOfKeys, numOfReaders, timeout):
    keys = list(map(lambda x: f"key{x}", range(0, numOfKeys)))

    checker = LinearizabilityHashmapChecker()

    for key in keys:
        wasSet = False
        for kv in kv_nodes:
            try:
                await kv.put_aio(key, "42:0", "0")
                checker.init("0", key, 0, "42:0")
                wasSet = True
                break
            except:
                pass
        if not wasSet:
            raise Exception("all kv_nodes rejected init write")

    stat = Stat()
    dims = []
    for kv in kv_nodes:
        dims.append(kv.name + ":ok")
        dims.append(kv.name + ":out")
        dims.append(kv.name + ":err")
    dumper = StatDumper(stat, dims)
    clients = []

    for key in keys:
        for kv in kv_nodes:
            clients.append(WriterClient(stat, checker, kv.name, kv, key))
            for _ in range(0, numOfReaders):
                clients.append(ReaderClient(stat, checker, kv.name, kv, key))
    tasks = []
    for client in clients:
        tasks.append(asyncio.create_task(client.start()))
    tasks.append(asyncio.create_task(dumper.start()))

    loop = asyncio.get_running_loop()
    end_time = loop.time() + timeout
    while checker.is_valid:
        if (loop.time() + 2) >= end_time:
            break
        await asyncio.sleep(2)

    print(checker.is_valid)
    print(checker.error)

    for client in clients:
        client.stop()
    dumper.stop()
    for task in tasks:
        await task
