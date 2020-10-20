import asyncio
from os import path
import os
import sys
import json
import time
import logging
import logging.handlers
import threading
import traceback
import pathlib
from datetime import datetime

from gobekli.kvapi import KVNode
from gobekli.workloads.symmetrical_mrsw import MRSWWorkload
from gobekli.logging import (init_logs, m)

from .analysis import (analyze_inject_recover_availability)

chaos_event_log = logging.getLogger("chaos-event")
chaos_stdout = logging.getLogger("chaos-stdout")
chaos_results = logging.getLogger("chaos-results")


class ExperimentResult:
    def __init__(self):
        self.is_valid = False
        self.error = None
        self.analysis = dict()
        self.title = None
        self.latency_log = None
        self.availability_log = None


class ViolationInducedExit(Exception):
    pass


class ThreadAsyncWaiter:
    def __init__(self, action):
        self.active = True
        self.has_error = False
        self.error_type = None
        self.error_value = None
        self.error_stacktrace = None
        self.thread = threading.Thread(
            target=lambda: self.do_execution(action))
        self.thread.start()

    async def wait(self, period_ms):
        while self.active:
            await asyncio.sleep(float(period_ms) / 1000)

        if self.has_error:
            msg = m("error on fault injection / recovery",
                    type=self.error_type,
                    value=self.error_value,
                    stacktrace=self.error_stacktrace).with_time()
            chaos_event_log.info(msg)
            raise Exception(str(msg))

    def do_execution(self, action):
        try:
            action()
            self.active = False
        except:
            self.has_error = True
            e, v = sys.exc_info()[:2]
            self.error_type = str(e)
            self.error_value = str(v)
            self.error_stacktrace = traceback.format_exc()
            self.active = False


async def inject_recover_scenario_aio(log_dir, config, cluster,
                                      workload_factory, failure_factory):
    cmd_log = path.join(log_dir, config["cmd_log"])
    latency_log = path.join(log_dir, config["latency_log"])
    availability_log = path.join(log_dir, config["availability_log"])

    init_logs(cmd_log, latency_log, availability_log, config["ss_metrics"])
    if not (config["verbose"]):
        gobekli_stdout = logging.getLogger("gobekli-stdout")
        gobekli_stdout.handlers = []

    workload = workload_factory()
    task = asyncio.create_task(workload.start())

    try:
        loop = asyncio.get_running_loop()

        end_time = loop.time() + config["warmup"]
        while workload.is_active:
            if (loop.time() + 1) >= end_time:
                break
            await asyncio.sleep(1)

        # inject
        fault = failure_factory()

        inject_side_thread = ThreadAsyncWaiter(
            lambda: fault.inject(cluster, workload))
        await inject_side_thread.wait(period_ms=500)

        end_time = loop.time() + config["exploitation"]
        while workload.is_active:
            if (loop.time() + 1) >= end_time:
                break
            await asyncio.sleep(1)

        # recover
        await ThreadAsyncWaiter(lambda: fault.recover()).wait(period_ms=500)

        end_time = loop.time() + config["cooldown"]
        while workload.is_active:
            if (loop.time() + 1) >= end_time:
                break
            await asyncio.sleep(1)
    except:
        workload.stop()

        try:
            await task
        except:
            e, v = sys.exc_info()[:2]
            stacktrace = traceback.format_exc()
            chaos_event_log.info(
                m("error on waiting for workflow's tast on handling error",
                  error_type=str(e),
                  error_value=str(v),
                  stacktrace=stacktrace).with_time())

        raise

    workload.stop()
    validation_result = await task
    await workload.dispose()

    scenario = "inject-recover"
    workload = config["workload"]["name"]

    result = ExperimentResult()
    result.is_valid = validation_result.is_valid
    result.error = validation_result.error
    result.title = f"{workload} with {scenario} using {fault.title}"
    result.availability_log = config["availability_log"]
    result.latency_log = config["latency_log"]
    result.analysis = analyze_inject_recover_availability(
        log_dir, config["availability_log"], config["latency_log"])
    return result


async def inject_recover_scenarios_aio(config, cluster, faults,
                                       workload_factory):
    scenario = "inject-recover"
    workload = config["workload"]["name"]

    if not path.exists(config["output"]):
        os.mkdir(config["output"])

    for fault in faults.keys():
        if config["reset_before_test"]:
            await cluster.restart()
            is_healthy = False
            for _ in range(0, 10):
                is_healthy = await cluster.is_ok()
                if is_healthy:
                    break
                chaos_event_log.info(
                    m(f"cluster isn't healthy, retrying").with_time())
                await asyncio.sleep(5)

            if not is_healthy:
                chaos_event_log.info(m(f"cluster isn't healthy").with_time())
                raise Exception(f"cluster isn't healthy")

        experiment_id = int(time.time())

        rel_dir = path.join(workload, scenario, fault, str(experiment_id))
        log_dir = path.join(config["output"], rel_dir)
        pathlib.Path(log_dir).mkdir(parents=True, exist_ok=True)

        chaos_event_log.info(
            m(f"starting {experiment_id} experiment (inject / recover, {fault})"
              ).with_time())

        now = datetime.now().strftime("%H:%M:%S")
        chaos_stdout.info(
            f"({now}) testing {workload} using {scenario} with {fault} - {experiment_id}"
        )

        result = await inject_recover_scenario_aio(log_dir, config, cluster,
                                                   workload_factory,
                                                   faults[fault])
        message = m(id=experiment_id,
                    type="result",
                    title=result.title,
                    latency_log=result.latency_log,
                    availability_log=result.availability_log,
                    workload=workload,
                    scenario=scenario,
                    fault=fault,
                    path=rel_dir,
                    metrics=result.analysis).with_time()
        if result.is_valid:
            chaos_stdout.info(f"\tlinearizability testing passed")
            message.kwargs["status"] = "passed"
            chaos_event_log.info(message)
            chaos_results.info(message)
        else:
            chaos_stdout.info(f"\tviolation: {result.error}")
            message.kwargs["status"] = "failed"
            message.kwargs["error"] = result.error
            chaos_event_log.info(message)
            chaos_results.info(message)

        chaos_stdout.info(f"\tmax latency: " + str(result.analysis["max_lat"]))
        chaos_stdout.info(f"\tmin latency: " + str(result.analysis["min_lat"]))
        chaos_stdout.info(f"\tunavailability:")
        chaos_stdout.info(f"\t\tmax     : " +
                          str(result.analysis["max_unavailability"]))
        chaos_stdout.info(f"\t\tbase    : " +
                          str(result.analysis["base_max_unavailability"]))
        chaos_stdout.info(f"\t\tfault   : " +
                          str(result.analysis["fault_max_unavailability"]))
        chaos_stdout.info(f"\t\trecovery: " +
                          str(result.analysis["recovery_max_unavailability"]))
        chaos_stdout.info("")

        if config["exit_on_violation"] and not result.is_valid:
            raise ViolationInducedExit()

        if not (await cluster.is_ok()):
            chaos_event_log.info(
                m(f"cluster hasn't recover after {experiment_id}").with_time())
            await cluster.restart()


def init_output(config):
    if path.exists(config["output"]):
        if not (path.isdir(config["output"])):
            raise Exception(config["output"] + " must be a directory")
    else:
        os.mkdir(config["output"])

    suitid = int(time.time())

    chaos_event_log.handlers = []
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.FileHandler(path.join(config["output"],
                                                f"{suitid}_events.log"),
                                      mode="w")
    fileHandler.setFormatter(formatter)
    chaos_event_log.setLevel(logging.INFO)
    chaos_event_log.addHandler(fileHandler)

    chaos_results.handlers = []
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.FileHandler(path.join(config["output"],
                                                f"{suitid}_results.log"),
                                      mode="w")
    fileHandler.setFormatter(formatter)
    chaos_results.setLevel(logging.INFO)
    chaos_results.addHandler(fileHandler)

    chaos_stdout.handlers = []
    formatter = logging.Formatter('%(message)s')
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    chaos_stdout.setLevel(logging.INFO)
    chaos_stdout.addHandler(ch)
