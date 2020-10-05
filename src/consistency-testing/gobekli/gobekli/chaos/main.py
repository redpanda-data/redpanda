import asyncio
from os import path
import os
import sys
import json
import time
import logging
import logging.handlers
from datetime import datetime

from gobekli.kvapi import KVNode
from gobekli.workloads.symmetrical_mrsw import MRSWWorkload
from gobekli.logging import (init_logs, m)

from .analysis import (make_overview_chart,
                       analyze_inject_recover_availability)

chaos_event_log = logging.getLogger("chaos-event")
chaos_stdout = logging.getLogger("chaos-stdout")
chaos_results = logging.getLogger("chaos-results")


class ExperimentResult:
    def __init__(self):
        self.is_valid = False
        self.error = None
        self.analysis = dict()


class ViolationInducedExit(Exception):
    pass


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

    loop = asyncio.get_running_loop()

    end_time = loop.time() + config["warmup"]
    while workload.is_active:
        if (loop.time() + 1) >= end_time:
            break
        await asyncio.sleep(1)

    # inject
    fault = failure_factory()
    await fault.inject(cluster, workload)

    end_time = loop.time() + config["exploitation"]
    while workload.is_active:
        if (loop.time() + 1) >= end_time:
            break
        await asyncio.sleep(1)

    # recover
    await fault.recover()

    end_time = loop.time() + config["cooldown"]
    while workload.is_active:
        if (loop.time() + 1) >= end_time:
            break
        await asyncio.sleep(1)

    workload.stop()
    validation_result = await task
    await workload.dispose()

    result = ExperimentResult()
    result.is_valid = validation_result.is_valid
    result.error = validation_result.error

    title = f"inject / recover: {fault.title}"

    make_overview_chart(title, log_dir, config["availability_log"],
                        config["latency_log"])
    result.analysis = analyze_inject_recover_availability(
        log_dir, config["availability_log"], config["latency_log"])

    return result


async def inject_recover_scenarios_aio(config, cluster, faults,
                                       workload_factory):
    for fault in faults.keys():
        if config["reset_before_test"]:
            await cluster.restart()
            if not await cluster.is_ok():
                chaos_event_log.info(m(f"cluster isn't healthy").with_time())
                raise Exception(f"cluster isn't healthy")

        experiment_id = int(time.time())

        fault_dir = path.join(config["output"], fault)
        if path.exists(fault_dir):
            if not (path.isdir(fault_dir)):
                raise Exception(fault_dir + " must be a directory")
        else:
            os.mkdir(fault_dir)
        log_dir = path.join(fault_dir, str(experiment_id))
        os.mkdir(log_dir)
        chaos_event_log.info(
            m(f"starting {experiment_id} experiment (inject / recover, {fault})"
              ).with_time())

        now = datetime.now().strftime("%H:%M:%S")
        chaos_stdout.info(
            f"({now}) testing inject / recover with {fault} - {experiment_id}")

        result = await inject_recover_scenario_aio(log_dir, config, cluster,
                                                   workload_factory,
                                                   faults[fault])
        if result.is_valid:
            chaos_stdout.info(f"\tlinearizability testing passed")
            message = m(id=experiment_id,
                        type="result",
                        status="passed",
                        scenario="inject/recover",
                        fault=fault,
                        metrics=result.analysis).with_time()
            chaos_event_log.info(message)
            chaos_results.info(message)
        else:
            chaos_stdout.info(f"\tviolation: {result.error}")
            message = m(id=experiment_id,
                        type="result",
                        status="failed",
                        error="result.error",
                        scenario="inject/recover",
                        fault=fault,
                        metrics=result.analysis).with_time()
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
