# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import dataclasses
import sys
from time import sleep

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.failure_injector import FailureInjector
import rptest.services.chaos.workloads.all as workloads
import rptest.services.chaos.faults.all as faults


@dataclasses.dataclass
class TimingConfig:
    wait_progress_timeout_s: int = 20

    warmup_s: int = 20

    no_fault_steady_s: int = 180

    recoverable_fault_steady_s: int = 60
    recoverable_fault_impact_s: int = 60
    recoverable_fault_recovery_s: int = 60

    oneoff_fault_steady_s: int = 60
    oneoff_fault_recovery_s: int = 120


class SingleFaultTestBase(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def run(self,
            workload: workloads.WorkloadServiceBase,
            fault: faults.FaultBase | None,
            timings: TimingConfig = TimingConfig()):
        try:
            self._do_run(workload, fault, timings)
        finally:
            workload.stop_and_validate()

            if sys.exc_info()[0] is None:
                # check crashes only if the test passed,
                # if it failed, the check will be performed automatically
                self.redpanda.raise_on_crash()

    def _do_run(self, workload: workloads.WorkloadServiceBase,
                fault: faults.FaultBase | None, timings: TimingConfig):
        if timings.warmup_s > 0:
            self.logger.info(f"warming up for {timings.warmup_s}s")
            sleep(timings.warmup_s)

        self.logger.info(f"start measuring")
        for node in workload.nodes:
            workload.emit_event(node, "measure")

        if fault == None:
            if timings.no_fault_steady_s > 0:
                self.logger.info(
                    f"wait for {timings.no_fault_steady_s} seconds "
                    f"to record steady state")
                sleep(timings.no_fault_steady_s)
        elif isinstance(fault, faults.RecoverableFault):
            if timings.recoverable_fault_steady_s > 0:
                self.logger.info(
                    f"wait for {timings.recoverable_fault_steady_s} seconds "
                    f" to record steady state")
                sleep(timings.recoverable_fault_steady_s)

            for node in workload.nodes:
                workload.emit_event(node, "injecting")
            self.logger.info(f"injecting '{fault.name}'")
            fault.inject()
            self.logger.info(f"injected '{fault.name}'")

            try:
                for node in workload.nodes:
                    workload.emit_event(node, "injected")

                after_fault_info = {}
                for node in workload.nodes:
                    after_fault_info[node.name] = workload.info(node)

                if timings.recoverable_fault_impact_s > 0:
                    self.logger.info(
                        f"wait for {timings.recoverable_fault_impact_s} seconds "
                        "to record impacted state")
                    sleep(timings.recoverable_fault_impact_s)
                self.logger.info(
                    f"done waiting for "
                    f"{timings.recoverable_fault_impact_s} seconds")

                before_heal_info = {}
                for node in workload.nodes:
                    before_heal_info[node.name] = workload.info(node)

                for node in workload.nodes:
                    workload.emit_event(node, "healing")
            finally:
                self.logger.info(f"healing '{fault.name}'")
                fault.heal()
                self.logger.info(f"healed '{fault.name}'")
                for node in workload.nodes:
                    workload.emit_event(node, "healed")

            if timings.recoverable_fault_recovery_s > 0:
                self.logger.info(
                    f"wait for {timings.recoverable_fault_recovery_s} seconds "
                    f"to record recovering state")
                sleep(timings.recoverable_fault_recovery_s)
        elif isinstance(fault, faults.OneoffFault):
            if timings.oneoff_fault_steady_s > 0:
                self.logger.info(
                    f"wait for {timings.oneoff_fault_steady_s} seconds "
                    f"to record steady state")
                sleep(timings.oneoff_fault_steady_s)

            for node in workload.nodes:
                workload.emit_event(node, "injecting")
            self.logger.info(f"injecting '{fault.name}'")
            fault.execute()
            self.logger.info(f"injected '{fault.name}'")
            for node in workload.nodes:
                workload.emit_event(node, "injected")

            if timings.oneoff_fault_recovery_s > 0:
                self.logger.info(
                    f"wait for {timings.oneoff_fault_recovery_s} seconds "
                    f"to record recovering / impacted state")
                sleep(timings.oneoff_fault_recovery_s)
        else:
            raise Exception(f"Unknown fault type {type(fault)}")

    def tearDown(self):
        FailureInjector(self.redpanda)._heal_all()
        super().tearDown()
